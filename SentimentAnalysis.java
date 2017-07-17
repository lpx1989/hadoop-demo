import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liu.peixin on 2017-07-16.
 */
public class SentimentAnalysis {

    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {

        public Map<String, String> emotionLibrary = new HashMap<String, String>();

        @Override
        public void setup(Context context) throws IOException{
            //create emotion dic
            Configuration configuration = context.getConfiguration();
            String dicPath = configuration.get("dictionary", "");
            BufferedReader br = new BufferedReader(new FileReader(dicPath));
            String line = br.readLine();

            while(line != null) {
                String[] word_feeling = line.split("\t");
                emotionLibrary.put(word_feeling[0], word_feeling[1]);
                line = br.readLine();
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            //read file from hdfs
            //split -> split into small words
            //look up emotionDic,word -> emotion(positive, negative)
            //write to reducer
            String[] words = value.toString().trim().split("\\s+"); //split by space
            for (String word : words) {
                if (emotionLibrary.containsKey(word.toLowerCase())) {
                    context.write(new Text(emotionLibrary.get(word.toLowerCase())), new IntWritable(1));
                }
            }
        }
    }


    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            //key: positive/negative
            //value = list<value> -> <1, 1, 1>
            //combine emotion total count
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.set("dictionary", args[2]);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
