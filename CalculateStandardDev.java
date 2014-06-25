import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.join.*;
import java.text.DecimalFormat;
import java.lang.Math.*;




public class CalculateStandardDev extends Configured implements Tool {
   
 
	
   public static boolean isNumber(String str){
		for(int i = 0; i < str.length();i++){
			if(!Character.isDigit(str.charAt(i))){
				return false;
			}
			
		}
		return true;
	}

   public static double roundTwoDecimals(double d) {
            DecimalFormat twoDForm = new DecimalFormat("#.##");
            return Double.valueOf(twoDForm.format(d));
     }
   public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	

	public void configure(JobConf job) {
	
   	}
	
	
	

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    String line = value.toString();

	    
	    String[] strList = line.split("\t");   //Parse the line with the tab charact
	    
	  
	    String year = strList[1];
	    String volume = strList[3];
	    
	    String t = volume + "," + "1";
	
	    output.collect(new Text("1"),new Text(t));
	    
	  }
   }
    
    //Two different Mappers
   public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	

    	public void configure(JobConf job) {
    	}
    	
    	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	    String line = value.toString();
    	
			String[] strList = line.split("\t");   //Parse the line with the tab character
 
			String[] word12 = strList[0].split(" ");	    

    	    String year = strList[1];
    	    String volume = strList[3];
    	    
			String t = volume + "," + "1";
			output.collect(new Text("1"),new Text(t));
      }
    }
   

   

   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {
		private double count = 0;
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	   		double sum_volume = 0;
			double sum_volume_square = 0;
			double std_dev = 0;
			double volume = 0;
			double max_volume = 0;
			String v = "";
			while(values.hasNext()){
				String line = values.next().toString();
				String[] strList = line.split(",");
				volume = Integer.parseInt(strList[0]);
				sum_volume += volume;
				sum_volume_square += volume*volume;
				count = count+1;
			}
			double temp = sum_volume_square/count-(sum_volume*sum_volume)/(count*count);
	   		std_dev = Math.sqrt(temp);

	   		output.collect(new Text("standard deviation"), new DoubleWritable(std_dev));
		}
   }
    


    public int run(String[] args) throws Exception {
	JobConf conf1 = new JobConf(getConf(),CalculateStandardDev.class); //Shall we set a different conf object for the second mapper class?
	
	conf1.setJobName("CalculateStandardDev");

	conf1.setOutputKeyClass(Text.class);
	conf1.setOutputValueClass(DoubleWritable.class);

	conf1.setMapperClass(Map1.class);
	conf1.setMapperClass(Map2.class);
	conf1.setReducerClass(Reduce.class);

	conf1.setInputFormat(TextInputFormat.class);
	conf1.setOutputFormat(TextOutputFormat.class);
		
	MultipleInputs.addInputPath(conf1,new Path(args[0]),TextInputFormat.class,Map1.class);
	MultipleInputs.addInputPath(conf1,new Path(args[1]),TextInputFormat.class,Map2.class);

	FileOutputFormat.setOutputPath(conf1, new Path(args[2]));

	JobClient.runJob(conf1);
	
	return 0;
   }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new CalculateStandardDev(), args);
	System.exit(res);
    }
}
