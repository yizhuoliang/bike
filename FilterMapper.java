import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Splitting the CSV line
        String[] fields = value.toString().split(",");
        
        if (key.get() == 0 && fields[0].equals("ride_id")) {
            // This is the header row; we ignore it in the mapper.
            return;
        }

        if (fields.length > 12) {
            String rideId = fields[0];
            String startTime = fields[2];
            String endTime = fields[3];
            String startLat = fields[8];
            String startLng = fields[9];
            String endLat = fields[10];
            String endLng = fields[11];

            // Construct the output as a CSV line
            String outputValue = String.join(",", rideId, startLat, startLng, endLat, endLng, startTime, endTime);

            // Write out the data as key and value
            context.write(new Text(rideId), new Text(outputValue));
        }
    }
}
