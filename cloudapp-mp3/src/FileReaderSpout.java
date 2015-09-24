
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private BufferedReader _reader;

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

    try {
      String inputFilename = (String) conf.get("SpoutInput");
      File file = new File(inputFilename);
      this._reader = new BufferedReader(new FileReader(file));
    }
    catch (FileNotFoundException ex) {
      this._reader = null;
    } 
    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
      
    if (this._reader == null ) {
      return;
    }

    try {
      String line = null;
      while ((line = _reader.readLine()) != null) {
          _collector.emit(new Values(line));
      }
   
      Thread.sleep(2 * 60 * 1000);
    }
    catch(IOException ioex) {
      // do nothing
    }
    catch(InterruptedException iex) {
      // do nothing
    }
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
    try {
      this._reader.close();
    }
    catch(IOException iex) {
      // do nothing
    }
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
