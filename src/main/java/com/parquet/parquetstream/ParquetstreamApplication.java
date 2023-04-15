package com.parquet.parquetstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@SpringBootApplication
public class ParquetstreamApplication {

	public static void main(String[] args) throws FileNotFoundException {
		SpringApplication.run(ParquetstreamApplication.class, args);
		Path path=new Path("C:\\Users\\Umair Husain\\Downloads\\parquetstream\\parquetstream\\src\\main\\resources\\sample-parquet.parquet");
		InputStream fileInputStream = new FileInputStream(path.toString());
		 try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(path, new Configuration()), ParquetReadOptions.builder().build())) {
	            final ParquetMetadata footer = parquetFileReader.getFooter();
	            final MessageType schema = createdParquetSchema(footer);
	            PageReadStore pages;

	            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
	                final long rows = pages.getRowCount();
	                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
	                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

	                for (int row = 0; row < rows; row++) {

	                        final Map<String, Object> eventData = new HashMap<>();

	                        int fieldIndex = 0;
	                        final SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
	                        for (Type field : schema.getFields()) {

	                            try {
	                               System.out.print(" key: "+field.getName()+" value: "+simpleGroup.getValueToString(fieldIndex, 0));
	                            }
	                            catch (Exception parquetException){
	                               System.out.println("error");
	                            }

	                            fieldIndex++;

	                        }
	                }
	            }
	        } catch (Exception parquetException) {
	           System.out.println("error1");
	        	} 	}
	 private static MessageType createdParquetSchema(ParquetMetadata parquetMetadata) {
	        return parquetMetadata.getFileMetaData().getSchema();
	    }

}
