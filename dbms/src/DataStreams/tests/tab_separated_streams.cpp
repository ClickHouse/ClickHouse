#include <string>

#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>


using namespace DB;

int main(int argc, char ** argv)
try
{
	Block sample;
	{
		ColumnWithTypeAndName col;
		col.type = new DataTypeUInt64;
		sample.insert(col);
	}
	{
		ColumnWithTypeAndName col;
		col.type = new DataTypeString;
		sample.insert(col);
	}

	ReadBufferFromFile in_buf("test_in");
	WriteBufferFromFile out_buf("test_out");

	RowInputStreamPtr row_input = new TabSeparatedRowInputStream(in_buf, sample);
	RowOutputStreamPtr row_output = new TabSeparatedRowOutputStream(out_buf, sample);

	BlockInputStreamFromRowInputStream block_input(row_input, sample);
	BlockOutputStreamFromRowOutputStream block_output(row_output);

	copyData(block_input, block_output);
	return 0;
}
catch (...)
{
	std::cerr << getCurrentExceptionMessage(true) << '\n';
	return 1;
}
