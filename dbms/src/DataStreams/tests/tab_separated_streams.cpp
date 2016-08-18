#include <string>

#include <iostream>
#include <fstream>

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
		col.type = std::make_shared<DataTypeUInt64>();
		sample.insert(std::move(col));
	}
	{
		ColumnWithTypeAndName col;
		col.type = std::make_shared<DataTypeString>();
		sample.insert(std::move(col));
	}

	ReadBufferFromFile in_buf("test_in");
	WriteBufferFromFile out_buf("test_out");

	RowInputStreamPtr row_input = std::make_shared<TabSeparatedRowInputStream>(in_buf, sample);
	RowOutputStreamPtr row_output = std::make_shared<TabSeparatedRowOutputStream>(out_buf, sample);

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
