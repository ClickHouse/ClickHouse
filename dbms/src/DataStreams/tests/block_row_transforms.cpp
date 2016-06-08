#include <string>

#include <iostream>
#include <fstream>

#include <DB/Core/Block.h>
#include <DB/Core/ColumnWithTypeAndName.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>


int main(int argc, char ** argv)
try
{
	using namespace DB;

	Block sample;

	ColumnWithTypeAndName col1;
	col1.name = "col1";
	col1.type = std::make_shared<DataTypeUInt64>();
	col1.column = col1.type->createColumn();
	sample.insert(col1);

	ColumnWithTypeAndName col2;
	col2.name = "col2";
	col2.type = std::make_shared<DataTypeString>();
	col2.column = col2.type->createColumn();
	sample.insert(col2);

	std::ifstream istr("test_in");
	std::ofstream ostr("test_out");

	ReadBufferFromIStream in_buf(istr);
	WriteBufferFromOStream out_buf(ostr);

	RowInputStreamPtr row_input = std::make_shared<TabSeparatedRowInputStream>(in_buf, sample);
	BlockInputStreamFromRowInputStream block_input(row_input, sample);
	RowOutputStreamPtr row_output = std::make_shared<TabSeparatedRowOutputStream>(out_buf, sample);
	BlockOutputStreamFromRowOutputStream block_output(row_output);

	copyData(block_input, block_output);
}
catch (const DB::Exception & e)
{
	std::cerr << e.what() << ", " << e.displayText() << std::endl;
	return 1;
}
