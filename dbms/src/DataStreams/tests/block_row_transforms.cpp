#include <string>

#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

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
{
	try
	{
		DB::Block sample;

		DB::ColumnWithTypeAndName col1;
		col1.name = "col1";
		col1.type = new DB::DataTypeUInt64;
		col1.column = col1.type->createColumn();
		sample.insert(col1);

		DB::ColumnWithTypeAndName col2;
		col2.name = "col2";
		col2.type = new DB::DataTypeString;
		col2.column = col2.type->createColumn();
		sample.insert(col2);

		std::ifstream istr("test_in");
		std::ofstream ostr("test_out");

		DB::ReadBufferFromIStream in_buf(istr);
		DB::WriteBufferFromOStream out_buf(ostr);

		DB::RowInputStreamPtr row_input = new DB::TabSeparatedRowInputStream(in_buf, sample);
		DB::BlockInputStreamFromRowInputStream block_input(row_input, sample);
		DB::RowOutputStreamPtr row_output = new DB::TabSeparatedRowOutputStream(out_buf, sample);
		DB::BlockOutputStreamFromRowOutputStream block_output(row_output);

		DB::copyData(block_input, block_output);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
