#include <string>

#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/Core/ColumnWithNameAndType.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/copyData.h>


int main(int argc, char ** argv)
{
	try
	{
		Poco::SharedPtr<DB::DataTypes> data_types = new DB::DataTypes;
		data_types->push_back(new DB::DataTypeUInt64);
		data_types->push_back(new DB::DataTypeString);

		DB::Block sample;

		DB::ColumnWithNameAndType col1;
		col1.name = "col1";
		col1.type = data_types->at(0);
		col1.column = col1.type->createColumn();
		sample.insert(col1);

		DB::ColumnWithNameAndType col2;
		col2.name = "col2";
		col2.type = data_types->at(1);
		col2.column = col2.type->createColumn();
		sample.insert(col2);

		std::ifstream istr("test_in");
		std::ofstream ostr("test_out");

		DB::ReadBufferFromIStream in_buf(istr);
		DB::WriteBufferFromOStream out_buf(ostr);

		DB::TabSeparatedRowInputStream row_input(in_buf, data_types);
		DB::BlockInputStreamFromRowInputStream block_input(row_input.clone(), sample);
		DB::TabSeparatedRowOutputStream row_output(out_buf, data_types);

		DB::copyData(block_input, row_output);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
