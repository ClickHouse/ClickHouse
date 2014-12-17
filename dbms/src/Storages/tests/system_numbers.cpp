#include <iostream>

#include <Poco/SharedPtr.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Interpreters/Context.h>

using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		DB::StoragePtr table = DB::StorageSystemNumbers::create("Numbers");

		DB::Names column_names;
		column_names.push_back("number");

		DB::Block sample;
		DB::ColumnWithNameAndType col;
		col.type = new DB::DataTypeUInt64;
		sample.insert(col);

		DB::WriteBufferFromOStream out_buf(std::cout);

		DB::QueryProcessingStage::Enum stage;
		
		DB::LimitBlockInputStream input(table->read(column_names, 0, DB::Context{}, DB::Settings(), stage, 10)[0], 10, 96);
		DB::RowOutputStreamPtr output_ = new DB::TabSeparatedRowOutputStream(out_buf, sample);
		DB::BlockOutputStreamFromRowOutputStream output(output_);
		
		DB::copyData(input, output);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
