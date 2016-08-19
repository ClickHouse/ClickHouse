#include <iostream>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Storages/System/StorageSystemNumbers.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Interpreters/Context.h>


int main(int argc, char ** argv)
try
{
	using namespace DB;

	StoragePtr table = StorageSystemNumbers::create("Numbers");

	Names column_names;
	column_names.push_back("number");

	Block sample;
	ColumnWithTypeAndName col;
	col.type = std::make_shared<DataTypeUInt64>();
	sample.insert(std::move(col));

	WriteBufferFromOStream out_buf(std::cout);

	QueryProcessingStage::Enum stage;

	LimitBlockInputStream input(table->read(column_names, 0, Context{}, Settings(), stage, 10)[0], 10, 96);
	RowOutputStreamPtr output_ = std::make_shared<TabSeparatedRowOutputStream>(out_buf, sample);
	BlockOutputStreamFromRowOutputStream output(output_);

	copyData(input, output);

	return 0;
}
catch (const DB::Exception & e)
{
	std::cerr << e.what() << ", " << e.displayText() << std::endl;
	return 1;
}
