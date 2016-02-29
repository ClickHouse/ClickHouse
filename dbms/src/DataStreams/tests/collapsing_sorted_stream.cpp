#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/ConsoleChannel.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>
#include <DB/DataStreams/CollapsingFinalBlockInputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/Interpreters/Context.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>


using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
		Logger::root().setChannel(channel);
		Logger::root().setLevel("trace");

		Block block1;

		{
			ColumnWithTypeAndName column1;
			column1.name = "Sign";
			column1.type = new DataTypeInt8;
			column1.column = new ColumnInt8;
			column1.column->insert(DB::Int64(1));
			column1.column->insert(DB::Int64(-1));
			block1.insert(column1);

			ColumnWithTypeAndName column2;
			column2.name = "CounterID";
			column2.type = new DataTypeUInt32;
			column2.column = new ColumnUInt32;
			column2.column->insert(DB::UInt64(123));
			column2.column->insert(DB::UInt64(123));
			block1.insert(column2);
		}

		Block block2;

		{
			ColumnWithTypeAndName column1;
			column1.name = "Sign";
			column1.type = new DataTypeInt8;
			column1.column = new ColumnInt8;
			column1.column->insert(DB::Int64(1));
			column1.column->insert(DB::Int64(1));
			block2.insert(column1);

			ColumnWithTypeAndName column2;
			column2.name = "CounterID";
			column2.type = new DataTypeUInt32;
			column2.column = new ColumnUInt32;
			column2.column->insert(DB::UInt64(123));
			column2.column->insert(DB::UInt64(456));
			block2.insert(column2);
		}

		BlockInputStreams inputs;
		inputs.push_back(new OneBlockInputStream(block1));
		inputs.push_back(new OneBlockInputStream(block2));

		SortDescription descr;
		SortColumnDescription col_descr("CounterID", 1);
		descr.push_back(col_descr);

		//CollapsingSortedBlockInputStream collapsed(inputs, descr, "Sign", 1048576);
		CollapsingFinalBlockInputStream collapsed(inputs, descr, "Sign");

		Context context;
		WriteBufferFromFileDescriptor out_buf(STDERR_FILENO);
		BlockOutputStreamPtr output = context.getOutputFormat("TabSeparated", out_buf, block1);

		copyData(collapsed, *output);
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
