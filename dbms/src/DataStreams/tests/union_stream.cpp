#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>
#include <Poco/NumberParser.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/Storages/StorageSystemNumbers.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		DB::StorageSystemNumbers table("numbers");

		DB::Names column_names;
		column_names.push_back("number");

		DB::BlockInputStreams streams;
		streams.push_back(new DB::LimitBlockInputStream(table.read(column_names, 0, 1)[0], 30, 30000));
		streams.push_back(new DB::LimitBlockInputStream(table.read(column_names, 0, 1)[0], 30, 2000));
		streams.push_back(new DB::LimitBlockInputStream(table.read(column_names, 0, 1)[0], 30, 100));

		DB::UnionBlockInputStream union_stream(streams, 2);

		DB::FormatFactory format_factory;
		DB::WriteBufferFromFileDescriptor wb(STDERR_FILENO);
		DB::Block sample = table.getSampleBlock();
		DB::BlockOutputStreamPtr out = format_factory.getOutput("TabSeparated", wb, sample);

		while (DB::Block block = union_stream.read())
		{
			out->write(block);
			wb.next();
		}
		//DB::copyData(union_stream, *out);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString();
		return 1;
	}

	return 0;
}
