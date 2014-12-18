#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/Storages/StorageSystemNumbers.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/loadMetadata.h>

using Poco::SharedPtr;


int main(int argc, char ** argv)
{
	try
	{
		DB::Context context;
		DB::Settings settings = context.getSettings();

		context.setPath("./");

		DB::loadMetadata(context);
		
		DB::Names column_names;
		column_names.push_back("WatchID");

		DB::StoragePtr table = context.getTable("default", "hits6");

		DB::QueryProcessingStage::Enum stage;
		DB::BlockInputStreams streams = table->read(column_names, nullptr, context, settings, stage, settings.max_block_size, settings.max_threads);

		for (size_t i = 0, size = streams.size(); i < size; ++i)
			streams[i] = new DB::AsynchronousBlockInputStream(streams[i]);
		
		DB::BlockInputStreamPtr stream = new DB::UnionBlockInputStream(streams, settings.max_threads);
		stream = new DB::LimitBlockInputStream(stream, 10);

		DB::FormatFactory format_factory;
		DB::WriteBufferFromFileDescriptor wb(STDERR_FILENO);
		DB::Block sample = table->getSampleBlock();
		DB::BlockOutputStreamPtr out = format_factory.getOutput("TabSeparated", wb, sample);

		DB::copyData(*stream, *out);
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl
			<< std::endl
			<< "Stack trace:" << std::endl
			<< e.getStackTrace().toString();
		return 1;
	}

	return 0;
}
