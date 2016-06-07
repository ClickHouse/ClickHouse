#include <iostream>
#include <iomanip>

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/Storages/System/StorageSystemNumbers.h>

#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/loadMetadata.h>


using namespace DB;

int main(int argc, char ** argv)
try
{
	Context context;
	Settings settings = context.getSettings();

	context.setPath("./");

	loadMetadata(context);

	Names column_names;
	column_names.push_back("WatchID");

	StoragePtr table = context.getTable("default", "hits6");

	QueryProcessingStage::Enum stage;
	BlockInputStreams streams = table->read(column_names, nullptr, context, settings, stage, settings.max_block_size, settings.max_threads);

	for (size_t i = 0, size = streams.size(); i < size; ++i)
		streams[i] = new AsynchronousBlockInputStream(streams[i]);

	BlockInputStreamPtr stream = new UnionBlockInputStream<>(streams, nullptr, settings.max_threads);
	stream = new LimitBlockInputStream(stream, 10, 0);

	WriteBufferFromFileDescriptor wb(STDERR_FILENO);
	Block sample = table->getSampleBlock();
	BlockOutputStreamPtr out = context.getOutputFormat("TabSeparated", wb, sample);

	copyData(*stream, *out);

	return 0;
}
catch (const Exception & e)
{
	std::cerr << e.what() << ", " << e.displayText() << std::endl
		<< std::endl
		<< "Stack trace:" << std::endl
		<< e.getStackTrace().toString();
	return 1;
}
