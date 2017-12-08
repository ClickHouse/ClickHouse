#include <iostream>
#include <iomanip>

#include <IO/WriteBufferFromFileDescriptor.h>

#include <Storages/System/StorageSystemNumbers.h>

#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>


using namespace DB;

int main(int, char **)
try
{
    Context context = Context::createGlobal();
    Settings settings = context.getSettings();

    context.setPath("./");

    loadMetadata(context);

    Names column_names;
    column_names.push_back("WatchID");

    StoragePtr table = context.getTable("default", "hits6");

    QueryProcessingStage::Enum stage;
    BlockInputStreams streams = table->read(column_names, {}, context, stage, settings.max_block_size, settings.max_threads);

    for (size_t i = 0, size = streams.size(); i < size; ++i)
        streams[i] = std::make_shared<AsynchronousBlockInputStream>(streams[i]);

    BlockInputStreamPtr stream = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, settings.max_threads);
    stream = std::make_shared<LimitBlockInputStream>(stream, 10, 0);

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
