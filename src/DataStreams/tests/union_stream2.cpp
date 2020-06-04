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
#include <Processors/Executors/TreeExecutorBlockInputStream.h>


using namespace DB;

int main(int, char **)
try
{
    SharedContextHolder shared_context = Context::createShared();
    Context context = Context::createGlobal(shared_context.get());
    context.makeGlobalContext();
    Settings settings = context.getSettings();

    context.setPath("./");

    loadMetadata(context);

    Names column_names;
    column_names.push_back("WatchID");

    StoragePtr table = DatabaseCatalog::instance().getTable({"default", "hits6"}, context);

    QueryProcessingStage::Enum stage = table->getQueryProcessingStage(context);
    auto pipes = table->read(column_names, {}, context, stage, settings.max_block_size, settings.max_threads);

    BlockInputStreams streams(pipes.size());

    for (size_t i = 0, size = streams.size(); i < size; ++i)
        streams[i] = std::make_shared<AsynchronousBlockInputStream>(std::make_shared<TreeExecutorBlockInputStream>(std::move(pipes[i])));

    BlockInputStreamPtr stream = std::make_shared<UnionBlockInputStream>(streams, nullptr, settings.max_threads);
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
        << e.getStackTraceString();
    return 1;
}
