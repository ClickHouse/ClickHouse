#include <iostream>

#include <IO/WriteBufferFromOStream.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/TreeExecutorBlockInputStream.h>


int main(int, char **)
try
{
    using namespace DB;

    StoragePtr table = StorageSystemNumbers::create(StorageID("test", "numbers"), false);

    Names column_names;
    column_names.push_back("number");

    Block sample;
    ColumnWithTypeAndName col;
    col.type = std::make_shared<DataTypeUInt64>();
    sample.insert(std::move(col));

    WriteBufferFromOStream out_buf(std::cout);

    SharedContextHolder shared_context = Context::createShared();
    auto context = Context::createGlobal(shared_context.get());
    context.makeGlobalContext();
    QueryProcessingStage::Enum stage = table->getQueryProcessingStage(context);

    auto stream = std::make_shared<TreeExecutorBlockInputStream>(std::move(table->read(column_names, {}, context, stage, 10, 1)[0]));
    LimitBlockInputStream input(stream, 10, 96);
    BlockOutputStreamPtr out = FormatFactory::instance().getOutput("TabSeparated", out_buf, sample, context);

    copyData(input, *out);

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
