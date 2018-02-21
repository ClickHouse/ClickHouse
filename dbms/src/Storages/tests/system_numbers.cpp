#include <iostream>

#include <IO/WriteBufferFromOStream.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/TabSeparatedRowOutputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>


int main(int, char **)
try
{
    using namespace DB;

    StoragePtr table = StorageSystemNumbers::create("numbers", false);

    Names column_names;
    column_names.push_back("number");

    Block sample;
    ColumnWithTypeAndName col;
    col.type = std::make_shared<DataTypeUInt64>();
    sample.insert(std::move(col));

    WriteBufferFromOStream out_buf(std::cout);

    QueryProcessingStage::Enum stage;

    LimitBlockInputStream input(table->read(column_names, {}, Context::createGlobal(), stage, 10, 1)[0], 10, 96);
    RowOutputStreamPtr output_ = std::make_shared<TabSeparatedRowOutputStream>(out_buf, sample);
    BlockOutputStreamFromRowOutputStream output(output_, sample);

    copyData(input, output);

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
