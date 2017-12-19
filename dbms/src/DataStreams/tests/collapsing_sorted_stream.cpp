#include <iostream>
#include <iomanip>

#include <Poco/ConsoleChannel.h>

#include <IO/WriteBufferFromFileDescriptor.h>

#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/CollapsingSortedBlockInputStream.h>
#include <DataStreams/CollapsingFinalBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>

#include <DataTypes/DataTypesNumber.h>


int main(int, char **)
try
{
    using namespace DB;

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Logger::root().setChannel(channel);
    Logger::root().setLevel("trace");

    Block block1;

    {
        ColumnWithTypeAndName column1;
        column1.name = "Sign";
        column1.type = std::make_shared<DataTypeInt8>();
        column1.column = ColumnInt8::create({1, -1});
        block1.insert(column1);

        ColumnWithTypeAndName column2;
        column2.name = "CounterID";
        column2.type = std::make_shared<DataTypeUInt32>();
        column2.column = ColumnUInt32::create({123, 123});
        block1.insert(column2);
    }

    Block block2;

    {
        ColumnWithTypeAndName column1;
        column1.name = "Sign";
        column1.type = std::make_shared<DataTypeInt8>();
        column1.column = ColumnInt8::create({1, 1});
        block2.insert(column1);

        ColumnWithTypeAndName column2;
        column2.name = "CounterID";
        column2.type = std::make_shared<DataTypeUInt32>();
        column2.column = ColumnUInt32::create({123, 456});
        block2.insert(column2);
    }

    BlockInputStreams inputs;
    inputs.push_back(std::make_shared<OneBlockInputStream>(block1));
    inputs.push_back(std::make_shared<OneBlockInputStream>(block2));

    SortDescription descr;
    SortColumnDescription col_descr("CounterID", 1, 1);
    descr.push_back(col_descr);

    //CollapsingSortedBlockInputStream collapsed(inputs, descr, "Sign", 1048576);
    CollapsingFinalBlockInputStream collapsed(inputs, descr, "Sign");

    Context context = Context::createGlobal();
    WriteBufferFromFileDescriptor out_buf(STDERR_FILENO);
    BlockOutputStreamPtr output = context.getOutputFormat("TabSeparated", out_buf, block1);

    copyData(collapsed, *output);

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}
