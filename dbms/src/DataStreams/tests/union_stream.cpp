#include <iostream>
#include <iomanip>

#include <IO/WriteBufferFromFileDescriptor.h>

#include <Storages/System/StorageSystemNumbers.h>

#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/BlockExtraInfoInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Interpreters/Context.h>


using namespace DB;

void test1()
{
    Context context = Context::createGlobal();
    StoragePtr table = StorageSystemNumbers::create("numbers", false);

    Names column_names;
    column_names.push_back("number");

    QueryProcessingStage::Enum stage1;
    QueryProcessingStage::Enum stage2;
    QueryProcessingStage::Enum stage3;

    BlockInputStreams streams;
    streams.emplace_back(std::make_shared<LimitBlockInputStream>(table->read(column_names, 0, context, stage1, 1, 1)[0], 30, 30000));
    streams.emplace_back(std::make_shared<LimitBlockInputStream>(table->read(column_names, 0, context, stage2, 1, 1)[0], 30, 2000));
    streams.emplace_back(std::make_shared<LimitBlockInputStream>(table->read(column_names, 0, context, stage3, 1, 1)[0], 30, 100));

    UnionBlockInputStream<> union_stream(streams, nullptr, 2);

    WriteBufferFromFileDescriptor wb(STDERR_FILENO);
    Block sample = table->getSampleBlock();
    BlockOutputStreamPtr out = context.getOutputFormat("TabSeparated", wb, sample);

    while (Block block = union_stream.read())
    {
        out->write(block);
        wb.next();
    }
}

void test2()
{
    Context context = Context::createGlobal();
    StoragePtr table = StorageSystemNumbers::create("numbers", false);

    Names column_names;
    column_names.push_back("number");

    QueryProcessingStage::Enum stage1;
    QueryProcessingStage::Enum stage2;
    QueryProcessingStage::Enum stage3;

    BlockExtraInfo extra_info1;
    extra_info1.host = "host1";
    extra_info1.resolved_address = "127.0.0.1";
    extra_info1.port = 9000;
    extra_info1.user = "user1";

    BlockExtraInfo extra_info2;
    extra_info2.host = "host2";
    extra_info2.resolved_address = "127.0.0.2";
    extra_info2.port = 9001;
    extra_info2.user = "user2";

    BlockExtraInfo extra_info3;
    extra_info3.host = "host3";
    extra_info3.resolved_address = "127.0.0.3";
    extra_info3.port = 9003;
    extra_info3.user = "user3";

    BlockInputStreams streams;

    BlockInputStreamPtr stream1 = std::make_shared<LimitBlockInputStream>(table->read(column_names, 0, context, stage1, 1, 1)[0], 30, 30000);
    stream1 = std::make_shared<BlockExtraInfoInputStream>(stream1, extra_info1);
    streams.emplace_back(stream1);

    BlockInputStreamPtr stream2 = std::make_shared<LimitBlockInputStream>(table->read(column_names, 0, context, stage2, 1, 1)[0], 30, 2000);
    stream2 = std::make_shared<BlockExtraInfoInputStream>(stream2, extra_info2);
    streams.emplace_back(stream2);

    BlockInputStreamPtr stream3 = std::make_shared<LimitBlockInputStream>(table->read(column_names, 0, context, stage3, 1, 1)[0], 30, 100);
    stream3 = std::make_shared<BlockExtraInfoInputStream>(stream3, extra_info3);
    streams.emplace_back(stream3);

    UnionBlockInputStream<StreamUnionMode::ExtraInfo> union_stream(streams, nullptr, 2);

    auto getSampleBlock = []()
    {
        Block block;
        ColumnWithTypeAndName col;

        col.name = "number";
        col.type = std::make_shared<DataTypeUInt64>();
        col.column = col.type->createColumn();
        block.insert(col);

        col.name = "host_name";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        block.insert(col);

        col.name = "host_address";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        block.insert(col);

        col.name = "port";
        col.type = std::make_shared<DataTypeUInt16>();
        col.column = col.type->createColumn();
        block.insert(col);

        col.name = "user";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        block.insert(col);

        return block;
    };

    WriteBufferFromFileDescriptor wb(STDERR_FILENO);
    Block sample = getSampleBlock();
    BlockOutputStreamPtr out = context.getOutputFormat("TabSeparated", wb, sample);

    while (Block block = union_stream.read())
    {
        const auto & col = block.safeGetByPosition(0);
        auto extra_info = union_stream.getBlockExtraInfo();

        ColumnPtr host_name_column = std::make_shared<ColumnString>();
        ColumnPtr host_address_column = std::make_shared<ColumnString>();
        ColumnPtr port_column = std::make_shared<ColumnUInt16>();
        ColumnPtr user_column = std::make_shared<ColumnString>();

        size_t row_count = block.rows();
        for (size_t i = 0; i < row_count; ++i)
        {
            host_name_column->insert(extra_info.resolved_address);
            host_address_column->insert(extra_info.host);
            port_column->insert(static_cast<UInt64>(extra_info.port));
            user_column->insert(extra_info.user);
        }

        Block out_block;
        out_block.insert(ColumnWithTypeAndName(col.column->clone(), col.type, col.name));
        out_block.insert(ColumnWithTypeAndName(host_name_column, std::make_shared<DataTypeString>(), "host_name"));
        out_block.insert(ColumnWithTypeAndName(host_address_column, std::make_shared<DataTypeString>(), "host_address"));
        out_block.insert(ColumnWithTypeAndName(port_column, std::make_shared<DataTypeUInt16>(), "port"));
        out_block.insert(ColumnWithTypeAndName(user_column, std::make_shared<DataTypeString>(), "user"));

        out->write(out_block);
        wb.next();
    }
}

int main(int argc, char ** argv)
{
    try
    {
        test1();
        test2();
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl
            << std::endl
            << "Stack trace:" << std::endl
            << e.getStackTrace().toString();
        return 1;
    }

    return 0;
}
