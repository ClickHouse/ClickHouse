#include <iostream>

#include <IO/WriteBufferFromOStream.h>
#include <Storages/StorageLog.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>
#include <Disks/DiskLocal.h>
#include <Processors/Executors/TreeExecutorBlockInputStream.h>

int main(int, char **)
try
{
    using namespace DB;

    const size_t rows = 10000000;

    /// create table with a pair of columns

    NamesAndTypesList names_and_types;
    names_and_types.emplace_back("a", std::make_shared<DataTypeUInt64>());
    names_and_types.emplace_back("b", std::make_shared<DataTypeUInt8>());

    SharedContextHolder shared_context = Context::createShared();
    auto context = Context::createGlobal(shared_context.get());
    context.makeGlobalContext();
    context.setPath("./");

    DiskPtr disk = std::make_unique<DiskLocal>("default", "./", 0);
    StoragePtr table = StorageLog::create(disk, "table/", StorageID("test", "test"), ColumnsDescription{names_and_types}, ConstraintsDescription{}, 1048576);

    table->startup();

    /// write into it
    {
        Block block;

        {
            ColumnWithTypeAndName column;
            column.name = "a";
            column.type = table->getColumns().getPhysical("a").type;
            auto col = column.type->createColumn();
            ColumnUInt64::Container & vec = typeid_cast<ColumnUInt64 &>(*col).getData();

            vec.resize(rows);
            for (size_t i = 0; i < rows; ++i)
                vec[i] = i;

            column.column = std::move(col);
            block.insert(column);
        }

        {
            ColumnWithTypeAndName column;
            column.name = "b";
            column.type = table->getColumns().getPhysical("b").type;
            auto col = column.type->createColumn();
            ColumnUInt8::Container & vec = typeid_cast<ColumnUInt8 &>(*col).getData();

            vec.resize(rows);
            for (size_t i = 0; i < rows; ++i)
                vec[i] = i * 2;

            column.column = std::move(col);
            block.insert(column);
        }

        BlockOutputStreamPtr out = table->write({}, context);
        out->write(block);
    }

    /// read from it
    {
        Names column_names;
        column_names.push_back("a");
        column_names.push_back("b");

        QueryProcessingStage::Enum stage = table->getQueryProcessingStage(context);

        BlockInputStreamPtr in = std::make_shared<TreeExecutorBlockInputStream>(std::move(table->read(column_names, {}, context, stage, 8192, 1)[0]));

        Block sample;
        {
            ColumnWithTypeAndName col;
            col.type = std::make_shared<DataTypeUInt64>();
            sample.insert(std::move(col));
        }
        {
            ColumnWithTypeAndName col;
            col.type = std::make_shared<DataTypeUInt8>();
            sample.insert(std::move(col));
        }

        WriteBufferFromOStream out_buf(std::cout);

        LimitBlockInputStream in_limit(in, 10, 0);
        BlockOutputStreamPtr output = FormatFactory::instance().getOutput("TabSeparated", out_buf, sample, context);

        copyData(in_limit, *output);
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
