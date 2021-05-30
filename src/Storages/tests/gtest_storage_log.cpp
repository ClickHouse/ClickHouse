#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/tests/gtest_disk.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/Context.h>
#include <Storages/StorageLog.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/typeid_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <memory>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Processors/QueryPipeline.h>

#if !defined(__clang__)
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wsuggest-override"
#endif


DB::StoragePtr createStorage(DB::DiskPtr & disk)
{
    using namespace DB;

    NamesAndTypesList names_and_types;
    names_and_types.emplace_back("a", std::make_shared<DataTypeUInt64>());

    StoragePtr table = StorageLog::create(
        disk, "table/", StorageID("test", "test"), ColumnsDescription{names_and_types}, ConstraintsDescription{}, String{}, false, 1048576);

    table->startup();

    return table;
}

template <typename T>
class StorageLogTest : public testing::Test
{
public:

    void SetUp() override
    {
        disk = createDisk<T>();
        table = createStorage(disk);
    }

    void TearDown() override
    {
        table->flushAndShutdown();
        destroyDisk<T>(disk);
    }

    const DB::DiskPtr & getDisk() { return disk; }
    DB::StoragePtr & getTable() { return table; }

private:
    DB::DiskPtr disk;
    DB::StoragePtr table;
};


using DiskImplementations = testing::Types<DB::DiskMemory, DB::DiskLocal>;
TYPED_TEST_SUITE(StorageLogTest, DiskImplementations);

// Returns data written to table in Values format.
std::string writeData(int rows, DB::StoragePtr & table, const DB::ContextPtr context)
{
    using namespace DB;
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    std::string data;

    Block block;

    {
        const auto & storage_columns = metadata_snapshot->getColumns();
        ColumnWithTypeAndName column;
        column.name = "a";
        column.type = storage_columns.getPhysical("a").type;
        auto col = column.type->createColumn();
        ColumnUInt64::Container & vec = typeid_cast<ColumnUInt64 &>(*col).getData();

        vec.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            vec[i] = i;
            if (i > 0)
                data += ",";
            data += "(" + std::to_string(i) + ")";
        }

        column.column = std::move(col);
        block.insert(column);
    }

    BlockOutputStreamPtr out = table->write({}, metadata_snapshot, context);
    out->write(block);
    out->writeSuffix();

    return data;
}

// Returns all table data in Values format.
std::string readData(DB::StoragePtr & table, const DB::ContextPtr context)
{
    using namespace DB;
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    Names column_names;
    column_names.push_back("a");

    SelectQueryInfo query_info;
    QueryProcessingStage::Enum stage = table->getQueryProcessingStage(
        context, QueryProcessingStage::Complete, metadata_snapshot, query_info);

    QueryPipeline pipeline;
    pipeline.init(table->read(column_names, metadata_snapshot, query_info, context, stage, 8192, 1));
    BlockInputStreamPtr in = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    Block sample;
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeUInt64>();
        sample.insert(std::move(col));
    }

    tryRegisterFormats();

    WriteBufferFromOwnString out_buf;
    BlockOutputStreamPtr output = FormatFactory::instance().getOutputStream("Values", out_buf, sample, context);

    copyData(*in, *output);

    output->flush();

    return out_buf.str();
}

TYPED_TEST(StorageLogTest, testReadWrite)
{
    using namespace DB;
    const auto & context_holder = getContext();

    std::string data;

    // Write several chunks of data.
    data += writeData(10, this->getTable(), context_holder.context);
    data += ",";
    data += writeData(20, this->getTable(), context_holder.context);
    data += ",";
    data += writeData(10, this->getTable(), context_holder.context);

    ASSERT_EQ(data, readData(this->getTable(), context_holder.context));
}
