#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/tests/gtest_disk.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/Context.h>
#include <Storages/StorageLog.h>
#include <Common/typeid_cast.h>
#include <Common/tests/gtest_global_context.h>

#include <memory>
#include <Processors/Executors/TreeExecutorBlockInputStream.h>

#if !__clang__
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wsuggest-override"
#endif


DB::StoragePtr createStorage(DB::DiskPtr & disk)
{
    using namespace DB;

    NamesAndTypesList names_and_types;
    names_and_types.emplace_back("a", std::make_shared<DataTypeUInt64>());

    StoragePtr table = StorageLog::create(
        disk, "table/", StorageID("test", "test"), ColumnsDescription{names_and_types}, ConstraintsDescription{}, 1048576);

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
        table->shutdown();
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
std::string writeData(int rows, DB::StoragePtr & table, const DB::Context & context)
{
    using namespace DB;

    std::string data;

    Block block;

    {
        const auto & storage_columns = table->getColumns();
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

    BlockOutputStreamPtr out = table->write({}, context);
    out->write(block);

    return data;
}

// Returns all table data in Values format.
std::string readData(DB::StoragePtr & table, const DB::Context & context)
{
    using namespace DB;

    Names column_names;
    column_names.push_back("a");

    QueryProcessingStage::Enum stage = table->getQueryProcessingStage(context);

    BlockInputStreamPtr in = std::make_shared<TreeExecutorBlockInputStream>(std::move(table->read(column_names, {}, context, stage, 8192, 1)[0]));

    Block sample;
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeUInt64>();
        sample.insert(std::move(col));
    }

    std::ostringstream ss;
    WriteBufferFromOStream out_buf(ss);
    BlockOutputStreamPtr output = FormatFactory::instance().getOutput("Values", out_buf, sample, context);

    copyData(*in, *output);

    output->flush();

    return ss.str();
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
