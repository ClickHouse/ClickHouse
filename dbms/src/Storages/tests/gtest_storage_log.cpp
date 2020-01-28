#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/StorageLog.h>
#include <Common/typeid_cast.h>
#include <Disks/tests/gtest_disk.h>

#if !__clang__
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wsuggest-override"
#endif

DB::Context createContext()
{
    auto context = DB::Context::createGlobal();
    context.makeGlobalContext();
    context.setPath("./");
    return context;
}

DB::StoragePtr createStorage(DB::DiskPtr & disk)
{
    using namespace DB;

    NamesAndTypesList names_and_types;
    names_and_types.emplace_back("a", std::make_shared<DataTypeUInt64>());
    names_and_types.emplace_back("b", std::make_shared<DataTypeUInt8>());

    StoragePtr table = StorageLog::create(
        disk, "table", StorageID("test", "test"), ColumnsDescription{names_and_types}, ConstraintsDescription{}, 1048576);

    table->startup();

    return table;
}

template <typename T>
class StorageLogTest : public testing::Test
{
public:
    void SetUp() override
    {
        context_ = createContext();
        disk_ = createDisk<T>();
        table_ = createStorage(disk_);
    }

    void TearDown() override
    {
        table_->shutdown();
        destroyDisk<T>(disk_);
        context_.shutdown();
    }

    const DB::DiskPtr & getDisk() { return disk_; }
    DB::Context getContext() { return context_; }
    DB::StoragePtr getTable() { return table_; }

private:
    DB::Context context_ = DB::Context::createGlobal();
    DB::DiskPtr disk_;
    DB::StoragePtr table_;
};


typedef testing::Types<DB::DiskMemory, DB::DiskLocal> DiskImplementations;
TYPED_TEST_SUITE(StorageLogTest, DiskImplementations);


TYPED_TEST(StorageLogTest, testReadWrite)
{
    using namespace DB;

    const int rows = 100;

    Block block;

    {
        ColumnWithTypeAndName column;
        column.name = "a";
        column.type = this->getTable()->getColumn("a").type;
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
        column.type = this->getTable()->getColumn("b").type;
        auto col = column.type->createColumn();
        ColumnUInt8::Container & vec = typeid_cast<ColumnUInt8 &>(*col).getData();

        vec.resize(rows);
        for (size_t i = 0; i < rows; ++i)
            vec[i] = i * 2;

        column.column = std::move(col);
        block.insert(column);
    }

    BlockOutputStreamPtr out = this->getTable()->write({}, this->getContext());
    out->write(block);

    /// read from it
    {
        Names column_names;
        column_names.push_back("a");
        column_names.push_back("b");

        QueryProcessingStage::Enum stage = this->getTable()->getQueryProcessingStage(this->getContext());

        BlockInputStreamPtr in = this->getTable()->read(column_names, {}, this->getContext(), stage, 8192, 1)[0];

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

        std::stringstream ss;
        WriteBufferFromOStream out_buf(ss);

        LimitBlockInputStream in_limit(in, 200, 0);
        BlockOutputStreamPtr output = FormatFactory::instance().getOutput("TabSeparated", out_buf, sample, this->getContext());

        copyData(in_limit, *output);

        std::string res = ss.str();
        std::cout << res << std::endl;
    }
}
