#include <gtest/gtest.h>

#include <Backups/IBackup.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/tests/gtest_disk.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/StorageLog.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/typeid_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <unistd.h>

#include <memory>
#include <string_view>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB::ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}


static DB::StoragePtr createStorage(DB::DiskPtr & disk)
{
    using namespace DB;

    NamesAndTypesList names_and_types;
    names_and_types.emplace_back("a", std::make_shared<DataTypeUInt64>());

    StoragePtr table = std::make_shared<StorageLog>(
        "Log", disk, "table/", StorageID("test", "test"), ColumnsDescription{names_and_types},
        ConstraintsDescription{}, String{}, LoadingStrictnessLevel::CREATE, /*is_fresh_definition=*/true,
        getContext().context);

    table->startup();

    return table;
}

class StorageLogTest : public testing::Test
{
public:

    void SetUp() override
    {
        disk = createDisk();
        table = createStorage(disk);
    }

    void TearDown() override
    {
        table->flushAndShutdown();
        destroyDisk(disk);
    }

    const DB::DiskPtr & getDisk() { return disk; }
    DB::StoragePtr & getTable() { return table; }

private:
    DB::DiskPtr disk;
    DB::StoragePtr table;
};


// Returns data written to table in Values format.
static std::string writeData(int rows, DB::StoragePtr & table, const DB::ContextPtr context)
{
    using namespace DB;
    auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);

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

    QueryPipeline pipeline(table->write({}, metadata_snapshot, context, /*async_insert=*/false));

    PushingPipelineExecutor executor(pipeline);
    executor.push(block);
    executor.finish();

    return data;
}

// Returns all table data in Values format.
static std::string readData(DB::StoragePtr & table, const DB::ContextPtr context)
{
    using namespace DB;
    auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = table->getStorageSnapshot(metadata_snapshot, context);

    Names column_names;
    column_names.push_back("a");

    SelectQueryInfo query_info;
    QueryProcessingStage::Enum stage = table->getQueryProcessingStage(
        context, QueryProcessingStage::Complete, storage_snapshot, query_info);

    QueryPlan plan;
    table->read(plan, column_names, storage_snapshot, query_info, context, stage, 8192, 1);

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context))));

    Block sample;
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeUInt64>();
        col.name = "a";
        sample.insert(std::move(col));
    }

    tryRegisterFormats();

    WriteBufferFromOwnString out_buf;
    auto output = FormatFactory::instance().getOutputFormat("Values", out_buf, sample, context);
    pipeline.complete(output);

    Block data;

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
    // output->flush();

    out_buf.finalize();
    return out_buf.str();
}

TEST_F(StorageLogTest, testReadWrite)
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

/// A name that overflows `<name>.bin` in one path component of any filesystem CI runs on.
static String overlongColumnName()
{
    return String(static_cast<size_t>(NAME_MAX), 'c');
}

/// The bound the storage must report for `disk`: what fits in one path component once `.bin` is
/// accounted for. Derived here the same way the storage derives it, so the test pins the value
/// rather than the formula.
static size_t expectedStreamNameLimit(const DB::DiskPtr & disk)
{
    const auto probed = pathconf(disk->getPath().c_str(), _PC_NAME_MAX);
    const size_t name_max = probed == -1 ? NAME_MAX : static_cast<size_t>(probed);
    return name_max - std::string_view{".bin"}.size();
}

/// The diagnostic has to name the offending file and the exact bound, not just the error class, so
/// that a message which dropped or corrupted either still fails.
static void expectNamesFileAndLimit(const DB::Exception & e, const DB::DiskPtr & disk)
{
    using namespace DB;
    EXPECT_EQ(e.code(), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    const String & message = e.message();
    const String column = overlongColumnName();
    EXPECT_NE(message.find(column + ".bin"), String::npos) << message;
    EXPECT_NE(message.find("current length is " + std::to_string(column.length())), String::npos) << message;
    EXPECT_NE(
        message.find("max length of a stream name is " + std::to_string(expectedStreamNameLimit(disk))),
        String::npos) << message;
}

/// The state a table created before the DDL check existed loads in: the definition is accepted
/// because it comes from stored metadata, so only the runtime guards can refuse it.
static DB::StoragePtr createLegacyOverlongStorage(DB::DiskPtr & disk)
{
    using namespace DB;

    NamesAndTypesList names_and_types;
    names_and_types.emplace_back(overlongColumnName(), std::make_shared<DataTypeUInt64>());

    return std::make_shared<StorageLog>(
        "Log", disk, "table/", StorageID("test", "test"), ColumnsDescription{names_and_types},
        ConstraintsDescription{}, String{}, LoadingStrictnessLevel::ATTACH, /*is_fresh_definition=*/false,
        getContext().context);
}

/// Pins that a legacy table still loads: the DDL guard must stay exempt for stored metadata.
TEST(StorageLogOverlongName, legacyDefinitionStillLoads)
{
    DB::DiskPtr disk = createDisk("tmp_log_legacy_load/");
    ASSERT_NO_THROW(createLegacyOverlongStorage(disk));
    destroyDisk(disk);
}

/// The `StorageLog::write` guard. Without it the sink reaches the filesystem, which refuses the
/// name as an untyped `STD_EXCEPTION` that loses its own message.
TEST(StorageLogOverlongName, writeIsRefusedWithTypedError)
{
    using namespace DB;
    DB::DiskPtr disk = createDisk("tmp_log_legacy_write/");
    StoragePtr table = createLegacyOverlongStorage(disk);
    const auto context = getContext().context;

    /// The handle has to outlive the call: its rvalue conversion to StorageMetadataPtr is deleted.
    auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);

    try
    {
        table->write({}, metadata_snapshot, context, /*async_insert=*/false);
        FAIL() << "write() accepted a stream name that does not fit a path component";
    }
    catch (const Exception & e)
    {
        expectNamesFileAndLimit(e, disk);
    }

    destroyDisk(disk);
}

/// A backup that carries a stream name which does not fit the destination. Only the file name
/// matters: the guard runs before any entry is read, so no entry has to exist.
class BackupWithOverlongStreamName : public DB::IBackup
{
public:
    bool hasFiles(const String &) const override { return true; }
    bool fileExists(const String &) const override { return true; }

    /// Reaching this means the guard did not run, which is the failure this test pins.
    size_t copyFileToDisk(const String &, DB::DiskPtr, const String &, DB::WriteMode) const override
    {
        ADD_FAILURE() << "restoreDataImpl appended a file without checking the stream name length";
        return 0;
    }

    const String & getNameForLogging() const override { return name; }
    OpenMode getOpenMode() const override { return OpenMode::READ; }
    std::map<String, String> getEngineSettings() const override { return {}; }
    time_t getTimestamp() const override { return 0; }
    DB::UUID getUUID() const override { return {}; }
    const String & getBackupId() const override { return name; }
    std::shared_ptr<const IBackup> getBaseBackup() const override { return nullptr; }
    size_t getNumFiles() const override { return 1; }
    UInt64 getTotalSize() const override { return 0; }
    size_t getNumEntries() const override { return 1; }
    UInt64 getSizeOfEntries() const override { return 0; }
    UInt64 getUncompressedSize() const override { return 0; }
    UInt64 getCompressedSize() const override { return 0; }
    size_t getNumReadFiles() const override { return 0; }
    UInt64 getNumReadBytes() const override { return 0; }
    bool directoryExists(const String &) const override { return true; }
    DB::Strings listFiles(const String &, bool) const override { return {}; }
    bool fileExists(const SizeAndChecksum &) const override { return true; }
    UInt64 getFileSize(const String &) const override { return 0; }
    UInt128 getFileChecksum(const String &) const override { return {}; }
    SizeAndChecksum getFileSizeAndChecksum(const String &) const override { return {}; }
    std::unique_ptr<DB::ReadBufferFromFileBase> readFile(const String &) const override { return nullptr; }
    std::unique_ptr<DB::ReadBufferFromFileBase> readFile(const String &, const SizeAndChecksum &) const override { return nullptr; }
    size_t copyFileToDisk(const SizeAndChecksum &, DB::DiskPtr, const String &, DB::WriteMode) const override { return 0; }
    void writeFile(const DB::BackupFileInfo &, DB::BackupEntryPtr) override { }
    void setOriginalEndpointAndNamespaceIfEmpty(const String &, const String &) noexcept override { }
    bool supportsWritingInMultipleThreads() const override { return true; }
    void finalizeWriting() override { }
    bool setIsCorrupted() noexcept override { return true; }
    bool tryRemoveAllFiles() noexcept override { return true; }

private:
    const String name = "test_backup";
};

/// The `StorageLog::restoreDataImpl` guard. That path appends the backup's files directly, so it
/// bypasses the `write` guard entirely.
TEST(StorageLogOverlongName, restoreIsRefusedWithTypedError)
{
    using namespace DB;
    DB::DiskPtr disk = createDisk("tmp_log_legacy_restore/");
    StoragePtr table = createLegacyOverlongStorage(disk);

    try
    {
        std::static_pointer_cast<StorageLog>(table)->restoreDataImpl(
            std::make_shared<const BackupWithOverlongStreamName>(), "data", std::chrono::seconds{60});
        FAIL() << "restoreDataImpl accepted a stream name that does not fit a path component";
    }
    catch (const Exception & e)
    {
        expectNamesFileAndLimit(e, disk);
    }

    destroyDisk(disk);
}
