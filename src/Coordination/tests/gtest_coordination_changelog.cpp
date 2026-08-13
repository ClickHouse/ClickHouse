#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/KeeperLogStore.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>

#include <atomic>
#include <barrier>
#include <future>
#include <thread>


namespace DB
{
namespace FailPoints
{
    extern const char keeper_changelog_read_plan_resolved[];
    extern const char keeper_changelog_removed_from_disk_set[];
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 log_readahead_commit_window_bytes;
}
}

namespace ProfileEvents
{
    extern const Event KeeperLogsEntryReadFromFile;
    extern const Event KeeperLogsReadAheadFillDecodedEntries;
    extern const Event KeeperLogsReadAheadCursorsInstalled;
    extern const Event KeeperLogsReadAheadScheduleRejected;
    extern const Event KeeperLogsReadAheadReadersCreated;
    extern const Event KeeperLogsReadAheadTimeoutFallbacks;
    extern const Event KeeperLogsReadAheadPlanEpochMismatches;
    extern const Event KeeperLogsEntryReadFromCommitReadAhead;
    extern const Event KeeperLogsEntryReadFromLatestCache;
}


template<typename TestType>
class CoordinationChangelogTest : public ::testing::Test
{
public:
    static constexpr bool enable_compression = TestType::enable_compression;
    std::string extension;

    DB::KeeperContextPtr keeper_context;
    LoggerPtr log{getLogger("CoordinationChangelogTest")};

    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel("trace");

        auto settings = std::make_shared<DB::CoordinationSettings>();
        keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
        keeper_context->setLocalLogsPreprocessed();
        extension = enable_compression ? ".zstd" : "";
    }

    void setLogDirectory(const std::string & path) { keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", path)); }
};

template <bool enable_compression_>
struct ChangelogTestParam
{
    static constexpr bool enable_compression = enable_compression_;
};

using ChangelogImplementation = testing::Types<ChangelogTestParam<true>, ChangelogTestParam<false>>;

TYPED_TEST_SUITE(CoordinationChangelogTest, ChangelogImplementation);

// Guards against pool_threads < max_peer_readers, which can pin every pool thread to a parked fill
// task and starve the commit reader. Plain TEST: exercises a free function, no changelog fixture needed.
TEST(ReadAheadSettingsValidation, RejectsPoolThreadsBelowMaxPeerReaders)
{
    DB::ReadAheadSettings settings;
    settings.max_peer_readers = 8;
    settings.pool_threads = 4;

    EXPECT_THROW(DB::validateReadAheadSettings(settings), DB::Exception);

    // Equal to max_peer_readers is the minimum accepted explicit value.
    settings.pool_threads = 8;
    EXPECT_NO_THROW(DB::validateReadAheadSettings(settings));

    // Above max_peer_readers is accepted.
    settings.pool_threads = 16;
    EXPECT_NO_THROW(DB::validateReadAheadSettings(settings));

    // 0 means "derive from max_peer_readers" and is always accepted.
    settings.pool_threads = 0;
    EXPECT_NO_THROW(DB::validateReadAheadSettings(settings));
}

// commit_logs_cache_size_threshold is OBSOLETE but must map onto log_readahead_commit_window_bytes
// when the latter isn't itself set.
TEST(CoordinationSettingsLoadFromConfig, MapsObsoleteCommitLogsCacheSizeThreshold)
{
    const auto make_config = []
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;
        config->setString("keeper_server.coordination_settings", "");
        return config;
    };

    const auto get_commit_window_bytes_from_config = [](const auto & config)
    {
        DB::CoordinationSettings settings;
        settings.loadFromConfig("keeper_server.coordination_settings", *config);
        return settings[DB::CoordinationSetting::log_readahead_commit_window_bytes];
    };

    {
        SCOPED_TRACE("only old setting set, to 0");
        auto config = make_config();
        config->setString("keeper_server.coordination_settings.commit_logs_cache_size_threshold", "0");
        EXPECT_EQ(get_commit_window_bytes_from_config(config), 0u);
    }

    {
        SCOPED_TRACE("both settings set, new wins");
        auto config = make_config();
        config->setString("keeper_server.coordination_settings.commit_logs_cache_size_threshold", "0");
        config->setString("keeper_server.coordination_settings.log_readahead_commit_window_bytes", "1048576");
        EXPECT_EQ(get_commit_window_bytes_from_config(config), 1048576u);
    }

    {
        SCOPED_TRACE("neither setting set, default");
        auto config = make_config();
        EXPECT_EQ(get_commit_window_bytes_from_config(config), 500ull * 1024 * 1024);
    }
}

TEST(ChangelogValidRuns, ForwardGapAfterCompactionStartsNewRun)
{
    /// Regression: compaction drops pending locations below its new start before refreshCache folds
    /// them in, so the next located record has a forward gap. It must open a fresh run, not throw.
    DB::ChangelogFileDescription::ValidRuns runs;
    runs.addLocatedRecord(1597, /*position=*/0, /*size_in_file=*/10);
    runs.addLocatedRecord(1598, /*position=*/10, /*size_in_file=*/10);

    /// end_index is now 1599; index 1600 skips the compacted 1599.
    EXPECT_NO_THROW(runs.addLocatedRecord(1600, /*position=*/30, /*size_in_file=*/10));

    ASSERT_EQ(runs.runs.size(), 2u);
    EXPECT_EQ(runs.runs[0].first_index, 1597u);
    EXPECT_EQ(runs.runs[0].start_position, 0u);
    EXPECT_EQ(runs.runs[1].first_index, 1600u);
    EXPECT_EQ(runs.runs[1].start_position, 30u);
    EXPECT_EQ(runs.end_index, 1601u);
    EXPECT_EQ(runs.end_position, 40u);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestSimple)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);
    auto entry = getLogEntry("hello world", 77);
    changelog.append(entry);
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.next_slot(), 2);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.last_entry()->get_term(), 77);
    EXPECT_EQ(changelog.entry_at(1)->get_term(), 77);
    EXPECT_EQ(changelog.log_entries(1, 2)->size(), 1);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestFlushThrottling)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1000},
        DB::FlushSettings{.max_flush_batch_size = 1000, .min_time_between_fsyncs_ms = 100},
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    Stopwatch watch;

    /// The first flush is not throttled.
    auto entry = getLogEntry("hello world", 77);
    changelog.append(entry);
    EXPECT_TRUE(changelog.flush());

    /// The second flush must not start earlier than min_time_between_fsyncs_ms
    /// after the start of the first one.
    changelog.append(entry);
    EXPECT_TRUE(changelog.flush());

    EXPECT_GE(watch.elapsedMilliseconds(), 100);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestFile)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);
    auto entry = getLogEntry("hello world", 77);
    changelog.append(entry);
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    for (const auto & p : fs::directory_iterator("./logs"))
        EXPECT_EQ(p.path(), "./logs/changelog_1_5.bin" + this->extension);

    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
}

TEST_P(CoordinationTestWithCompression, ChangelogReadWrite)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1000},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 10);

    waitDurableLogs(changelog);

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1000},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(0, 0);
    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), changelog.last_entry()->get_term());
    EXPECT_EQ(changelog_reader.start_index(), changelog.start_index());
    EXPECT_EQ(changelog_reader.next_slot(), changelog.next_slot());

    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ(changelog_reader.entry_at(i + 1)->get_term(), changelog.entry_at(i + 1)->get_term());

    auto entries_from_range_read = changelog_reader.log_entries(1, 11);
    auto entries_from_range = changelog.log_entries(1, 11);
    EXPECT_EQ(entries_from_range_read->size(), entries_from_range->size());
    EXPECT_EQ(10, entries_from_range->size());
}

TEST_P(CoordinationTestWithCompression, ChangelogWriteAt)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1000},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }

    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 10);

    auto entry = getLogEntry("writer", 77);
    changelog.write_at(7, entry);
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.last_entry()->get_term(), 77);
    EXPECT_EQ(changelog.entry_at(7)->get_term(), 77);
    EXPECT_EQ(changelog.next_slot(), 8);

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1000},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(0, 0);

    EXPECT_EQ(changelog_reader.size(), changelog.size());
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), changelog.last_entry()->get_term());
    EXPECT_EQ(changelog_reader.start_index(), changelog.start_index());
    EXPECT_EQ(changelog_reader.next_slot(), changelog.next_slot());
}


TEST_P(CoordinationTestWithCompression, ChangelogTestAppendAfterRead)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);
    for (size_t i = 0; i < 7; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 7);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(0, 0);

    EXPECT_EQ(changelog_reader.size(), 7);
    for (size_t i = 7; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog_reader.append(entry);
    }
    changelog_reader.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog_reader.size(), 10);

    waitDurableLogs(changelog_reader);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));

    size_t logs_count = 0;
    for (const auto & _ [[maybe_unused]] : fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 2);

    auto entry = getLogEntry("someentry", 77);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog_reader.size(), 11);

    waitDurableLogs(changelog_reader);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));

    logs_count = 0;
    for (const auto & _ [[maybe_unused]] : fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 3);
}

namespace
{

}

TEST_P(CoordinationTestWithCompression, ChangelogTestCompaction)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 3; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_EQ(changelog.size(), 3);

    this->keeper_context->setLastCommitIndex(2);
    changelog.compact(2);

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 3);
    EXPECT_EQ(changelog.next_slot(), 4);
    EXPECT_EQ(changelog.last_entry()->get_term(), 20);
    // nothing should be deleted
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));

    auto e1 = getLogEntry("hello world", 30);
    changelog.append(e1);
    auto e2 = getLogEntry("hello world", 40);
    changelog.append(e2);
    auto e3 = getLogEntry("hello world", 50);
    changelog.append(e3);
    auto e4 = getLogEntry("hello world", 60);
    changelog.append(e4);
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));

    this->keeper_context->setLastCommitIndex(6);
    changelog.compact(6);
    std::this_thread::sleep_for(std::chrono::microseconds(1000));

    assertFileDeleted("./logs/changelog_1_5.bin" + this->extension);
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 7);
    EXPECT_EQ(changelog.next_slot(), 8);
    EXPECT_EQ(changelog.last_entry()->get_term(), 60);
    /// And we able to read it
    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(6, 0);

    EXPECT_EQ(changelog_reader.size(), 1);
    EXPECT_EQ(changelog_reader.start_index(), 7);
    EXPECT_EQ(changelog_reader.next_slot(), 8);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 60);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestBatchOperations)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 10);

    waitDurableLogs(changelog);

    auto entries = changelog.pack(1, 5);

    DB::KeeperLogStore apply_changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    apply_changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(apply_changelog.entry_at(i + 1)->get_term(), i * 10);
    }
    EXPECT_EQ(apply_changelog.size(), 10);

    apply_changelog.apply_pack(8, *entries);
    apply_changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(apply_changelog.size(), 12);
    EXPECT_EQ(apply_changelog.start_index(), 1);
    EXPECT_EQ(apply_changelog.next_slot(), 13);

    for (size_t i = 0; i < 7; ++i)
    {
        EXPECT_EQ(apply_changelog.entry_at(i + 1)->get_term(), i * 10);
    }

    EXPECT_EQ(apply_changelog.entry_at(8)->get_term(), 0);
    EXPECT_EQ(apply_changelog.entry_at(9)->get_term(), 10);
    EXPECT_EQ(apply_changelog.entry_at(10)->get_term(), 20);
    EXPECT_EQ(apply_changelog.entry_at(11)->get_term(), 30);
    EXPECT_EQ(apply_changelog.entry_at(12)->get_term(), 40);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestBatchOperationsEmpty)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    nuraft::ptr<nuraft::buffer> entries;
    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);
        for (size_t i = 0; i < 10; ++i)
        {
            auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);

        EXPECT_EQ(changelog.size(), 10);

        waitDurableLogs(changelog);

        entries = changelog.pack(5, 5);
    }

    ChangelogDirTest test1("./logs1");
    this->setLogDirectory("./logs1");
    DB::KeeperLogStore changelog_new(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_new.init(0, 0);
    EXPECT_EQ(changelog_new.size(), 0);

    changelog_new.apply_pack(5, *entries);
    changelog_new.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog_new.size(), 5);
    EXPECT_EQ(changelog_new.start_index(), 5);
    EXPECT_EQ(changelog_new.next_slot(), 10);

    for (size_t i = 4; i < 9; ++i)
        EXPECT_EQ(changelog_new.entry_at(i + 1)->get_term(), i * 10);

    auto e = getLogEntry("hello_world", 110);
    changelog_new.append(e);
    changelog_new.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog_new.size(), 6);
    EXPECT_EQ(changelog_new.start_index(), 5);
    EXPECT_EQ(changelog_new.next_slot(), 11);

    waitDurableLogs(changelog_new);

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(5, 0);
}


TEST_P(CoordinationTestWithCompression, ChangelogTestWriteAtPreviousFile)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + this->extension));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(7, e1);
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 8);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + this->extension));

    DB::KeeperLogStore changelog_read(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_read.init(0, 0);
    EXPECT_EQ(changelog_read.size(), 7);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 8);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestWriteAtFileBorder)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + this->extension));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(11, e1);
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 11);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 12);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + this->extension));

    DB::KeeperLogStore changelog_read(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_read.init(0, 0);
    EXPECT_EQ(changelog_read.size(), 11);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 12);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestWriteAtAllFiles)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);
    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + this->extension));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(1, e1);
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 2);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + this->extension));
}

TEST_P(CoordinationTestWithCompression, ChangelogTestStartNewLogAfterRead)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 35);

    waitDurableLogs(changelog);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_36_40.bin" + this->extension));

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(0, 0);

    auto entry = getLogEntry("36_hello_world", 360);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog_reader.size(), 36);

    waitDurableLogs(changelog_reader);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_36_40.bin" + this->extension));
}

namespace
{
void assertBrokenFileRemoved(const fs::path & directory, const fs::path & filename)
{
    EXPECT_FALSE(fs::exists(directory / filename));
    // broken files are sent to the detached/{timestamp} folder
    // we don't know timestamp so we iterate all of them
    for (const auto & dir_entry : fs::recursive_directory_iterator(directory / "detached"))
    {
        if (dir_entry.path().filename() == filename)
            return;
    }

    FAIL() << "Broken log " << filename << " was not moved to the detached folder";
}

}

TEST_P(CoordinationTestWithCompression, ChangelogTestReadAfterBrokenTruncate)
{
    static const fs::path log_folder{"./logs"};


    ChangelogDirTest test(log_folder);
    this->setLogDirectory(log_folder);

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 35);

    waitDurableLogs(changelog);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + this->extension));

    DB::WriteBufferFromFile plain_buf(
        "./logs/changelog_11_15.bin" + this->extension, DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(0);
    plain_buf.finalize();

    {
        DB::KeeperLogStore changelog_reader(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        ASSERT_THROW(changelog_reader.init(0, 0), DB::Exception);
    }

    fs::remove(log_folder / ("changelog_16_20.bin" + this->extension));
    fs::remove(log_folder / ("changelog_21_25.bin" + this->extension));
    fs::remove(log_folder / ("changelog_26_30.bin" + this->extension));
    fs::remove(log_folder / ("changelog_31_35.bin" + this->extension));

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(0, 0);
    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 90);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));

    auto entry = getLogEntry("h", 7777);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog_reader.size(), 11);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 7777);

    waitDurableLogs(changelog_reader);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + this->extension));

    DB::KeeperLogStore changelog_reader2(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader2.init(0, 0);
    EXPECT_EQ(changelog_reader2.size(), 11);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

/// Truncating all entries
TEST_P(CoordinationTestWithCompression, ChangelogTestReadAfterBrokenTruncate2)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_40.bin" + this->extension));

    DB::WriteBufferFromFile plain_buf(
        "./logs/changelog_1_20.bin" + this->extension, DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(30);
    plain_buf.finalize();

    {
        DB::KeeperLogStore changelog_reader(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        ASSERT_THROW(changelog_reader.init(0, 0), DB::Exception);
    }

    fs::remove("./logs/changelog_21_40.bin" + this->extension);

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(0, 0);

    EXPECT_EQ(changelog_reader.size(), 0);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin" + this->extension));
    auto entry = getLogEntry("hello_world", 7777);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);

    waitDurableLogs(changelog_reader);

    EXPECT_EQ(changelog_reader.size(), 1);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 7777);

    DB::KeeperLogStore changelog_reader2(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader2.init(0, 0);
    EXPECT_EQ(changelog_reader2.size(), 1);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

/// Truncating only some entries from the end
/// For compressed logs we have no reliable way of knowing how many log entries were lost
/// after we truncate some bytes from the end
TEST_P(CoordinationTestWithCompression, ChangelogTestReadAfterBrokenTruncate3)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }

    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_40.bin"));

    DB::WriteBufferFromFile plain_buf(
        "./logs/changelog_1_20.bin", DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(plain_buf.size() - 30);
    plain_buf.finalize();

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_reader.init(0, 0);

    EXPECT_EQ(changelog_reader.size(), 19);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin"));
    assertBrokenFileRemoved("./logs", "changelog_21_40.bin");
    EXPECT_TRUE(fs::exists("./logs/changelog_20_39.bin"));
    auto entry = getLogEntry("hello_world", 7777);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);

    waitDurableLogs(changelog_reader);

    EXPECT_EQ(changelog_reader.size(), 20);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 7777);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestMixedLogTypes)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    std::vector<std::string> changelog_files;

    const auto verify_changelog_files = [&]
    {
        for (const auto & log_file : changelog_files)
            EXPECT_TRUE(fs::exists(log_file)) << "File " << log_file << " not found";
    };

    size_t last_term = 0;
    size_t log_size = 0;

    const auto append_log = [&](auto & changelog, const std::string & data, uint64_t term)
    {
        last_term = term;
        ++log_size;
        auto entry = getLogEntry(data, last_term);
        changelog.append(entry);
    };

    const auto verify_log_content = [&](const auto & changelog)
    {
        EXPECT_EQ(changelog.size(), log_size);
        EXPECT_EQ(changelog.last_entry()->get_term(), last_term);
    };

    {
        SCOPED_TRACE("Initial uncompressed log");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        for (size_t i = 0; i < 35; ++i)
            append_log(changelog, std::to_string(i) + "_hello_world", (i+ 44) * 10);

        changelog.end_of_append_batch(0, 0);

        waitDurableLogs(changelog);
        changelog_files.push_back("./logs/changelog_1_20.bin");
        changelog_files.push_back("./logs/changelog_21_40.bin");
        verify_changelog_files();

        verify_log_content(changelog);
    }

    {
        SCOPED_TRACE("Compressed log");
        DB::KeeperLogStore changelog_compressed(
            DB::LogFileSettings{.force_sync = true, .compress_logs = true, .rotate_interval = 20},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog_compressed.init(0, 0);

        verify_changelog_files();
        verify_log_content(changelog_compressed);

        append_log(changelog_compressed, "hello_world", 7777);
        changelog_compressed.end_of_append_batch(0, 0);

        waitDurableLogs(changelog_compressed);

        verify_log_content(changelog_compressed);

        changelog_files.push_back("./logs/changelog_36_55.bin.zstd");
        verify_changelog_files();
    }

    {
        SCOPED_TRACE("Final uncompressed log");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        verify_changelog_files();
        verify_log_content(changelog);

        append_log(changelog, "hello_world", 7778);
        changelog.end_of_append_batch(0, 0);

        waitDurableLogs(changelog);

        verify_log_content(changelog);

        changelog_files.push_back("./logs/changelog_37_56.bin");
        verify_changelog_files();
    }
}

TEST_P(CoordinationTestWithCompression, ChangelogTestLostFiles)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_40.bin" + this->extension));

    fs::remove("./logs/changelog_1_20.bin" + this->extension);

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);

    ASSERT_THROW(changelog_reader.init(5, 0), DB::Exception);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestLostFiles2)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 10},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    waitDurableLogs(changelog);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_10.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_20.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_30.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_40.bin" + this->extension));

    // we have a gap in our logs, we need to remove all the logs after the gap
    fs::remove("./logs/changelog_21_30.bin" + this->extension);

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 10},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    ASSERT_THROW(changelog_reader.init(5, 0), DB::Exception);
}

TEST_P(CoordinationTestWithCompression, TestRotateIntervalChanges)
{
    using namespace Coordination;

    ChangelogDirTest snapshots("./logs");
    this->setLogDirectory("./logs");
    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);

        changelog.init(0, 3);
        for (size_t i = 1; i < 55; ++i)
        {
            std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
            request->path = "/hello_" + std::to_string(i);
            auto entry = getLogEntryFromZKRequest(0, 1, i, request);
            changelog.append(entry);
            changelog.end_of_append_batch(0, 0);
        }

        waitDurableLogs(changelog);
    }


    EXPECT_TRUE(fs::exists("./logs/changelog_1_100.bin" + this->extension));

    DB::KeeperLogStore changelog_1(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 10},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_1.init(0, 50);
    for (size_t i = 0; i < 55; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(100 + i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog_1.append(entry);
        changelog_1.end_of_append_batch(0, 0);
    }

    waitDurableLogs(changelog_1);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_100.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_101_110.bin" + this->extension));

    DB::KeeperLogStore changelog_2(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 7},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_2.init(98, 55);

    for (size_t i = 0; i < 17; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(200 + i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog_2.append(entry);
        changelog_2.end_of_append_batch(0, 0);
    }

    waitDurableLogs(changelog_2);

    this->keeper_context->setLastCommitIndex(105);
    changelog_2.compact(105);
    std::this_thread::sleep_for(std::chrono::microseconds(1000));

    assertFileDeleted("./logs/changelog_1_100.bin" + this->extension);
    EXPECT_TRUE(fs::exists("./logs/changelog_101_110.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_111_117.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_118_124.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_125_131.bin" + this->extension));

    DB::KeeperLogStore changelog_3(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog_3.init(116, 3);
    for (size_t i = 0; i < 17; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(300 + i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog_3.append(entry);
        changelog_3.end_of_append_batch(0, 0);
    }

    waitDurableLogs(changelog_3);

    this->keeper_context->setLastCommitIndex(125);
    changelog_3.compact(125);
    std::this_thread::sleep_for(std::chrono::microseconds(1000));
    assertFileDeleted("./logs/changelog_101_110.bin" + this->extension);
    assertFileDeleted("./logs/changelog_111_117.bin" + this->extension);
    assertFileDeleted("./logs/changelog_118_124.bin" + this->extension);

    EXPECT_TRUE(fs::exists("./logs/changelog_125_131.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_132_136.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_137_141.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_142_146.bin" + this->extension));
}

TEST_P(CoordinationTestWithCompression, ChangelogTestMaxLogSize)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    uint64_t last_entry_index{0};
    size_t i{0};
    {
        SCOPED_TRACE("Small rotation interval, big size limit");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{
                .force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20, .max_size = 50 * 1024 * 1024},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        for (; i < 100; ++i)
        {
            auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
            last_entry_index = changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);

        waitDurableLogs(changelog);

        ASSERT_EQ(changelog.entry_at(last_entry_index)->get_term(), (i - 1 + 44) * 10);
    }
    {
        SCOPED_TRACE("Large rotation interval, small size limit");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{
                .force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100'000, .max_size = 4000},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        ASSERT_EQ(changelog.entry_at(last_entry_index)->get_term(), (i - 1 + 44) * 10);

        for (; i < 500; ++i)
        {
            auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
            last_entry_index = changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);

        waitDurableLogs(changelog);

        ASSERT_EQ(changelog.entry_at(last_entry_index)->get_term(), (i - 1 + 44) * 10);
    }
    {
        SCOPED_TRACE("Final verify all logs");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{
                .force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100'000, .max_size = 4000},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);
        ASSERT_EQ(changelog.entry_at(last_entry_index)->get_term(), (i - 1 + 44) * 10);
    }
}

TEST_P(CoordinationTestWithCompression, TestCompressedLogsMultipleRewrite)
{
    using namespace Coordination;
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");
    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);

    changelog.init(0, 3);
    for (size_t i = 1; i < 55; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
    }

    waitDurableLogs(changelog);

    DB::KeeperLogStore changelog1(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog1.init(0, 3);
    for (size_t i = 55; i < 70; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog1.append(entry);
        changelog1.end_of_append_batch(0, 0);
    }

    waitDurableLogs(changelog1);

    DB::KeeperLogStore changelog2(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog2.init(0, 3);
    for (size_t i = 70; i < 80; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog2.append(entry);
        changelog2.end_of_append_batch(0, 0);
    }
}

TEST_P(CoordinationTestWithCompression, ChangelogInsertThreeTimesSmooth)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");
    {
        SCOPED_TRACE("================First time=====================");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 2);
        waitDurableLogs(changelog);
    }

    {
        SCOPED_TRACE("================Second time=====================");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 3);
        waitDurableLogs(changelog);
    }

    {
        SCOPED_TRACE("================Third time=====================");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 4);
        waitDurableLogs(changelog);
    }

    {
        SCOPED_TRACE("================Fourth time=====================");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 5);
        waitDurableLogs(changelog);
    }
}


TEST_P(CoordinationTestWithCompression, ChangelogInsertMultipleTimesSmooth)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");
    for (size_t i = 0; i < 36; ++i)
    {
        SCOPED_TRACE("================First time=====================");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);
        for (size_t j = 0; j < 7; ++j)
        {
            auto entry = getLogEntry("hello_world", 7);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);
        waitDurableLogs(changelog);
    }

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(0, 0);
    EXPECT_EQ(changelog.next_slot(), 36 * 7 + 1);
}

TEST_P(CoordinationTestWithCompression, ChangelogInsertThreeTimesHard)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");
    {
        SCOPED_TRACE("================First time=====================");
        DB::KeeperLogStore changelog1(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog1.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog1.append(entry);
        changelog1.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog1.next_slot(), 2);
        waitDurableLogs(changelog1);
    }

    {
        SCOPED_TRACE("================Second time=====================");
        DB::KeeperLogStore changelog2(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog2.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog2.append(entry);
        changelog2.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog2.next_slot(), 3);
        waitDurableLogs(changelog2);
    }

    {
        SCOPED_TRACE("================Third time=====================");
        DB::KeeperLogStore changelog3(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog3.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog3.append(entry);
        changelog3.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog3.next_slot(), 4);
        waitDurableLogs(changelog3);
    }

    {
        SCOPED_TRACE("================Fourth time=====================");
        DB::KeeperLogStore changelog4(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog4.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog4.append(entry);
        changelog4.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog4.next_slot(), 5);
        waitDurableLogs(changelog4);
    }
}

TEST_P(CoordinationTestWithCompression, TestLogGap)
{
    using namespace Coordination;
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);

    changelog.init(0, 3);
    for (size_t i = 1; i < 55; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
    }

    /// append/end_of_append_batch flush asynchronously on a background thread. Wait for the
    /// log to be durable before opening a second store that reads the same file, otherwise
    /// the reader races the writer.
    waitDurableLogs(changelog);

    DB::KeeperLogStore changelog1(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog1.init(60, 3);

    /// Logs discarded
    EXPECT_FALSE(fs::exists("./logs/changelog_1_100.bin" + this->extension));
    EXPECT_EQ(changelog1.start_index(), 61);
    EXPECT_EQ(changelog1.next_slot(), 61);
}

TEST_P(CoordinationTestWithCompression, ChangelogTestBrokenWriteAt)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Body always writes with compress_logs=false; running under the compressed type "
                        "parameter would be a duplicate.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        for (size_t i = 0; i < 20; ++i)
        {
            auto entry = getLogEntry(std::to_string(i) + "_hello_world", 1);
            changelog.append(entry);
        }

        changelog.end_of_append_batch(0, 0);

        waitDurableLogs(changelog);
        EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin"));
    }

    DB::WriteBufferFromFile plain_buf(
        "./logs/changelog_1_20.bin", DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(plain_buf.size() - 3);
    plain_buf.finalize();

    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        for (size_t i = 20; i < 25; ++i)
        {
            auto entry = getLogEntry(std::to_string(i) + "_hello_world", 1);
            changelog.append(entry);
        }

        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.size(), 24);
        waitDurableLogs(changelog);

        auto entry = getLogEntry(std::to_string(19) + "_hello_world", 2);
        changelog.write_at(18, entry);
        changelog.end_of_append_batch(0, 0);
        waitDurableLogs(changelog);

        for (size_t i = 19; i < 25; ++i)
        {
            entry = getLogEntry(std::to_string(i) + "_hello_world", 2);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);
        waitDurableLogs(changelog);
    }

    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        EXPECT_EQ(changelog.size(), 24);
    }
}

TEST_P(CoordinationTestWithCompression, ChangelogLoadingFromInvalidName)
{
    if (this->enable_compression)
        GTEST_SKIP() << "The renamed changelog path below hardcodes the .bin extension, which is "
                        "incompatible with the .zstd suffix a compressed changelog would need.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{
                .force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100'000, .max_size = 500},
            DB::FlushSettings(),
            DB::ReadAheadSettings{},
            this->keeper_context);
        changelog.init(0, 0);

        EXPECT_TRUE(fs::exists("./logs/changelog_1_100000.bin"));
        for (size_t i = 0; i < 500; ++i)
        {
            auto entry = getLogEntry(std::to_string(i) + "_hello_world", 1);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);

        waitDurableLogs(changelog);
    }

    // Find file starting with "changelog_1_" (renamed because of file size limit)
    fs::path new_changelog_path;
    for (const auto & entry : fs::directory_iterator("./logs"))
    {
        if (entry.is_regular_file())
        {
            const auto filename = entry.path().filename().string();
            if (filename.starts_with("changelog_1_"))
                new_changelog_path = entry.path();
        }
    }

    ASSERT_NE(new_changelog_path, fs::path{});

    fs::rename(new_changelog_path, "./logs/changelog_1_100000.bin");

    std::cout << new_changelog_path << std::endl;

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100'000, .max_size = 500},
        DB::FlushSettings(),
        DB::ReadAheadSettings{},
        this->keeper_context);
    changelog.init(15, 0);

    ASSERT_EQ(changelog.next_slot(), 501);
}

// Tests: PLAN/EXECUTE split + removed_from_disk fence

// Verify the PLAN/EXECUTE split releases changelog_lock before disk I/O.
// An append must complete while a reader is parked between PLAN and EXECUTE.
TYPED_TEST(CoordinationChangelogTest, ConcurrentAppendWhileHistoricalReadPaused)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Lock behaviour is independent of compression; body always uses compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    // Tiny cache forces entries to disk.
    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 100,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);


    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("data", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;

    std::thread reader([&]
    {
        // log_entries_ext calls getReadPlan (under shared lock), then
        // pauseFailPoint (no lock held), then executeReadPlan.
        auto entries = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, DB::KeeperLogStore::NO_PEER_ID);
        entries_promise.set_value(std::move(entries));
    });

    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

    std::promise<void> append_done_promise;
    std::thread appender([&]
    {
        auto entry = getLogEntry("new_entry", 11);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        append_done_promise.set_value();
    });

    auto append_future = append_done_promise.get_future();
    ASSERT_EQ(append_future.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "append deadlocked — changelog_lock was held across the EXECUTE disk read";

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    reader.join();
    appender.join();

    auto entries = entries_promise.get_future().get();
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*entries)[i]->get_term(), static_cast<uint64_t>(i + 1));
}


// Verify removed_from_disk fence: compaction between PLAN and EXECUTE must
// produce nullptr (snapshot fallback), not a throw or a read of a deleted file.
TYPED_TEST(CoordinationChangelogTest, CompactionRemovesFileAfterPlanBeforeRead)
{
    if (this->enable_compression)
        GTEST_SKIP() << "The compaction/removed_from_disk race is independent of compression; body always "
                        "uses compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 5,
        .latest_logs_cache_size_threshold = 1,
    };

    // Write+close, then re-derive from disk so B1's compaction race hits an actual disk read, not an
    // in-memory cache hit that would be served regardless of compact()'s effect on the file.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), DB::ReadAheadSettings{.commit_window_bytes = 1}, this->keeper_context);
        writer.init(0, 0);
        for (size_t i = 0; i < 10; ++i)
        {
            auto entry = getLogEntry("d", static_cast<size_t>(i + 1));
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), DB::ReadAheadSettings{.commit_window_bytes = 1}, this->keeper_context);
    changelog.init(0, 0);

    {
        SCOPED_TRACE("Sub-case B1: compact first file after PLAN, before EXECUTE");
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);
        // compact() defers the disk unlink to a background thread; pause at removed_from_disk_set and
        // drain it deterministically so the race is not flaky (same idiom as CommitReadAheadCompactionFallback).
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);

        std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;

        std::thread reader([&]
        {
            auto entries = changelog.log_entries_ext(1, 4, /*batch_size_hint_in_bytes=*/0, DB::KeeperLogStore::NO_PEER_ID);
            entries_promise.set_value(std::move(entries));
        });

        DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

        this->keeper_context->setLastCommitIndex(5);
        changelog.compact(5); // schedules async removal of changelog_1_5.bin

        DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_removed_from_disk_set);
        DB::FailPointInjection::notifyFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);
        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);

        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

        reader.join();

        EXPECT_EQ(entries_promise.get_future().get(), nullptr) << "Compacted-before-EXECUTE range must return nullptr, not throw";
    }

    {
        SCOPED_TRACE("Sub-case B2: fully compacted store must return nullptr, not throw");
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);

        this->keeper_context->setLastCommitIndex(10);
        changelog.compact(10); // schedules async removal of the one remaining changelog file

        // Deterministically wait for the async removal to finish instead of a fixed sleep.
        DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_removed_from_disk_set);
        DB::FailPointInjection::notifyFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);
        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);

        auto entries = changelog.log_entries_ext(1, 4, 0, DB::KeeperLogStore::NO_PEER_ID);
        EXPECT_EQ(entries, nullptr);
    }
}


// write_at never physically overwrites old records on disk (see ChangelogFileDescription::ValidRuns), so a
// plan built before write_at, served after, can silently decode stale-but-valid old records unless
// executeReadPlan's truncation-epoch check discards it.
TYPED_TEST(CoordinationChangelogTest, WriteAtRaceHistoricalRead)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("d", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;

    std::thread reader([&]
    {
        try
        {
            auto entries = changelog.log_entries_ext(1, 6, 0, DB::KeeperLogStore::NO_PEER_ID);
            entries_promise.set_value(std::move(entries));
        }
        catch (...) // Ok: promise handles exception-case via nullptr
        {
            entries_promise.set_value(nullptr);
        }
    });

    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

    auto new_entry = getLogEntry("overwrite", 999);
    changelog.write_at(5, new_entry);
    changelog.end_of_append_batch(0, 0);

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);
    reader.join();

    auto entries = entries_promise.get_future().get();
    // The plan was fully built (and epoch-stamped) before write_at ran, so the truncation-epoch check must discard it.
    EXPECT_EQ(entries, nullptr) << "A plan built before write_at must not be served with possibly-stale content afterwards";
    if (entries != nullptr && !entries->empty())
    {
        // Defensive: whatever is returned must never mix pre- and post-truncation terms in one batch.
        bool seen_new_term = false;
        for (size_t i = 0; i < entries->size(); ++i)
        {
            const auto term = (*entries)[i]->get_term();
            EXPECT_TRUE(term == static_cast<uint64_t>(i + 1) || term == 999u) << "Unexpected term " << term << " at position " << i;
            if (term == 999u)
                seen_new_term = true;
            else
                EXPECT_FALSE(seen_new_term) << "Old-term entry served after a new-term entry at position " << i;
        }
    }

    // A follow-up read (fresh plan, current epoch) must observe the rewritten entry.
    auto follow_up = changelog.log_entries_ext(1, 6, 0, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(follow_up, nullptr);
    ASSERT_EQ(follow_up->size(), 5u);
    for (size_t i = 0; i < 4; ++i)
        EXPECT_EQ((*follow_up)[i]->get_term(), static_cast<uint64_t>(i + 1));
    EXPECT_EQ((*follow_up)[4]->get_term(), 999u);
}

// Same truncation-epoch race as WriteAtRaceHistoricalRead, but on the peer read-ahead path
// (getReadAheadPlan/serveReadAhead) instead of the direct one.
TYPED_TEST(CoordinationChangelogTest, ReadAheadPeerWriteAtEpochRace)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 100,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = true, .max_peer_readers = 8, .serve_wait_timeout_ms = 5000, .chunk_size = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (uint64_t i = 1; i <= 10; ++i)
    {
        auto entry = getLogEntry("epoch_race_peer_" + std::to_string(i), i);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;
    std::thread reader([&]
    {
        entries_promise.set_value(changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/7));
    });

    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

    // Truncate at index 3 (inside the paused [1,6) plan) and rewrite 3..10 with distinct new terms,
    // simulating a leader-change log conflict resolution racing a follower catch-up read.
    auto rewrite_entry = getLogEntry("epoch_race_peer_rewrite_3", 999);
    changelog.write_at(3, rewrite_entry);
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);
    for (uint64_t i = 4; i <= 10; ++i)
    {
        auto entry = getLogEntry("epoch_race_peer_new_" + std::to_string(i), 900 + i);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);
    changelog.end_of_append_batch(0, 0);

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);
    reader.join();

    auto entries = entries_promise.get_future().get();
    ASSERT_EQ(entries, nullptr)
        << "A read-ahead plan built before write_at must not be served with possibly pre-truncation content";

    // A follow-up read (fresh plan, current epoch) must observe the rewritten content consistently --
    // never the pre-truncation terms.
    auto follow_up = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/7);
    ASSERT_NE(follow_up, nullptr);
    ASSERT_EQ(follow_up->size(), 5u);
    EXPECT_EQ((*follow_up)[0]->get_term(), 1u);
    EXPECT_EQ((*follow_up)[1]->get_term(), 2u);
    EXPECT_EQ((*follow_up)[2]->get_term(), 999u);
    EXPECT_EQ((*follow_up)[3]->get_term(), 904u);
    EXPECT_EQ((*follow_up)[4]->get_term(), 905u);
}

// Same truncation-epoch race, but on the commit path: index 2 is always safe (Raft never truncates a committed
// entry), but the plan's speculative cursors past it must not get installed into the commit reader with
// pre-truncation file positions.
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadWriteAtEpochRace)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 100,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = false, .serve_wait_timeout_ms = 5000, .commit_window_bytes = 64 * 1024 * 1024},
        this->keeper_context);
    changelog.init(0, 0);

    for (uint64_t i = 1; i <= 10; ++i)
    {
        auto entry = getLogEntry("epoch_race_commit_" + std::to_string(i), i);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);
    // A second end_of_append_batch after waiting for durability is needed to apply the locations
    // registered during the wait and evict index 2 out of latest_logs_cache.
    changelog.end_of_append_batch(0, 0);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    std::promise<nuraft::ptr<nuraft::log_entry>> entry_promise;
    std::thread reader([&]
    {
        entry_promise.set_value(changelog.entry_at_ext(2, /*for_commit=*/true));
    });

    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

    // Truncate above the requested index 2 (never a committed entry) and rewrite 3..10.
    auto rewrite_entry = getLogEntry("epoch_race_commit_rewrite_3", 999);
    changelog.write_at(3, rewrite_entry);
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);
    for (uint64_t i = 4; i <= 10; ++i)
    {
        auto entry = getLogEntry("epoch_race_commit_new_" + std::to_string(i), 1000 + i);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);
    changelog.end_of_append_batch(0, 0);

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);
    reader.join();

    auto entry2 = entry_promise.get_future().get();
    ASSERT_NE(entry2, nullptr);
    EXPECT_EQ(entry2->get_term(), 2u) << "Index below the truncation point is always safe to serve as-is";

    // Later commit reads for the rewritten indices must observe the new terms, never pre-truncation ones.
    auto entry3 = changelog.entry_at_ext(3, /*for_commit=*/true);
    ASSERT_NE(entry3, nullptr);
    EXPECT_EQ(entry3->get_term(), 999u) << "Commit read-ahead must not resurrect the pre-truncation term for index 3";

    auto entry4 = changelog.entry_at_ext(4, /*for_commit=*/true);
    ASSERT_NE(entry4, nullptr);
    EXPECT_EQ(entry4->get_term(), 1004u) << "Commit read-ahead must not resurrect the pre-truncation term for index 4";
}


// Entries evicted from cache must be readable from disk on the direct path, and byte-hint truncation on
// that same path must behave the same way as it does for cached data.
TYPED_TEST(CoordinationChangelogTest, DirectPathEvictedReadsAndByteHints)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::LogFileSettings settings{
        .force_sync = true,
        .compress_logs = this->enable_compression,
        .rotate_interval = 1000,
        .latest_logs_cache_size_threshold = 1,
    };

    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), DB::ReadAheadSettings{.commit_window_bytes = 1}, this->keeper_context);
        writer.init(0, 0);

        for (size_t i = 0; i < 20; ++i)
        {
            auto entry = getLogEntry("data", static_cast<size_t>(i + 1));
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), DB::ReadAheadSettings{.commit_window_bytes = 1}, this->keeper_context);
    changelog.init(0, 0);

    uint64_t counter_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile];

    auto entries = changelog.log_entries_ext(1, 11, 0, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 10u);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ((*entries)[i]->get_term(), static_cast<uint64_t>(i + 1))
            << "Wrong term at index " << (i + 1);
    }

    uint64_t counter_after
        = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile];
    EXPECT_GT(counter_after, counter_before);

    // Byte-hint truncation on the direct path (the read above already covers the hint=0 case).
    auto hint_1b = changelog.log_entries_ext(1, 21, /*batch_size_hint_in_bytes=*/1, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(hint_1b, nullptr);
    ASSERT_GE(hint_1b->size(), 1u); // a 1-byte hint must still return at least one entry

    auto hint_max = changelog.log_entries_ext(1, 21, /*batch_size_hint_in_bytes=*/0x7FFFFFFF, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(hint_max, nullptr);
    ASSERT_EQ(hint_max->size(), 20u); // a huge hint must not truncate
}



// Stress: concurrent appends and disk reads detect races under TSan.
TYPED_TEST(CoordinationChangelogTest, ConcurrentAppendVsActiveFileRead)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = this->enable_compression,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("base", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    std::atomic<bool> stop{false};
    std::atomic<size_t> appended{10};

    std::thread writer([&]
    {
        while (!stop.load(std::memory_order_relaxed))
        {
            size_t idx = appended.fetch_add(1, std::memory_order_relaxed) + 1;
            auto entry = getLogEntry("w", idx);
            changelog.append(entry);
            changelog.end_of_append_batch(0, 0);
        }
    });

    std::thread reader_thread([&]
    {
        for (int iter = 0; iter < 200 && !stop.load(std::memory_order_relaxed); ++iter)
        {
            try
            {
                auto entries = changelog.log_entries_ext(1, 6, 0, DB::KeeperLogStore::NO_PEER_ID);
                // Either nullptr (if compacted) or non-empty.
                if (entries && !entries->empty())
                {
                    EXPECT_LE(entries->size(), 5u);
                }
            }
            catch (const DB::Exception &) // NOLINT(bugprone-empty-catch) - a read racing concurrent appends/rotation may throw; this test only hunts data races
            {
            }
        }
        stop.store(true, std::memory_order_relaxed);
    });

    reader_thread.join();
    writer.join();
}

// Tests: valid-run metadata maintenance, checked via Changelog::checkValidRunsConsistencyForTests.

// Live writeAt-driven rewrites (including a go-to-previous-file rewrite that removes later files)
// must keep valid-run metadata consistent at every step, and a fresh instance's init scan must
// reconstruct equally-consistent metadata from the resulting on-disk layout.
TYPED_TEST(CoordinationChangelogTest, ValidRunsMaintenanceInvariants)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = this->enable_compression,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = 1,
    };

    DB::Changelog changelog(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);

    for (uint64_t i = 1; i <= 35; ++i)
        changelog.appendEntry(i, getLogEntry("valid_runs_" + std::to_string(i), i));

    if (this->enable_compression)
    {
        // Compressed appends record no runtime locations, so live-maintenance checks would be
        // vacuous; only the fresh-init scan reconstruction below applies to compressed logs.
        changelog.flush();
    }
    else
    {
        // flush() twice: the first waits for durability, the second drains locations into
        // logs_location, which checkValidRunsConsistencyForTests reads right after.
        changelog.flush();
        changelog.flush();
        changelog.checkValidRunsConsistencyForTests();

        changelog.writeAt(23, getLogEntry("valid_runs_rewrite_23", 9923));
        changelog.flush();
        for (uint64_t i = 24; i <= 30; ++i)
            changelog.appendEntry(i, getLogEntry("valid_runs_" + std::to_string(i), i));
        changelog.flush();
        changelog.flush();
        changelog.checkValidRunsConsistencyForTests();

        // Go-to-previous-file rewrite: removes changelog_11_20, changelog_21_30, changelog_31_40.
        changelog.writeAt(7, getLogEntry("valid_runs_rewrite_7", 9907));
        changelog.flush();
        assertFileDeleted("./logs/changelog_11_20.bin" + this->extension);
        assertFileDeleted("./logs/changelog_21_30.bin" + this->extension);
        assertFileDeleted("./logs/changelog_31_40.bin" + this->extension);
        for (uint64_t i = 8; i <= 12; ++i)
            changelog.appendEntry(i, getLogEntry("valid_runs_" + std::to_string(i), i));
        changelog.flush();
        changelog.flush();
        changelog.checkValidRunsConsistencyForTests();
    }

    // A fresh instance's init scan must rebuild equally-consistent valid-run metadata.
    DB::Changelog changelog_read(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
    changelog_read.readChangelogAndInitWriter(0, 0);
    changelog_read.checkValidRunsConsistencyForTests();
}

// Tests: per-peer read-ahead

namespace DB
{
namespace FailPoints
{
    extern const char keeper_changelog_readahead_fill_wedge[];
    extern const char keeper_changelog_readahead_serve_wait[];
    extern const char keeper_changelog_readahead_park_armed[];
    extern const char keeper_changelog_readahead_pre_drain[];
    extern const char keeper_changelog_readahead_fill_exception[];
}
}

// Bounded fill cursors, tested directly against DB::Changelog since KeeperLogStore::changelog is
// private. getReadAheadPlan emits a natural cursor over the active file's flushed prefix; this test
// shrinks that cursor's count to exercise the bound explicitly.

// A bounded cursor stops exactly at its count bound, whether the fill runs straight through (served via
// direct-read fallback for the rest) or parks mid-range (byte budget reached before the bound) and must
// resume correctly on refill.
TYPED_TEST(CoordinationChangelogTest, ReadAheadBoundedCursor)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 100,
        .latest_logs_cache_size_threshold = 1,
    };

    // Write+close with a throwaway writer, then re-derive caches from disk via a fresh Changelog's
    // init-read path: flushAsync()'s refreshCache() runs before write locations are registered, so it
    // can't be relied on to evict latest_logs_cache deterministically on this instance.
    {
        DB::Changelog writer(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 40; ++i)
            writer.appendEntry(i, getLogEntry("bounded_cursor_" + std::to_string(i), i));
        writer.flush();
    }

    this->keeper_context->setLastCommitIndex(40);

    // Each phase below is scoped so two Changelog instances are never simultaneously live on the same
    // directory.

    // Phase A: the fill runs straight through with no park and must still stop exactly at the bound.
    {
        DB::Changelog changelog(
            this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
        changelog.readChangelogAndInitWriter(0, 0);

        auto plan = changelog.getReadAheadPlan(1, 41, 0);
        // The active file's flushed prefix is a valid run, so the planner emits one natural cursor
        // covering it (previously excluded by the seal). Truncate it to exercise the bound.
        ASSERT_TRUE(plan.read_ahead_window.has_value());
        ASSERT_EQ(plan.read_ahead_window->size(), 1u);
        EXPECT_EQ(plan.read_ahead_window->front().first_index, 1u);
        EXPECT_EQ(plan.read_ahead_window->front().count, 40u);
        ASSERT_FALSE(plan.items.empty());

        plan.read_ahead_window->front().count = 25;

        const uint64_t decoded_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];

        auto entries = changelog.serveReadAhead(/*reader_id=*/7, plan);
        ASSERT_NE(entries, nullptr);
        ASSERT_EQ(entries->size(), 40u);
        for (uint64_t i = 0; i < 40; ++i)
            EXPECT_EQ((*entries)[i]->get_term(), i + 1) << "Wrong term at index " << (i + 1);

        const uint64_t decoded_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];
        EXPECT_EQ(decoded_after - decoded_before, 25u) << "Fill must stop exactly at the bounded cursor's count";
    }

    // Phase B: a small window_bytes budget forces the fill to park mid-range before reaching the bound;
    // it must resume correctly on refill and still stop exactly at the bound.
    {
        DB::Changelog changelog(
            this->log,
            settings,
            DB::FlushSettings(),
            // window_bytes sized to roughly 3 "bounded_cursor_N" entries so the fill parks well before
            // reaching the count=25 bound below.
            DB::ReadAheadSettings{.enabled = true, .window_bytes = 54, .serve_wait_timeout_ms = 5000},
            this->keeper_context);
        changelog.readChangelogAndInitWriter(0, 0);

        auto plan = changelog.getReadAheadPlan(1, 41, 0);
        ASSERT_TRUE(plan.read_ahead_window.has_value());
        ASSERT_EQ(plan.read_ahead_window->size(), 1u);
        EXPECT_EQ(plan.read_ahead_window->front().first_index, 1u);
        EXPECT_EQ(plan.read_ahead_window->front().count, 40u);
        ASSERT_FALSE(plan.items.empty());

        plan.read_ahead_window->front().count = 25;

        const uint64_t decoded_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];

        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_park_armed);

        std::promise<DB::LogEntriesPtr> result_promise;
        std::thread reader([&]
        {
            auto served = changelog.serveReadAhead(/*reader_id=*/9, plan);
            result_promise.set_value(served);
        });

        // Wait for one park, then let the fill and the concurrent drain finish via notify-on-pop -- no sleeps.
        DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_readahead_park_armed);
        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_park_armed);

        auto entries = result_promise.get_future().get();
        reader.join();

        ASSERT_NE(entries, nullptr);
        ASSERT_EQ(entries->size(), 40u);
        for (uint64_t i = 0; i < 40; ++i)
            EXPECT_EQ((*entries)[i]->get_term(), i + 1) << "Wrong term at index " << (i + 1);

        const uint64_t decoded_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];
        EXPECT_EQ(decoded_after - decoded_before, 25u) << "Fill must stop exactly at the bounded cursor's count after park/refill";
    }
}

// Regression test for the peer stale-serve hole: writeAt never truncates the on-disk file, so a live
// rewrite whose tail lines up with the (renamed) file end leaves a stale interior run behind. The old
// unbounded fill cursor decoded straight through it and Eofed silently, no failpoint needed. A restart
// (fresh instance opening the same on-disk layout) must rebuild metadata that keeps the peer planner off
// the stale run just as reliably as the live writeAt path did.
TYPED_TEST(CoordinationChangelogTest, ReadAheadWriteAtInvalidation)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = 1,
    };

    // Phase 1 (live): the live instance must be fully destroyed (scope exit) before phase 2 opens the
    // same directory below -- two live instances on one directory are forbidden.
    {
        DB::KeeperLogStore changelog(
            settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
        changelog.init(0, 0);

        for (uint64_t i = 1; i <= 50; ++i)
        {
            auto entry = getLogEntry("write_at_invalidation_" + std::to_string(i), i);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);
        // KeeperLogStore settle sequence: refreshCache's synchronous drain (inside flushAsync) races the
        // write thread's addLogLocations, so a second end_of_append_batch after waiting for durability is
        // needed to actually apply the locations registered during the wait.
        waitDurableLogs(changelog);
        changelog.end_of_append_batch(0, 0);

        auto rewrite_entry = getLogEntry("write_at_invalidation_rewrite", 999);
        changelog.write_at(45, rewrite_entry);
        changelog.end_of_append_batch(0, 0);
        waitDurableLogs(changelog);
        for (uint64_t i = 46; i <= 50; ++i)
        {
            auto entry = getLogEntry("write_at_invalidation_new_" + std::to_string(i), 900 + i);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);
        waitDurableLogs(changelog);
        changelog.end_of_append_batch(0, 0);

        // Rotate changelog_41_50 out from under the active writer, finalizing it with the rewritten tail
        // still lined up exactly at the renamed to_log_index -- the deterministic silent sub-case.
        for (uint64_t i = 51; i <= 55; ++i)
        {
            auto entry = getLogEntry("write_at_invalidation_tail_" + std::to_string(i), i);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);
        waitDurableLogs(changelog);
        changelog.end_of_append_batch(0, 0);

        auto b1 = changelog.log_entries_ext(41, 45, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/7);
        ASSERT_NE(b1, nullptr);
        ASSERT_EQ(b1->size(), 4u);
        for (uint64_t i = 41; i < 45; ++i)
            EXPECT_EQ((*b1)[i - 41]->get_term(), i) << "Wrong term at index " << i;

        auto b2 = changelog.log_entries_ext(45, 51, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/7);
        ASSERT_NE(b2, nullptr);
        ASSERT_EQ(b2->size(), 6u);
        EXPECT_EQ((*b2)[0]->get_term(), 999u) << "Peer must see the rewritten entry at index 45, not the stale one";
        for (uint64_t i = 45; i < 51; ++i)
            EXPECT_EQ((*b2)[i - 45]->get_term(), changelog.entry_at(i)->get_term()) << "Wrong term at index " << i;
    }

    // Phase 2 (restart): same on-disk layout, but a fresh instance's init scan (not live writeAt) must
    // rebuild equally-consistent metadata. No consistency-checker call here (it's DB::Changelog-only;
    // KeeperLogStore::changelog is private) -- re-running the same served-terms assertions below is this
    // phase's end-to-end validation.
    DB::KeeperLogStore changelog(
        settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
    changelog.init(0, 0);

    auto b1 = changelog.log_entries_ext(41, 45, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/7);
    ASSERT_NE(b1, nullptr);
    ASSERT_EQ(b1->size(), 4u);
    for (uint64_t i = 41; i < 45; ++i)
        EXPECT_EQ((*b1)[i - 41]->get_term(), i) << "Wrong term at index " << i;

    auto b2 = changelog.log_entries_ext(45, 51, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/7);
    ASSERT_NE(b2, nullptr);
    ASSERT_EQ(b2->size(), 6u);
    EXPECT_EQ((*b2)[0]->get_term(), 999u) << "Peer must see the rewritten entry at index 45, not the stale one";
    for (uint64_t i = 45; i < 51; ++i)
        EXPECT_EQ((*b2)[i - 45]->get_term(), changelog.entry_at(i)->get_term()) << "Wrong term at index " << i;
}

// New capability: the active (unrotated) file's flushed prefix is now covered by a natural read-ahead
// cursor, and further appends to the SAME active file are picked up by the next plan.
TYPED_TEST(CoordinationChangelogTest, ReadAheadActiveFileFlushedPrefix)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 1000, // single active (unrotated) file for the whole test
        .latest_logs_cache_size_threshold = 1,
    };

    {
        DB::Changelog writer(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 50; ++i)
            writer.appendEntry(i, getLogEntry("active_prefix_" + std::to_string(i), i));
        writer.flush();
    }

    this->keeper_context->setLastCommitIndex(50);

    DB::Changelog changelog(
        this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);

    auto plan = changelog.getReadAheadPlan(1, 11, 0);
    ASSERT_TRUE(plan.read_ahead_window.has_value());
    ASSERT_EQ(plan.read_ahead_window->size(), 1u);
    EXPECT_EQ(plan.read_ahead_window->front().first_index, 1u);
    EXPECT_EQ(plan.read_ahead_window->front().count, 50u);

    auto entries = changelog.serveReadAhead(/*reader_id=*/13, plan);
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 10u);
    for (uint64_t i = 0; i < 10; ++i)
        EXPECT_EQ((*entries)[i]->get_term(), i + 1) << "Wrong term at index " << (i + 1);

    // Extend the SAME active file: further appends must be picked up by the next plan.
    for (uint64_t i = 51; i <= 80; ++i)
        changelog.appendEntry(i, getLogEntry("active_prefix_" + std::to_string(i), i));
    changelog.flush();

    auto plan2 = changelog.getReadAheadPlan(51, 61, 0);
    auto entries2 = changelog.serveReadAhead(/*reader_id=*/14, plan2);
    ASSERT_NE(entries2, nullptr);
    ASSERT_EQ(entries2->size(), 10u);
    for (uint64_t i = 0; i < 10; ++i)
        EXPECT_EQ((*entries2)[i]->get_term(), 51 + i) << "Wrong term at index " << (51 + i);
}

// A broken_at_end file (readable prefix + corrupt physical tail) must be included in read-ahead
// with an exact bound at the last valid index. The decoded-counter delta is deterministic here
// since the request spans the full bounded window and each cursor decodes in a single chunk.
TYPED_TEST(CoordinationChangelogTest, ReadAheadBrokenFileBounded)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = 1,
    };

    {
        DB::Changelog writer(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 15; ++i)
            writer.appendEntry(i, getLogEntry("broken_bounded_" + std::to_string(i + 10), i));
        writer.flush();
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_20.bin"));

    // The 5 records (11..15) have equal-length content, so truncating to 3.5 records (same technique
    // as ChangelogTestReadAfterBrokenTruncate) leaves 11..13 valid and 14 partially corrupted.
    const size_t file_size = fs::file_size("./logs/changelog_11_20.bin");
    const size_t record_size = file_size / 5;
    ASSERT_EQ(record_size * 5, file_size) << "Records must be equal-sized for a deterministic truncation point";
    {
        DB::WriteBufferFromFile plain_buf(
            "./logs/changelog_11_20.bin", DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
        plain_buf.truncate(3 * record_size + record_size / 2);
        plain_buf.finalize();
    }

    this->keeper_context->setLastCommitIndex(13);
    DB::Changelog changelog(
        this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);
    ASSERT_EQ(changelog.size(), 13u);

    auto plan = changelog.getReadAheadPlan(1, 14, 0);
    ASSERT_TRUE(plan.read_ahead_window.has_value());
    size_t total_count = 0;
    for (const auto & cursor : *plan.read_ahead_window)
        total_count += cursor.count;
    EXPECT_EQ(total_count, 13u) << "No cursor may cover the corrupted tail past the last valid index";

    const uint64_t decoded_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];
    auto entries = changelog.serveReadAhead(/*reader_id=*/11, plan);
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 13u);
    for (uint64_t i = 0; i < 13; ++i)
        EXPECT_EQ((*entries)[i]->get_term(), i + 1) << "Wrong term at index " << (i + 1);

    const uint64_t decoded_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];
    EXPECT_EQ(decoded_after - decoded_before, 13u) << "The bounded window must decode exactly through the last valid index";
}

// Strictly sequential entry_at_ext(i, true) calls across a changelog spanning many files must all return
// the correct entry, with the commit reader engaging (not falling back to a pure direct-read path), and
// once the reader's window covers the requested range, subsequent calls must be served via the fast path
// without rebuilding the plan / installing more cursors than one benign duplicate per file boundary.
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadSequentialCatchup)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = 1,
    };
    const DB::ReadAheadSettings readahead_settings{.enabled = false, .serve_wait_timeout_ms = 100, .commit_window_bytes = 64 * 1024 * 1024};

    constexpr uint64_t total = 200;

    // Write+close, then re-derive from disk via a fresh instance's init-read path (see
    // ReadAheadBoundedCursor) so the ≥150 counter assertion below isn't flaky.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= total; ++i)
        {
            auto entry = getLogEntry("commit_ra_seq", i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    const uint64_t cra_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromCommitReadAhead];
    const uint64_t cursors_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadCursorsInstalled];

    // First fetch triggers the initial miss: plan build + cursor installation.
    auto first = changelog.entry_at_ext(1, /*for_commit=*/true);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->get_term(), 1u);

    const uint64_t cursors_after_first = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadCursorsInstalled];
    EXPECT_GT(cursors_after_first, cursors_before) << "The initial miss must build a plan and install at least one read-ahead cursor";

    // Subsequent indices are covered by the already-installed window: no further plan/cursor rebuilds.
    for (uint64_t i = 2; i <= total; ++i)
    {
        auto entry = changelog.entry_at_ext(i, /*for_commit=*/true);
        ASSERT_NE(entry, nullptr) << "Missing entry at index " << i;
        EXPECT_EQ(entry->get_term(), i) << "Wrong term at index " << i;
    }

    const uint64_t cra_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromCommitReadAhead];
    const uint64_t cursors_after_all = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadCursorsInstalled];
    EXPECT_GE(cra_after - cra_before, 150u) << "Most entries must be served through the commit read-ahead reader";

    const uint64_t num_files = total / settings.rotate_interval;
    // With N+1 lookahead, a genuine miss's rebuilt plan can install up to 2 cursors instead of 1, but
    // misses then happen at roughly every OTHER boundary, so the two effects largely offset. Bound at
    // the worst case of 2 per boundary so this still catches an O(total) per-entry rebuild regression.
    EXPECT_LE(cursors_after_all - cursors_after_first, 2 * num_files)
        << "Cursor count must not grow by more than N+1's worst case of 2 cursors per file boundary once "
           "the commit reader's window already covers subsequent indices";
}


// A single, unrotated (unsealed) file must still support bounded commit cursors over its flushed prefix,
// and further appends to the SAME active file must be picked up by the next miss's plan rebuild.
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadActiveFileExtension)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 1000, // single unsealed file for the whole test
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = false, .serve_wait_timeout_ms = 100, .commit_window_bytes = 64 * 1024 * 1024},
        this->keeper_context);
    changelog.init(0, 0);

    for (uint64_t i = 1; i <= 50; ++i)
    {
        auto entry = getLogEntry("commit_ra_active_ext", i);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    for (uint64_t i = 1; i <= 50; ++i)
    {
        auto entry = changelog.entry_at_ext(i, /*for_commit=*/true);
        ASSERT_NE(entry, nullptr) << "Missing entry at index " << i;
        EXPECT_EQ(entry->get_term(), i) << "Wrong term at index " << i;
    }

    for (uint64_t i = 51; i <= 100; ++i)
    {
        auto entry = getLogEntry("commit_ra_active_ext", i);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    for (uint64_t i = 51; i <= 100; ++i)
    {
        auto entry = changelog.entry_at_ext(i, /*for_commit=*/true);
        ASSERT_NE(entry, nullptr) << "Missing entry at index " << i;
        EXPECT_EQ(entry->get_term(), i) << "Wrong term at index " << i;
    }
}


// Compaction racing with a paused commit-plan resolution must yield nullptr only when the requested
// index is genuinely gone, and must still serve entries above the compaction boundary correctly.
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadCompactionFallback)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = 1,
    };
    const DB::ReadAheadSettings readahead_settings{.enabled = false, .serve_wait_timeout_ms = 5000, .commit_window_bytes = 64 * 1024 * 1024};

    // Write+close, then re-derive from disk via a fresh instance's init-read path so indices 15/25 are
    // deterministically NOT in latest_logs_cache; otherwise entry_at_ext could skip the
    // keeper_changelog_read_plan_resolved pause and waitForPause below would hang.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= 30; ++i)
        {
            auto entry = getLogEntry("commit_ra_compaction", i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    {
        SCOPED_TRACE("Sub-case A: compaction removes the requested index's file after PLAN, before EXECUTE -> nullptr, not a throw");
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);
        // compact() marks files for removal and defers the disk unlink to the background
        // changelog-operations thread; pause at removed_from_disk_set to wait for it deterministically
        // instead of racing the fallback direct-read against an in-flight removal.
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);

        std::promise<nuraft::ptr<nuraft::log_entry>> entry_promise;
        std::thread reader([&]
        {
            entry_promise.set_value(changelog.entry_at_ext(15, /*for_commit=*/true));
        });

        DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

        this->keeper_context->setLastCommitIndex(20);
        changelog.compact(20); // schedules async removal of changelog_1_10.bin and changelog_11_20.bin

        // Both removals run sequentially on the background changelog-operations thread and each pauses
        // at keeper_changelog_removed_from_disk_set; drain both before the fallback direct-read proceeds.
        for (int removed = 0; removed < 2; ++removed)
        {
            DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_removed_from_disk_set);
            DB::FailPointInjection::notifyFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);
        }
        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_removed_from_disk_set);

        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

        reader.join();
        EXPECT_EQ(entry_promise.get_future().get(), nullptr) << "Compacted entry must return nullptr, not throw";
    }

    {
        SCOPED_TRACE("Sub-case B: compaction only removes files strictly below the requested index -> entry still served");
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

        std::promise<nuraft::ptr<nuraft::log_entry>> entry_promise;
        std::thread reader([&]
        {
            entry_promise.set_value(changelog.entry_at_ext(25, /*for_commit=*/true));
        });

        DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

        this->keeper_context->setLastCommitIndex(20);
        changelog.compact(20); // well below the requested index 25; no-op the second time around

        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

        reader.join();
        auto entry = entry_promise.get_future().get();
        ASSERT_NE(entry, nullptr) << "Entry above the compaction boundary must still be served";
        EXPECT_EQ(entry->get_term(), 25u);
    }
}

// A wedged fill task must not stall the commit thread beyond serve_wait_timeout_ms: the serve must fall
// back to a blocking direct read, and shutdown must still join the wedged fill cleanly afterwards.
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadServeTimeoutFallback)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = 1,
    };
    const DB::ReadAheadSettings readahead_settings{.enabled = false, .serve_wait_timeout_ms = 100, .commit_window_bytes = 64 * 1024 * 1024};

    // Write+close, then re-derive from disk via a fresh instance's init-read path so index 1 is
    // deterministically NOT in latest_logs_cache; otherwise entry_at_ext could skip the wedged fill,
    // making the KeeperLogsEntryReadFromFile assertion below flaky.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= 20; ++i)
        {
            auto entry = getLogEntry("commit_ra_timeout", i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    const uint64_t file_reads_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile];

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    const auto start = std::chrono::steady_clock::now();
    auto entry = changelog.entry_at_ext(1, /*for_commit=*/true);
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    EXPECT_LE(elapsed_ms, 5000);

    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->get_term(), 1u);

    const uint64_t file_reads_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile];
    EXPECT_GT(file_reads_after, file_reads_before) << "Serve timeout must fall back to a blocking direct read";

    // KeeperLogStore's destructor (via ~Changelog -> shutdown) must join the still-wedged fill task
    // cleanly once the failpoint is disabled -- no deadlock on test teardown.
}

// Once the latest logs cache holds the tail of the changelog, entries at/above that boundary must be
// served as cheap in-memory hits and must never engage the commit read-ahead reader.
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadExhaustedLatestCacheHandoff)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    constexpr uint64_t total = 100;
    constexpr uint64_t cached_tail = 20;
    const std::string payload = "commit_ra_cache_pad_entry"; // fixed-size payload, so the threshold below
                                                              // deterministically admits `cached_tail` entries
    const uint64_t threshold = cached_tail * payload.size();

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = threshold,
    };
    const DB::ReadAheadSettings readahead_settings{.enabled = false, .serve_wait_timeout_ms = 100, .commit_window_bytes = 64 * 1024 * 1024};

    // Write+close, then re-derive latest_logs_cache deterministically via a fresh instance's init-read
    // path (see ReadAheadBoundedCursor) for exact-boundary assertions.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= total; ++i)
        {
            auto entry = getLogEntry(payload, i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    const uint64_t latest_cache_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromLatestCache];

    for (uint64_t i = 1; i <= total; ++i)
    {
        auto entry = changelog.entry_at_ext(i, /*for_commit=*/true);
        ASSERT_NE(entry, nullptr) << "Missing entry at index " << i;
        EXPECT_EQ(entry->get_term(), i) << "Wrong term at index " << i;
    }

    const uint64_t latest_cache_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromLatestCache];
    EXPECT_GE(latest_cache_after - latest_cache_before, cached_tail)
        << "Entries at/above the latest-cache boundary must be served as cheap hits, never via the commit reader";
}


// Peer read-ahead and commit read-ahead must coexist safely: concurrent streaming reads through both
// paths must complete correctly with no starvation or deadlock (pool sized peers+1).
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadWithPeerReadAheadConcurrent)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{
            .enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 2, .serve_wait_timeout_ms = 100, .commit_window_bytes = 64 * 1024 * 1024},
        this->keeper_context);
    changelog.init(0, 0);

    constexpr uint64_t total = 100;
    for (uint64_t i = 1; i <= total; ++i)
    {
        auto entry = getLogEntry("commit_ra_concurrent_stress", i);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    std::atomic<bool> peer_ok{true};
    std::atomic<bool> commit_ok{true};

    std::thread peer_reader([&]
    {
        uint64_t start = 1;
        while (start < total)
        {
            uint64_t end = std::min(start + 5, total + 1);
            auto result = changelog.log_entries_ext(start, end, 0, /*peer_id=*/42);
            if (result == nullptr || result->empty())
            {
                peer_ok.store(false, std::memory_order_relaxed);
                break;
            }
            for (size_t i = 0; i < result->size(); ++i)
            {
                if ((*result)[i]->get_term() != start + i)
                {
                    peer_ok.store(false, std::memory_order_relaxed);
                    break;
                }
            }
            start += result->size();
        }
    });

    std::thread commit_reader([&]
    {
        for (uint64_t i = 1; i <= total; ++i)
        {
            auto entry = changelog.entry_at_ext(i, /*for_commit=*/true);
            if (entry == nullptr || entry->get_term() != i)
            {
                commit_ok.store(false, std::memory_order_relaxed);
                break;
            }
        }
    });

    peer_reader.join();
    commit_reader.join();

    EXPECT_TRUE(peer_ok.load());
    EXPECT_TRUE(commit_ok.load());
}

// NO_PEER_ID or disabled read-ahead must return identical results to the direct path.
// Verify read-ahead returns the same entries as the direct path.
TYPED_TEST(CoordinationChangelogTest, ReadAheadMatchesDirectPath)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        // commit_window_bytes = 1 (not 0) keeps the commit reader out of the way without disabling it.
        DB::ReadAheadSettings{.enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 4, .serve_wait_timeout_ms = 100, .commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test1", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // Direct path (NO_PEER_ID skips read-ahead).
    auto direct_result = changelog.log_entries_ext(1, 11, /*batch_size_hint_in_bytes=*/0, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(direct_result, nullptr);
    ASSERT_EQ(direct_result->size(), 10u);

    // Read-ahead path (peer_id != NO_PEER_ID, enabled = true).
    auto ra_result = changelog.log_entries_ext(1, 11, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/42);
    ASSERT_NE(ra_result, nullptr);
    ASSERT_EQ(ra_result->size(), 10u);

    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ((*direct_result)[i]->get_term(), (*ra_result)[i]->get_term());

    // Disabled read-ahead (enabled = false) must also match the direct path.
    DB::KeeperLogStore changelog_disabled(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = false, .commit_window_bytes = 1},
        this->keeper_context);
    changelog_disabled.init(0, 0);
    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test1", static_cast<size_t>(i + 1));
        changelog_disabled.append(entry);
    }
    changelog_disabled.end_of_append_batch(0, 0);
    waitDurableLogs(changelog_disabled);

    auto disabled_result = changelog_disabled.log_entries_ext(1, 11, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/42);
    ASSERT_NE(disabled_result, nullptr);
    ASSERT_EQ(disabled_result->size(), 10u);
    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ((*direct_result)[i]->get_term(), (*disabled_result)[i]->get_term());
}

// serveReadAhead runs without changelog_lock, so a concurrent append must not deadlock while a fill is
// wedged; and the wedged fill's serve must escape via the serve_wait timeout fallback (blocking direct
// read), still returning the correct entries, all while the failpoint remains enabled.
TYPED_TEST(CoordinationChangelogTest, ReadAheadWedgedFill)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 4, .serve_wait_timeout_ms = 100, .commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test5", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    std::atomic<int64_t> elapsed_ms{0};
    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> result_promise;
    std::thread reader([&]
    {
        const auto start = std::chrono::steady_clock::now();
        auto result = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, /*peer_id=*/1);
        elapsed_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count());
        result_promise.set_value(std::move(result));
    });

    std::promise<void> append_done;
    std::thread appender([&]
    {
        auto entry = getLogEntry("new_entry", 11);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        append_done.set_value();
    });

    auto append_fut = append_done.get_future();
    ASSERT_EQ(append_fut.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "append deadlocked while the read-ahead fill is wedged";

    // The serve must escape the still-wedged fill via the serve_wait timeout fallback and deliver the
    // correct entries -- checked here, before the failpoint is disabled, to prove the escape is
    // deterministic rather than an artifact of releasing the fill early.
    auto result_fut = result_promise.get_future();
    ASSERT_EQ(result_fut.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "serve did not escape the wedged fill via the serve_wait timeout fallback";

    auto result = result_fut.get();
    EXPECT_LE(elapsed_ms.load(), 5000);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*result)[i]->get_term(), static_cast<uint64_t>(i + 1));

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    reader.join();
    appender.join();
}

// When a serve-wait timeout falls back to a direct read, the reader must be fast-forwarded past the
// served range instead of leaving the fill task to re-decode it entry by entry.
TYPED_TEST(CoordinationChangelogTest, ReadAheadTimeoutFallbackFastForwardsFill)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    constexpr uint64_t total = 10;

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 1000, // keep every entry in a single file: no boundary confounds the fill accounting
        .latest_logs_cache_size_threshold = 1,
    };
    const DB::ReadAheadSettings readahead_settings{
        .enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 4, .serve_wait_timeout_ms = 2000, .chunk_size = 1, .commit_window_bytes = 1};

    // Write+close, then re-derive from disk so every entry is deterministically NOT in latest_logs_cache
    // (a live instance's refreshCache() races the write thread and could serve from memory instead).
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= total; ++i)
        {
            auto entry = getLogEntry("readahead_timeout_fast_forward", i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    const uint64_t decoded_baseline = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];
    const uint64_t fallbacks_baseline = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadTimeoutFallbacks];

    constexpr int32_t peer_id = 7;

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    // Requests [1, 6): the wedged fill produces nothing within serve_wait_timeout_ms, so this must
    // escape via the timeout fallback and serve entries 1-5 directly.
    auto first_batch = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, peer_id);

    const uint64_t fallbacks_after_first = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadTimeoutFallbacks];
    EXPECT_EQ(fallbacks_after_first - fallbacks_baseline, 1u) << "Exactly one timeout fallback must be recorded for the wedged batch";

    ASSERT_NE(first_batch, nullptr);
    ASSERT_EQ(first_batch->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*first_batch)[i]->get_term(), static_cast<uint64_t>(i + 1));

    // Still wedged: the stale cursor (captured before the fast-forward reset) has not yet been given
    // a chance to run at all -- no decode should have happened yet.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries], decoded_baseline)
        << "The wedged fill must not have decoded anything before being unwedged";

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    // The reader must have been fast-forwarded to index 6, so this batch is served from the deque with
    // no further timeout fallback.
    auto second_batch = changelog.log_entries_ext(6, 11, /*batch_size_hint_in_bytes=*/0, peer_id);

    const uint64_t fallbacks_after_second = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadTimeoutFallbacks];
    EXPECT_EQ(fallbacks_after_second, fallbacks_after_first)
        << "The second batch must be served from the read-ahead deque, not another timeout fallback";

    ASSERT_NE(second_batch, nullptr);
    ASSERT_EQ(second_batch->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*second_batch)[i]->get_term(), static_cast<uint64_t>(i + 6));

    // The 5 legitimate entries for [6, 11) plus at most one wasted entry from the stale cursor's
    // already-started chunk (chunk_size == 1 bounds the waste); without fast-forward this would be >=10.
    const uint64_t decoded_total = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries] - decoded_baseline;
    EXPECT_GE(decoded_total, 5u) << "The fill must have decoded the legitimate entries for [6, 11)";
    EXPECT_LE(decoded_total, 6u) << "The fill must not have re-decoded the already-served range [1, 5]";
}

// A fill-task exception must not poison the shared read-ahead pool: the reader transitions to Error,
// wakes the blocked serve immediately, and a later serve recreates a fresh reader.
TYPED_TEST(CoordinationChangelogTest, ReadAheadFillExceptionRecovers)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    constexpr uint64_t total = 10;

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 1000,
        .latest_logs_cache_size_threshold = 1,
    };
    const DB::ReadAheadSettings readahead_settings{
        .enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 4, .serve_wait_timeout_ms = 5000, .chunk_size = 1, .commit_window_bytes = 1};

    // Write+close, then re-derive from disk so entries aren't in latest_logs_cache.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= total; ++i)
        {
            auto entry = getLogEntry("readahead_fill_exception", i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    constexpr int32_t peer_id = 3;

    const uint64_t fallbacks_baseline = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadTimeoutFallbacks];
    const uint64_t created_baseline = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_exception);

    auto first_batch = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, peer_id);

    ASSERT_NE(first_batch, nullptr);
    ASSERT_EQ(first_batch->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*first_batch)[i]->get_term(), static_cast<uint64_t>(i + 1));

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadTimeoutFallbacks], fallbacks_baseline)
        << "Must wake via the reader's Error state, not the serve_wait timeout";

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_exception);

    auto second_batch = changelog.log_entries_ext(6, 11, /*batch_size_hint_in_bytes=*/0, peer_id);
    ASSERT_NE(second_batch, nullptr);
    ASSERT_EQ(second_batch->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*second_batch)[i]->get_term(), static_cast<uint64_t>(i + 6));

    EXPECT_GE(ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated] - created_baseline, 2u)
        << "The Error reader must have been retired and a fresh one created";
}

// A wedged fill task keeps occupying its pool thread even after its reader is logically retired, since
// the OS thread hasn't returned to the pool yet. makeReaderLocked must trySchedule (fail fast) rather
// than block under readers_mutex, and the caller must degrade to a direct read instead of hanging.
TYPED_TEST(CoordinationChangelogTest, ReadAheadPoolSaturationFallsBackToDirectRead)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 100,
        .latest_logs_cache_size_threshold = 1,
    };
    const DB::ReadAheadSettings readahead_settings{
        .enabled = true,
        .window_bytes = 64 * 1024 * 1024,
        .max_peer_readers = 2,
        .eviction_timeout_ms = 1,
        .pool_threads = 0,
        .serve_wait_timeout_ms = 100,
        .commit_window_bytes = 1,
    };

    // Write+close, then re-derive from disk so logs_location/valid_runs are ready for read-ahead
    // (see ReadAheadBoundedCursor for the same pitfall with a live instance).
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= 20; ++i)
        {
            auto entry = getLogEntry("readahead_pool_saturation", i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    const uint64_t rejected_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadScheduleRejected];

    // The pool holds (max_peer_readers + 1) threads and 2x that as total queue capacity (running +
    // queued). eviction_timeout_ms=1 lets evictIdleReadersLocked reap each reader's map entry while
    // its fill task is still wedged occupying a pool slot, so repeated distinct requests each consume
    // a fresh slot without ever freeing an old one.
    const size_t pool_capacity = 2 * (readahead_settings.max_peer_readers + 1);
    int32_t next_peer_id = 1;
    for (size_t i = 0; i < pool_capacity; ++i)
    {
        if (i == pool_capacity / 2)
        {
            auto commit_entry = changelog.entry_at_ext(1, /*for_commit=*/true);
            ASSERT_NE(commit_entry, nullptr);
            EXPECT_EQ(commit_entry->get_term(), 1u);
            continue;
        }
        auto peer_batch = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, next_peer_id++);
        ASSERT_NE(peer_batch, nullptr);
        EXPECT_EQ(peer_batch->size(), 5u);
    }

    // The pool is now fully occupied; a new reader's fill scheduling must trySchedule-fail and fall
    // back to a direct read instead of wedging on the saturated pool.
    auto peer_final = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, next_peer_id);
    ASSERT_NE(peer_final, nullptr);
    ASSERT_EQ(peer_final->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*peer_final)[i]->get_term(), static_cast<uint64_t>(i + 1));

    const uint64_t rejected_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadScheduleRejected];
    EXPECT_GT(rejected_after, rejected_before)
        << "A saturated read-ahead pool must reject the new reader's fill scheduling instead of blocking, "
           "and the caller must fall back to a direct read";

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);
}

// Rewind must clear the deque and return contiguous entries from the new start.
TYPED_TEST(CoordinationChangelogTest, ReadAheadNonSequentialRewind)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 4, .serve_wait_timeout_ms = 100, .commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test9", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    auto r1 = changelog.log_entries_ext(1, 6, 0, /*peer_id=*/2);
    ASSERT_NE(r1, nullptr);
    ASSERT_EQ(r1->size(), 5u);

    auto r2 = changelog.log_entries_ext(1, 4, 0, /*peer_id=*/2);
    ASSERT_NE(r2, nullptr);
    ASSERT_EQ(r2->size(), 3u);
    for (size_t i = 0; i < 3; ++i)
        EXPECT_EQ((*r2)[i]->get_term(), static_cast<uint64_t>(i + 1));
}

// Compaction must retire the peer's terminal reader (the next request must create a fresh one), and
// reads of fully-compacted ranges must not crash or deadlock.
TYPED_TEST(CoordinationChangelogTest, ReadAheadCompactionReaderLifecycle)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 4, .serve_wait_timeout_ms = 100, .commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("compaction_lifecycle", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    auto r1 = changelog.log_entries_ext(1, 6, 0, /*peer_id=*/6);
    ASSERT_NE(r1, nullptr);
    ASSERT_EQ(r1->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*r1)[i]->get_term(), static_cast<uint64_t>(i + 1));

    changelog.compact(15);

    // The next serve call reaps the terminal reader (Compacted due to compaction) and creates a new one
    // for entries 16-20. The fill may still be in-flight when compact returns; serve_wait_timeout_ms
    // (100ms) bounds the wait, and the direct-read fallback still returns correct data.
    auto r2 = changelog.log_entries_ext(16, 21, 0, /*peer_id=*/6);
    ASSERT_NE(r2, nullptr);
    ASSERT_EQ(r2->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*r2)[i]->get_term(), static_cast<uint64_t>(16 + i));

    changelog.compact(20);

    // Fully compacted range: nullptr is the expected steady-state result, but this must not crash or
    // deadlock even if a reader still in flight hands back a short batch instead.
    auto r3 = changelog.log_entries_ext(1, 6, 0, /*peer_id=*/6);
    if (r3 != nullptr)
        EXPECT_LE(r3->size(), 5u);
}


// TSan stress: interleave appends, read-ahead serves, and compaction.
TYPED_TEST(CoordinationChangelogTest, ReadAheadTSanStress)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        DB::ReadAheadSettings{.enabled = true, .window_bytes = 64 * 1024 * 1024, .max_peer_readers = 4, .serve_wait_timeout_ms = 100, .commit_window_bytes = 1},
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 50; ++i)
    {
        auto entry = getLogEntry("l2_stress", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    std::atomic<bool> stop{false};
    std::atomic<int> appended{50};

    std::thread appender([&]
    {
        while (!stop.load(std::memory_order_relaxed))
        {
            int idx = appended.fetch_add(1, std::memory_order_relaxed);
            auto entry = getLogEntry("l2_stress", static_cast<size_t>(idx) + 1);
            changelog.append(entry);
            changelog.end_of_append_batch(0, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    });

    constexpr int NUM_READERS = 3;
    std::vector<std::thread> readers;
    readers.reserve(NUM_READERS);
    for (int peer = 0; peer < NUM_READERS; ++peer)
    {
        readers.emplace_back([&, peer]
        {
            size_t start = 1;
            while (!stop.load(std::memory_order_relaxed))
            {
                size_t end = std::min(start + 5, static_cast<size_t>(appended.load(std::memory_order_relaxed)));
                if (end <= start)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                auto result = changelog.log_entries_ext(
                    start, end, /*batch_size_hint_in_bytes=*/0, static_cast<int32_t>(peer + 1));
                if (result != nullptr)
                    start += result->size();
                else
                    start = 1;
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    stop.store(true, std::memory_order_relaxed);

    appender.join();
    for (auto & t : readers)
        t.join();
}

// A peer catching up across several sealed-file rotations must reuse the same reader instead of being
// torn down and recreated at every boundary: fillTask now parks on a cursor-less EOF instead of exiting.
TYPED_TEST(CoordinationChangelogTest, ReadAheadReaderSurvivesFileBoundaries)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 5,
        .latest_logs_cache_size_threshold = 1,
    };

    // Write+close, then re-derive from disk via a fresh instance's init-read path (see
    // ReadAheadBoundedCursor) so latest_logs_cache eviction is deterministic.
    {
        DB::Changelog writer(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 40; ++i)
            writer.appendEntry(i, getLogEntry("boundary_survive_" + std::to_string(i), i));
        writer.flush();
    }

    DB::Changelog changelog(
        this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);

    const int32_t peer_id = 31;
    const uint64_t created_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];
    const uint64_t fill_decoded_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];
    const uint64_t from_file_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile];

    // 8 files of 5 entries each: 7 file-boundary crossings, well over the required 3.
    for (uint64_t start = 1; start <= 40; start += 5)
    {
        auto plan = changelog.getReadAheadPlan(start, start + 5, 0);
        ASSERT_TRUE(plan.read_ahead_window.has_value());
        auto entries = changelog.serveReadAhead(peer_id, plan);
        ASSERT_NE(entries, nullptr);
        ASSERT_EQ(entries->size(), 5u);
        for (uint64_t i = 0; i < 5; ++i)
            EXPECT_EQ((*entries)[i]->get_term(), start + i) << "Wrong term at index " << (start + i);
    }

    const uint64_t created_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];
    EXPECT_EQ(created_after - created_before, 1u)
        << "Crossing sealed-file boundaries must not tear down and recreate the read-ahead reader";

    // The fill's decodes count only into KeeperLogsReadAheadFillDecodedEntries, never into
    // KeeperLogsEntryReadFromFile (the direct-read counter), so no direct-read fallback should have occurred.
    const uint64_t fill_decoded_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadFillDecodedEntries];
    const uint64_t from_file_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile];
    EXPECT_EQ(from_file_after, from_file_before) << "No entry should have needed a direct-read fallback across the boundaries";
    EXPECT_GT(fill_decoded_after, fill_decoded_before) << "Entries must have been decoded by the fill";
}


// A reader chasing the moving tip of the active (unrotated) file must resume in place rather than being
// recreated -- same park-on-EOF mechanism as a sealed-file boundary, against a moving tip instead.
TYPED_TEST(CoordinationChangelogTest, ReadAheadReaderSurvivesActiveFileTipCatchup)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 1000, // single active (unrotated) file for the whole test
        .latest_logs_cache_size_threshold = 1,
    };

    {
        DB::Changelog writer(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 20; ++i)
            writer.appendEntry(i, getLogEntry("tip_catchup_" + std::to_string(i), i));
        writer.flush();
    }

    this->keeper_context->setLastCommitIndex(20);

    DB::Changelog changelog(
        this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);

    const int32_t peer_id = 41;
    const uint64_t created_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];

    auto plan1 = changelog.getReadAheadPlan(1, 11, 0);
    ASSERT_TRUE(plan1.read_ahead_window.has_value());
    auto entries1 = changelog.serveReadAhead(peer_id, plan1);
    ASSERT_NE(entries1, nullptr);
    ASSERT_EQ(entries1->size(), 10u);
    for (uint64_t i = 0; i < 10; ++i)
        EXPECT_EQ((*entries1)[i]->get_term(), i + 1) << "Wrong term at index " << (i + 1);

    // The window covers the whole flushed file, so the fill parks at the tip once it catches up. Two
    // more rounds chase that tip: append past the boundary, flush, read with the SAME reader.
    for (int round = 0; round < 2; ++round)
    {
        const uint64_t base = 20 + static_cast<uint64_t>(round) * 30;
        for (uint64_t i = base + 1; i <= base + 30; ++i)
            changelog.appendEntry(i, getLogEntry("tip_catchup_" + std::to_string(i), i));
        changelog.flush();

        auto plan = changelog.getReadAheadPlan(base + 1, base + 11, 0);
        auto entries = changelog.serveReadAhead(peer_id, plan);
        ASSERT_NE(entries, nullptr);
        ASSERT_EQ(entries->size(), 10u);
        for (uint64_t i = 0; i < 10; ++i)
            EXPECT_EQ((*entries)[i]->get_term(), base + 1 + i) << "Wrong term at index " << (base + 1 + i);
    }

    const uint64_t created_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];
    EXPECT_EQ(created_after - created_before, 1u)
        << "Chasing the active file's moving tip must not force the reader to be recreated";
}

// Race installing a new cursor while the fill task is parked at the "EOF with nothing queued" decision
// point: must never miss the wakeup or force the reader to be recreated.
TYPED_TEST(CoordinationChangelogTest, ReadAheadParkOnEofRace)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Read-ahead engine mechanics are independent of compression; body always uses "
                        "compress_logs=false.";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 5,
        .latest_logs_cache_size_threshold = 1,
    };

    {
        DB::Changelog writer(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 10; ++i)
            writer.appendEntry(i, getLogEntry("park_race_" + std::to_string(i), i));
        writer.flush();
    }

    DB::Changelog changelog(
        this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{.enabled = true, .serve_wait_timeout_ms = 5000}, this->keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);

    const int32_t peer_id = 51;
    const uint64_t created_before = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];

    auto plan1 = changelog.getReadAheadPlan(1, 6, 0);
    ASSERT_TRUE(plan1.read_ahead_window.has_value());
    ASSERT_EQ(plan1.read_ahead_window->front().count, 5u) << "file1 holds exactly 5 entries with rotate_interval=5";

    auto plan2 = changelog.getReadAheadPlan(6, 11, 0);
    ASSERT_TRUE(plan2.read_ahead_window.has_value());

    // Must arm before the fill task reaches the Eof/park point, or waitForPause() below would hang.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_park_armed);

    // Runs on a separate thread: the failpoint blocks while holding deque_mutex, which drainReader also
    // needs, so calling this synchronously would self-deadlock.
    std::promise<DB::LogEntriesPtr> first_promise;
    std::thread first_reader([&]
    {
        first_promise.set_value(changelog.serveReadAhead(peer_id, plan1));
    });

    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_readahead_park_armed);

    // The fill is parked holding deque_mutex; this install will block on it until the failpoint releases.
    std::promise<DB::LogEntriesPtr> second_promise;
    std::thread second_reader([&]
    {
        second_promise.set_value(changelog.serveReadAhead(peer_id, plan2));
    });

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_park_armed);

    auto entries1 = first_promise.get_future().get();
    auto entries2 = second_promise.get_future().get();
    first_reader.join();
    second_reader.join();

    ASSERT_NE(entries1, nullptr);
    ASSERT_EQ(entries1->size(), 5u);
    for (uint64_t i = 0; i < 5; ++i)
        EXPECT_EQ((*entries1)[i]->get_term(), i + 1) << "Wrong term at index " << (i + 1);

    ASSERT_NE(entries2, nullptr);
    ASSERT_EQ(entries2->size(), 5u);
    for (uint64_t i = 0; i < 5; ++i)
        EXPECT_EQ((*entries2)[i]->get_term(), 6 + i) << "Wrong term at index " << (6 + i);

    const uint64_t created_after = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];
    EXPECT_EQ(created_after - created_before, 1u)
        << "Racing an install against the EOF park must never force the reader to be recreated";
}

// Without byte-aware chunking, a single decodeChunk call can overshoot window_budget_bytes by up to
// chunk_size * entry_size; the fix must keep the buffered total within budget + one entry's size.
TYPED_TEST(CoordinationChangelogTest, ReadAheadByteAwareChunking)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 100,
        .latest_logs_cache_size_threshold = 1,
    };

    // Measured directly so the test doesn't guess at internal serialization overhead.
    const std::string big_payload(20000, 'x');
    const size_t entry_size = getLogEntry(big_payload, 1)->get_buf().size();

    {
        DB::Changelog writer(this->log, settings, DB::FlushSettings(), DB::ReadAheadSettings{}, this->keeper_context);
        writer.readChangelogAndInitWriter(0, 0);
        for (uint64_t i = 1; i <= 20; ++i)
            writer.appendEntry(i, getLogEntry(big_payload, i));
        writer.flush();
    }

    this->keeper_context->setLastCommitIndex(20);

    // budget sized to ~1.5 entries; chunk_size covers the whole 20-entry file, so an unbounded chunk
    // decode (the pre-fix behavior) would pull in all 20 entries in a single decodeChunk call.
    const size_t budget = entry_size + entry_size / 2;
    DB::Changelog changelog(
        this->log,
        settings,
        DB::FlushSettings(),
        DB::ReadAheadSettings{
            .enabled = true, .window_bytes = budget, .max_peer_readers = 8, .serve_wait_timeout_ms = 5000, .chunk_size = 25},
        this->keeper_context);
    changelog.readChangelogAndInitWriter(0, 0);

    const int32_t peer_id = 61;

    // Request only the first entry; the window naturally covers the whole file, so the fill decodes ahead.
    auto plan = changelog.getReadAheadPlan(1, 2, 0);
    ASSERT_TRUE(plan.read_ahead_window.has_value());
    ASSERT_FALSE(plan.read_ahead_window->empty());
    EXPECT_EQ(plan.read_ahead_window->front().count, 20u);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_park_armed);

    // Runs on a separate thread: the failpoint blocks while holding deque_mutex, which drainReader also
    // needs, so calling this synchronously would self-deadlock.
    std::promise<DB::LogEntriesPtr> result_promise;
    std::thread reader([&]
    {
        result_promise.set_value(changelog.serveReadAhead(peer_id, plan));
    });

    // Wait for the background fill to park at the byte budget, then let it enter cv.wait (which
    // releases deque_mutex) before reading the buffered byte total below.
    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_readahead_park_armed);
    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_park_armed);

    const size_t decoded_bytes_at_park = changelog.getReaderDecodedBytesForTests(peer_id);

    auto entries = result_promise.get_future().get();
    reader.join();

    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 1u);

    EXPECT_GT(decoded_bytes_at_park, 0u) << "Forward-progress guarantee: at least one entry must have been buffered";
    EXPECT_LE(decoded_bytes_at_park, budget + entry_size)
        << "Byte-aware chunking must not overshoot the budget by more than one entry's size";
}


// A peer reader idle after disk catch-up must be evicted even once follow-up requests are served
// purely from latest_logs_cache (never re-entering the reader-acquisition path).
TYPED_TEST(CoordinationChangelogTest, ReadAheadIdleReaderEvictedAfterCacheHitCatchup)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    constexpr uint64_t total = 100;
    constexpr uint64_t cached_tail = 20;
    const std::string payload = "peer_idle_evict_pad_entry";
    const uint64_t threshold = cached_tail * payload.size();

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = threshold,
    };
    const DB::ReadAheadSettings readahead_settings{
        .enabled = true,
        .window_bytes = 64 * 1024 * 1024,
        .max_peer_readers = 8,
        .eviction_timeout_ms = 0,
        .serve_wait_timeout_ms = 5000,
        .commit_window_bytes = 1,
    };

    // Fresh instance's init-read path deterministically pins the cache boundary.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= total; ++i)
        {
            auto entry = getLogEntry(payload, i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    constexpr int32_t peer_id = 4;

    auto disk_batch = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0, peer_id);
    ASSERT_NE(disk_batch, nullptr);
    ASSERT_EQ(disk_batch->size(), 5u);

    const uint64_t created_before_hit = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];

    // Entirely within the cached tail: serveReadAhead takes the cache-hit early return.
    auto cache_hit
        = changelog.log_entries_ext(total - cached_tail + 1, total - cached_tail + 2, /*batch_size_hint_in_bytes=*/0, peer_id);
    ASSERT_NE(cache_hit, nullptr);
    ASSERT_EQ(cache_hit->size(), 1u);

    EXPECT_EQ(changelog.getReaderDecodedBytesForTests(peer_id), 0u) << "The idle peer reader must be evicted";

    auto disk_batch2 = changelog.log_entries_ext(6, 11, /*batch_size_hint_in_bytes=*/0, peer_id);
    ASSERT_NE(disk_batch2, nullptr);
    ASSERT_EQ(disk_batch2->size(), 5u);

    const uint64_t created_after_hit = ProfileEvents::global_counters[ProfileEvents::KeeperLogsReadAheadReadersCreated];
    EXPECT_GT(created_after_hit, created_before_hit) << "The evicted reader must have been recreated";
}

// The commit reader is exempt from capacity limits but not idle eviction; refreshCache is the only
// scan opportunity when serveReadAhead never runs (single-node/write-only).
TYPED_TEST(CoordinationChangelogTest, CommitReadAheadIdleReaderEvictedViaRefreshCache)
{
    if (this->enable_compression)
        GTEST_SKIP() << "Compressed logs not supported in executeReadPlan seek path";

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    constexpr uint64_t total = 100;
    constexpr uint64_t cached_tail = 20;
    const std::string payload = "commit_idle_evict_pad_entry";
    const uint64_t threshold = cached_tail * payload.size();

    const DB::LogFileSettings settings{
        .force_sync = false,
        .compress_logs = false,
        .rotate_interval = 10,
        .latest_logs_cache_size_threshold = threshold,
    };
    const DB::ReadAheadSettings readahead_settings{
        .enabled = false, .eviction_timeout_ms = 0, .serve_wait_timeout_ms = 5000, .commit_window_bytes = 64 * 1024 * 1024};

    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
        writer.init(0, 0);
        for (uint64_t i = 1; i <= total; ++i)
        {
            auto entry = getLogEntry(payload, i);
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), readahead_settings, this->keeper_context);
    changelog.init(0, 0);

    auto entry1 = changelog.entry_at_ext(1, /*for_commit=*/true);
    ASSERT_NE(entry1, nullptr);
    EXPECT_EQ(entry1->get_term(), 1u);
    ASSERT_TRUE(changelog.hasCommitReaderForTests());

    // Hand off to the latest cache; the commit reader is now idle.
    for (uint64_t i = total - cached_tail + 1; i <= total; ++i)
    {
        auto entry = changelog.entry_at_ext(i, /*for_commit=*/true);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->get_term(), i);
    }

    // flush() -> refreshCache() -> maybeEvictIdleReaders() is what triggers the retirement below.
    changelog.flush();

    EXPECT_FALSE(changelog.hasCommitReaderForTests()) << "The idle commit reader must be retired";

    auto entry2 = changelog.entry_at_ext(2, /*for_commit=*/true);
    ASSERT_NE(entry2, nullptr);
    EXPECT_EQ(entry2->get_term(), 2u);
    EXPECT_TRUE(changelog.hasCommitReaderForTests());
}

#endif
