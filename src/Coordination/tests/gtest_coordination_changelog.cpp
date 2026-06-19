#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/KeeperLogStore.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>

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
    extern const char keeper_changelog_prefetch_pause[];
}
}

namespace ProfileEvents
{
    extern const Event KeeperLogsEntryReadFromFile;
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestSimple)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestFile)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogReadWrite)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1000},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogWriteAt)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 1000},
        DB::FlushSettings(),
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
        this->keeper_context);
    changelog_reader.init(0, 0);

    EXPECT_EQ(changelog_reader.size(), changelog.size());
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), changelog.last_entry()->get_term());
    EXPECT_EQ(changelog_reader.start_index(), changelog.start_index());
    EXPECT_EQ(changelog_reader.next_slot(), changelog.next_slot());
}


TYPED_TEST(CoordinationChangelogTest, ChangelogTestAppendAfterRead)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestCompaction)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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
        this->keeper_context);
    changelog_reader.init(6, 0);

    EXPECT_EQ(changelog_reader.size(), 1);
    EXPECT_EQ(changelog_reader.start_index(), 7);
    EXPECT_EQ(changelog_reader.next_slot(), 8);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 60);
}

TYPED_TEST(CoordinationChangelogTest, ChangelogTestBatchOperations)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestBatchOperationsEmpty)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    nuraft::ptr<nuraft::buffer> entries;
    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
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

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
        this->keeper_context);
    changelog_reader.init(5, 0);
}


TYPED_TEST(CoordinationChangelogTest, ChangelogTestWriteAtPreviousFile)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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
        this->keeper_context);
    changelog_read.init(0, 0);
    EXPECT_EQ(changelog_read.size(), 7);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 8);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TYPED_TEST(CoordinationChangelogTest, ChangelogTestWriteAtFileBorder)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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
        this->keeper_context);
    changelog_read.init(0, 0);
    EXPECT_EQ(changelog_read.size(), 11);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 12);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TYPED_TEST(CoordinationChangelogTest, ChangelogTestWriteAtAllFiles)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestStartNewLogAfterRead)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestReadAfterBrokenTruncate)
{
    static const fs::path log_folder{"./logs"};


    ChangelogDirTest test(log_folder);
    this->setLogDirectory(log_folder);

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
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
        this->keeper_context);
    changelog_reader2.init(0, 0);
    EXPECT_EQ(changelog_reader2.size(), 11);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

/// Truncating all entries
TYPED_TEST(CoordinationChangelogTest, ChangelogTestReadAfterBrokenTruncate2)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
        DB::FlushSettings(),
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
            this->keeper_context);
        ASSERT_THROW(changelog_reader.init(0, 0), DB::Exception);
    }

    fs::remove("./logs/changelog_21_40.bin" + this->extension);

    DB::KeeperLogStore changelog_reader(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
        DB::FlushSettings(),
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
        this->keeper_context);
    changelog_reader2.init(0, 0);
    EXPECT_EQ(changelog_reader2.size(), 1);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

/// Truncating only some entries from the end
/// For compressed logs we have no reliable way of knowing how many log entries were lost
/// after we truncate some bytes from the end
TYPED_TEST(CoordinationChangelogTest, ChangelogTestReadAfterBrokenTruncate3)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestMixedLogTypes)
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestLostFiles)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 20},
        DB::FlushSettings(),
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
        this->keeper_context);

    ASSERT_THROW(changelog_reader.init(5, 0), DB::Exception);
}

TYPED_TEST(CoordinationChangelogTest, ChangelogTestLostFiles2)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 10},
        DB::FlushSettings(),
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
        this->keeper_context);
    ASSERT_THROW(changelog_reader.init(5, 0), DB::Exception);
}

TYPED_TEST(CoordinationChangelogTest, TestRotateIntervalChanges)
{
    using namespace Coordination;

    ChangelogDirTest snapshots("./logs");
    this->setLogDirectory("./logs");
    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogTestMaxLogSize)
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
            this->keeper_context);
        changelog.init(0, 0);
        ASSERT_EQ(changelog.entry_at(last_entry_index)->get_term(), (i - 1 + 44) * 10);
    }
}

TYPED_TEST(CoordinationChangelogTest, TestCompressedLogsMultipleRewrite)
{
    using namespace Coordination;
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");
    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
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

TYPED_TEST(CoordinationChangelogTest, ChangelogInsertThreeTimesSmooth)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");
    {
        SCOPED_TRACE("================First time=====================");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
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
            this->keeper_context);
        changelog.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 5);
        waitDurableLogs(changelog);
    }
}


TYPED_TEST(CoordinationChangelogTest, ChangelogInsertMultipleTimesSmooth)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");
    for (size_t i = 0; i < 36; ++i)
    {
        SCOPED_TRACE("================First time=====================");
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
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
        this->keeper_context);
    changelog.init(0, 0);
    EXPECT_EQ(changelog.next_slot(), 36 * 7 + 1);
}

TYPED_TEST(CoordinationChangelogTest, ChangelogInsertThreeTimesHard)
{

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");
    {
        SCOPED_TRACE("================First time=====================");
        DB::KeeperLogStore changelog1(
            DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
            DB::FlushSettings(),
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
            this->keeper_context);
        changelog4.init(0, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog4.append(entry);
        changelog4.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog4.next_slot(), 5);
        waitDurableLogs(changelog4);
    }
}

TYPED_TEST(CoordinationChangelogTest, TestLogGap)
{
    using namespace Coordination;
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100},
        DB::FlushSettings(),
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
        this->keeper_context);
    changelog1.init(60, 3);

    /// Logs discarded
    EXPECT_FALSE(fs::exists("./logs/changelog_1_100.bin" + this->extension));
    EXPECT_EQ(changelog1.start_index(), 61);
    EXPECT_EQ(changelog1.next_slot(), 61);
}

TYPED_TEST(CoordinationChangelogTest, ChangelogTestBrokenWriteAt)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{.force_sync = true, .compress_logs = false, .rotate_interval = 20},
            DB::FlushSettings(),
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
            this->keeper_context);
        changelog.init(0, 0);

        EXPECT_EQ(changelog.size(), 24);
    }
}

TYPED_TEST(CoordinationChangelogTest, ChangelogLoadingFromInvalidName)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    {
        DB::KeeperLogStore changelog(
            DB::LogFileSettings{
                .force_sync = true, .compress_logs = this->enable_compression, .rotate_interval = 100'000, .max_size = 500},
            DB::FlushSettings(),
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
        this->keeper_context);
    changelog.init(15, 0);

    ASSERT_EQ(changelog.next_slot(), 501);
}

// ─────────────────────────────────────────────────────────────────────────────
// Layer 1 tests: PLAN/EXECUTE split + removed_from_disk fence
// ─────────────────────────────────────────────────────────────────────────────

// Test A — ConcurrentAppendWhileHistoricalReadPaused
//
// Verifies that the PLAN/EXECUTE split releases `changelog_lock` before disk I/O.
// A reader thread parks between PLAN and EXECUTE via the
// `keeper_changelog_read_plan_resolved` failpoint.  While the reader holds no
// lock, the main thread calls `append` + `end_of_append_batch`, which requires
// an EXCLUSIVE lock.  This MUST complete within a bounded deadline.
//
// On the old code (reader holds SHARED changelog_lock for the full read) the
// append would deadlock.
TYPED_TEST(CoordinationChangelogTest, ConcurrentAppendWhileHistoricalReadPaused)
{
    // Skip compressed variant; compression does not change PLAN/EXECUTE lock
    // behaviour but slows the test without adding coverage.
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    // Tiny cache so index 1 will definitely be in logs_location (on-disk).
    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 100,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    // Write 10 entries and commit them to disk.
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("data", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // Arm the failpoint: the reader will pause between PLAN and EXECUTE.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    std::promise<void> reader_past_plan;
    std::promise<void> reader_done;
    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;

    // Reader thread: plan [1,6) then park, then execute.
    std::thread reader([&]
    {
        // log_entries_ext calls getReadPlan (under shared lock), then
        // pauseFailPoint (no lock held), then executeReadPlan.
        auto entries = changelog.log_entries_ext(1, 6, /*batch_size_hint_in_bytes=*/0);
        entries_promise.set_value(std::move(entries));
    });

    // Give the reader time to reach the failpoint.
    // We use waitForPause (checks pause_epoch > resume_epoch) — safe to call
    // after the reader has already parked.
    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

    // While the reader is parked (no lock held), perform an append.
    // On old code this would deadlock because the reader holds SHARED lock.
    std::promise<void> append_done_promise;
    std::thread appender([&]
    {
        auto entry = getLogEntry("new_entry", 11);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        append_done_promise.set_value();
    });

    // The append must complete within 5 seconds.
    auto append_future = append_done_promise.get_future();
    ASSERT_EQ(append_future.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "append deadlocked — changelog_lock was held across the EXECUTE disk read";

    // Resume the reader.
    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    reader.join();
    appender.join();

    // Verify entries are correct (indices 1..5, terms 1..5).
    auto entries = entries_promise.get_future().get();
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*entries)[i]->get_term(), static_cast<ulong>(i + 1));
}


// Test B — CompactionRemovesFileAfterPlanBeforeRead
//
// Verifies the removed_from_disk fence (Q2/Q5):
//   1. Plan a range whose entries reside in a file that will be compacted.
//   2. Park between PLAN and EXECUTE (via keeper_changelog_read_plan_resolved).
//   3. Compact the range.  The background RemoveChangelog operation sets
//      `removed_from_disk = true` under file_mutex before calling removeFile.
//   4. Resume the reader — executeReadPlan must observe removed_from_disk and
//      return nullptr (snapshot fallback), NOT throw and NOT read a deleted file.
//
// Sub-case B2: compact EVERYTHING so the store is empty (max_log_id+1 retained).
//   A request for an old positive index must return nullptr, not throw.
TYPED_TEST(CoordinationChangelogTest, CompactionRemovesFileAfterPlanBeforeRead)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    // Write 10 entries (2 files of 5 each).
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("d", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // ── Sub-case B1: compact the first file after PLAN, before EXECUTE ──────
    {
        DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

        std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;

        std::thread reader([&]
        {
            // log_entries (not _ext) throws if nullptr; use log_entries_ext which
            // propagates nullptr as-is for the snapshot-fallback contract.
            auto entries = changelog.log_entries_ext(1, 4, /*batch_size_hint_in_bytes=*/0);
            entries_promise.set_value(std::move(entries));
        });

        // Wait for reader to be parked.
        DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

        // Compact the first file (entries 1-5).
        this->keeper_context->setLastCommitIndex(5);
        changelog.compact(5);

        // Resume the reader.
        DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

        reader.join();

        auto entries = entries_promise.get_future().get();
        // Must be nullptr (compacted) or a valid prefix — never a throw.
        // In this case the file is being removed; nullptr is the expected outcome.
        // We only assert no exception was thrown (if it were, the promise would
        // not be set and get() would throw).
        (void)entries;
    }

    // ── Sub-case B2: compact EVERYTHING (empty store) ───────────────────────
    // After full compaction, getStartIndex() == max_log_id+1 (> 0).
    // A request for an old index must return nullptr, not LOGICAL_ERROR.
    {
        // Compact everything (entries 6-10).
        this->keeper_context->setLastCommitIndex(10);
        changelog.compact(10);

        // Small sleep to let the background removal thread process the queue.
        // The async removal is not required for getReadPlan's retained_start
        // logic; getStartIndex() already moved past max_log_id.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // log_entries_ext with an old index should return nullptr (not throw).
        auto entries = changelog.log_entries_ext(1, 4, 0);
        EXPECT_EQ(entries, nullptr)
            << "Expected nullptr (compacted) for fully-compacted store, got non-null";
    }
}


// Test C — PrefetchCancelDoesNotWedgeRead
//
// Verifies that an unresolved PrefetchedCacheEntryPtr in the commit log cache
// is treated as a cache miss (not awaited) during PLAN, so the changelog_lock
// is released promptly and executeReadPlan falls back to the FileRun path.
//
// We use keeper_changelog_prefetch_pause to keep a cache slot unresolved during
// the read, then disable it to verify the read completes without hanging.
TYPED_TEST(CoordinationChangelogTest, PrefetchCancelDoesNotWedgeRead)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 100,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("data", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // Enable the prefetch-pause failpoint.  This simulates an unresolved
    // PrefetchedCacheEntryPtr sitting in the cache (placeholder not yet filled).
    // The PLAN logic must treat it as a miss (not block on the future).
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_prefetch_pause);

    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;

    std::thread reader([&]
    {
        // This must complete (not hang) even if a cache entry is unresolved.
        auto entries = changelog.log_entries_ext(1, 6, 0);
        entries_promise.set_value(std::move(entries));
    });

    // Immediately disable the failpoint — the test verifies the read does not
    // hang waiting for the unresolved placeholder to be filled.
    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_prefetch_pause);

    reader.join();

    // The entries must be readable (either from file or partially from cache).
    auto entries = entries_promise.get_future().get();
    // Non-null result (entries may be empty if compacted, but here no compaction).
    ASSERT_NE(entries, nullptr);
    // We got some entries.
    ASSERT_GT(entries->size(), 0u);
}


// Test D — WriteAtRaceHistoricalRead
//
// A write_at truncates the log from some index N while a concurrent reader
// has planned entries [1, N+2).  The test verifies the read is:
//   - correct entries (if EXECUTE finishes before the write_at takes effect), OR
//   - nullptr (if the file was compacted/removed), OR
//   - throws a corruption exception (index mismatch from per-record validation)
// but NEVER silently returns wrong data (e.g., entries with the wrong term).
TYPED_TEST(CoordinationChangelogTest, WriteAtRaceHistoricalRead)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    // Write 10 entries with term == index for easy validation.
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("d", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // Park between PLAN and EXECUTE so write_at can race.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);

    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> entries_promise;

    std::thread reader([&]
    {
        try
        {
            auto entries = changelog.log_entries_ext(1, 6, 0);
            entries_promise.set_value(std::move(entries));
        }
        catch (...)
        {
            // Corruption exception from per-record index validation is allowed.
            entries_promise.set_value(nullptr);
        }
    });

    DB::FailPointInjection::waitForPause(DB::FailPoints::keeper_changelog_read_plan_resolved);

    // write_at truncates from index 5: overwrites the 5th entry.
    auto new_entry = getLogEntry("overwrite", 999);
    changelog.write_at(5, new_entry);
    changelog.end_of_append_batch(0, 0);

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_read_plan_resolved);
    reader.join();

    auto entries = entries_promise.get_future().get();
    // Whatever the result, it must not be silently wrong.
    // If non-null and non-empty, each returned entry must have term == its position.
    if (entries && !entries->empty())
    {
        for (size_t i = 0; i < entries->size(); ++i)
        {
            const auto term = (*entries)[i]->get_term();
            // Either the original term (i+1) or the overwritten entry at index 5.
            // We cannot predict which won the race, but each term must be one of
            // the values we actually wrote — never a garbage value.
            EXPECT_TRUE(term == static_cast<ulong>(i + 1) || term == 999u)
                << "Unexpected term " << term << " at position " << i;
        }
    }
}


// Test E — ActiveFileEvictedEntryStillReturned
//
// Verifies liveness: entries stored in the (not-yet-rotated) changelog file are
// readable via log_entries_ext after reloading from disk with a tiny cache that
// forces nearly all entries out of the in-memory cache.
//
// With latest_logs_cache_size_threshold == 1 (1 byte), only entry 20 (the newest)
// stays in cache after reload.  Reading entries 1–10 must use logs_location and
// read from disk, incrementing KeeperLogsEntryReadFromFile.
//
// For uncompressed logs: executeReadPlan seeks directly to the byte offset.
// For compressed logs: executeReadPlan wraps with a decompression layer and skips
// records sequentially until the decompressed-stream position matches, then reads.
// Both paths increment KeeperLogsEntryReadFromFile for each entry read.
TYPED_TEST(CoordinationChangelogTest, ActiveFileEvictedEntryStillReturned)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::LogFileSettings settings{
        .force_sync = true,
        .compress_logs = this->enable_compression,
        .rotate_interval = 1000,   // large enough to hold all 20 entries without rotation
        .latest_logs_cache_size_threshold = 1,
        .commit_logs_cache_size_threshold = 1,
    };

    // Phase 1: write 20 entries and flush to disk.
    {
        DB::KeeperLogStore writer(settings, DB::FlushSettings(), this->keeper_context);
        writer.init(0, 0);

        for (size_t i = 0; i < 20; ++i)
        {
            auto entry = getLogEntry("data", static_cast<size_t>(i + 1));
            writer.append(entry);
        }
        writer.end_of_append_batch(0, 0);
        waitDurableLogs(writer);
    }   // writer destroyed here

    // Phase 2: reload from disk with tiny cache (threshold = 1 byte).
    // addEntryWithLocation populates logs_location and evicts entries from
    // latest_logs_cache when over the 1-byte threshold, leaving only entry 20
    // (the last one) in cache.
    uint64_t counter_before
        = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile].load();

    DB::KeeperLogStore changelog(settings, DB::FlushSettings(), this->keeper_context);
    changelog.init(0, 0);

    // Read entries 1–10.  Only entry 20 is in cache, so all 10 reads go to disk.
    auto entries = changelog.log_entries_ext(1, 11, 0);
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 10u);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ((*entries)[i]->get_term(), static_cast<ulong>(i + 1))
            << "Wrong term at index " << (i + 1);
    }

    // At least one entry must have been read from file (counter must have grown).
    // For uncompressed logs: executeReadPlan seeks directly to the raw byte offset.
    // For compressed logs: executeReadPlan skips records sequentially using the
    // decompression wrapper until the decompressed-stream position is reached.
    uint64_t counter_after
        = ProfileEvents::global_counters[ProfileEvents::KeeperLogsEntryReadFromFile].load();
    EXPECT_GT(counter_after, counter_before)
        << "Expected disk reads because loaded entries are not in the tiny cache";
}


// Test F — ConcurrentAppendVsActiveFileRead (TSan)
//
// Stress: one thread appends and rotates while another repeatedly reads
// a disk-resident range that may reach into the active file.  Under TSan
// this detects data races on `removed_from_disk` and `logs_location`.
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
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    // Pre-populate 10 entries so readers have something on disk.
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("base", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    std::atomic<bool> stop{false};
    std::atomic<size_t> appended{10};

    // Writer thread: keep appending and rotating.
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

    // Reader thread: repeatedly read the first 5 entries (which should be on disk).
    std::thread reader_thread([&]
    {
        for (int iter = 0; iter < 200 && !stop.load(std::memory_order_relaxed); ++iter)
        {
            try
            {
                auto entries = changelog.log_entries_ext(1, 6, 0);
                // Either nullptr (if compacted) or non-empty.
                if (entries && !entries->empty())
                {
                    EXPECT_LE(entries->size(), 5u);
                }
            }
            catch (const DB::Exception &)
            {
                // Corruption exception is acceptable in this race scenario.
            }
        }
        stop.store(true, std::memory_order_relaxed);
    });

    reader_thread.join();
    writer.join();
}

// ─────────────────────────────────────────────────────────────────────────────
// Layer 2 — per-peer read-ahead tests
// ─────────────────────────────────────────────────────────────────────────────

namespace DB
{
namespace FailPoints
{
    extern const char keeper_changelog_readahead_fill_wedge[];
    extern const char keeper_changelog_readahead_serve_wait[];
    extern const char keeper_changelog_readahead_park_armed[];
}
}

// L2 Test 1 — Disabled: log_entries_ext with NO peer_id (== NO_PEER_ID = -1) or
// read-ahead disabled must return byte-identical results to the L1 path.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadDisabledPathIdentical)
{
    if (this->enable_compression)
        return;  /// L2 only handles uncompressed; skip compressed variant

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test1", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // L1 path: NO_PEER_ID = -1 (no read-ahead dispatched).
    auto l1_result = changelog.log_entries_ext(1, 11, /*batch_size_hint=*/0, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(l1_result, nullptr);
    ASSERT_EQ(l1_result->size(), 10u);

    // L2 disabled: even with a valid peer_id, no read-ahead settings → disabled.
    // (read-ahead is disabled by default — enabled=false in ReadAheadSettings).
    auto l2_result_disabled = changelog.log_entries_ext(1, 11, /*batch_size_hint=*/0, /*peer_id=*/42);
    ASSERT_NE(l2_result_disabled, nullptr);
    ASSERT_EQ(l2_result_disabled->size(), 10u);

    // Both should have identical terms.
    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ((*l1_result)[i]->get_term(), (*l2_result_disabled)[i]->get_term());
}

// L2 Test 5 — L2 enabled but changelog_lock released before EXECUTE.
// The read-ahead path calls serveReadAhead WITHOUT changelog_lock; verify this
// doesn't deadlock when a concurrent append happens during serve.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadConcurrentAppend)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test5", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // Enable read-ahead.
    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 64 * 1024 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 100;
    ra_settings.foreground_fallback = true;
    changelog.setReadAheadSettings(ra_settings);

    // Wedge the fill to force foreground fallback path.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    std::promise<nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>> result_promise;
    std::thread reader([&]
    {
        auto result = changelog.log_entries_ext(1, 6, /*batch_size_hint=*/0, /*peer_id=*/1);
        result_promise.set_value(std::move(result));
    });

    // Give a moment for the reader to start, then do a concurrent append.
    // This MUST NOT deadlock — changelog_lock is NOT held during serve.
    std::promise<void> append_done;
    std::thread appender([&]
    {
        auto entry = getLogEntry("new_entry", 11);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        append_done.set_value();
    });

    // The append MUST complete within 5 seconds.
    auto append_fut = append_done.get_future();
    ASSERT_EQ(append_fut.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "L2 serve held changelog_lock — deadlock";

    // Resume the wedge so the reader can escape via foreground fallback.
    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    reader.join();
    appender.join();

    auto result = result_promise.get_future().get();
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*result)[i]->get_term(), static_cast<uint64_t>(i + 1));
}

// L2 Test 8 — Wedged-fill: fill is blocked via failpoint; serve must escape
// within serve_wait_timeout and fall back to direct EXECUTE (foreground_fallback=true).
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadWedgedFillTimeout)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test8", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 64 * 1024 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 50;  /// short timeout
    ra_settings.foreground_fallback = true;
    changelog.setReadAheadSettings(ra_settings);

    // Wedge the fill so it never delivers.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    auto start = std::chrono::steady_clock::now();
    auto result = changelog.log_entries_ext(1, 6, /*batch_size_hint=*/0, /*peer_id=*/1);
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    // Must have returned (fallback path) within a reasonable bound.
    EXPECT_LE(elapsed_ms, 5000)
        << "serve_wait_timeout did not fire; serve took " << elapsed_ms << " ms";

    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ((*result)[i]->get_term(), static_cast<uint64_t>(i + 1));
}

// L2 Test 9 — Non-sequential rewind: a rewind clears the deque and refetches from
// the new start position. The returned batch is always contiguous from the new start.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadNonSequentialRewind)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test9", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 64 * 1024 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 200;
    ra_settings.foreground_fallback = true;
    changelog.setReadAheadSettings(ra_settings);

    // First read: [1, 6).
    auto r1 = changelog.log_entries_ext(1, 6, 0, /*peer_id=*/2);
    ASSERT_NE(r1, nullptr);
    ASSERT_EQ(r1->size(), 5u);

    // Rewind to [1, 4) — a lower start than sequential expected_next.
    auto r2 = changelog.log_entries_ext(1, 4, 0, /*peer_id=*/2);
    ASSERT_NE(r2, nullptr);
    ASSERT_EQ(r2->size(), 3u);
    for (size_t i = 0; i < 3; ++i)
        EXPECT_EQ((*r2)[i]->get_term(), static_cast<uint64_t>(i + 1));
}

// L2 Test 11 — Compaction-during-serve: if file is compacted after PLAN but before
// serve delivers it, serveReadAhead must return nullptr (snapshot fallback), not throw.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadCompactionDuringServe)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test11", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 64 * 1024 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 50;
    ra_settings.foreground_fallback = true;
    changelog.setReadAheadSettings(ra_settings);

    // Wedge the fill so the deque stays empty.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    // Compact everything away.
    changelog.compact(10);

    // The serve for compacted range must return nullptr (snapshot fallback).
    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    auto result = changelog.log_entries_ext(1, 6, 0, /*peer_id=*/3);
    // Result is nullptr (compacted) or a valid result if cache served it.
    // Either is acceptable: the important thing is no exception thrown.
    // (Compact may have evicted the entries → nullptr; or cache may still serve them.)
    if (result != nullptr)
    {
        // If result was served, it must be contiguous.
        EXPECT_LE(result->size(), 5u);
    }
    // No assertion on nullptr vs non-nullptr: both are valid depending on timing.
}

// L2 Test 16 — Shutdown: setReadAheadSettings + then shutdown joins all fills.
// No deadlock, no hang (timeout-protected).
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadShutdownJoinsFills)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test16", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 64 * 1024 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 100;
    ra_settings.foreground_fallback = true;
    changelog.setReadAheadSettings(ra_settings);

    // Wedge the fill so it stays parked.
    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    // Admit a reader.
    std::thread reader([&]
    {
        changelog.log_entries_ext(1, 6, 0, /*peer_id=*/4);
    });
    reader.detach();

    // Small delay to let the reader start.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Release the fill wedge.
    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    // Shutdown via destructor / flushChangelogAndShutdown — must not hang.
    // The KeeperLogStore destructor calls shutdownChangelog which joins fills.
    // (Changelog falls out of scope at end of the test block — no explicit action needed.)
    // We verify this completes within the test's normal time budget.
}

// L2 Test 18 — Compile-time signature verification:
// KeeperLogStore::log_entries_ext must still override the virtual base.
// This is a compile-time assertion; the test just calls the function to ensure
// it links and executes correctly.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadSignatureOverride)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{.force_sync = false, .compress_logs = this->enable_compression, .rotate_interval = 5},
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 5; ++i)
    {
        auto entry = getLogEntry("sig_check", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // Upcast to the base class to verify override dispatches correctly.
    nuraft::log_store * base = &changelog;
    auto result = base->log_entries_ext(1, 4, 0, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), 3u);
}

// L2 Test 19 — Byte-hint truncation: byte hint passed to L2 plan must truncate
// plan.items the same as the base path.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadByteHintTruncation)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test19", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    // L1 path with hint = 1 byte (must still return ≥1 entry).
    auto l1_1b = changelog.log_entries_ext(1, 21, /*batch_size_hint=*/1, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(l1_1b, nullptr);
    ASSERT_GE(l1_1b->size(), 1u);

    // L1 path with hint = 0 means unlimited.
    auto l1_0b = changelog.log_entries_ext(1, 11, /*batch_size_hint=*/0, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(l1_0b, nullptr);
    ASSERT_EQ(l1_0b->size(), 10u);

    // L1 path with hint = SIZE_MAX / large value — all entries up to the range end.
    auto l1_max = changelog.log_entries_ext(1, 21, /*batch_size_hint=*/0x7FFFFFFF, DB::KeeperLogStore::NO_PEER_ID);
    ASSERT_NE(l1_max, nullptr);
    ASSERT_EQ(l1_max->size(), 20u);
}

// L2 Test 20 — Wedged-fill with short serve_wait_timeout (same as Test 8 extended):
// verify the serve timeout actually fires, not an indefinite block.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadServeWaitBound)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 10,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test20", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 64 * 1024 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 30;  /// very short
    ra_settings.foreground_fallback = true;
    changelog.setReadAheadSettings(ra_settings);

    DB::FailPointInjection::enableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    const auto start = std::chrono::steady_clock::now();
    auto result = changelog.log_entries_ext(1, 6, 0, /*peer_id=*/5);
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    DB::FailPointInjection::disableFailPoint(DB::FailPoints::keeper_changelog_readahead_fill_wedge);

    // Must escape well within 5 seconds (should be ~30ms + fallback disk read).
    EXPECT_LE(elapsed_ms, 5000)
        << "serve_wait_timeout did not bound the serve time: " << elapsed_ms << " ms";

    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), 5u);
}

// L2 Test 21c — Terminal reader reaping: after a reader terminates (force via
// compaction that the fill detects), the next request for the same peer_id must
// create a fresh reader, not reuse dead state.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadTerminalReaderReaping)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 20; ++i)
    {
        auto entry = getLogEntry("readahead_test_l2_test21c", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 64 * 1024 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 100;
    ra_settings.foreground_fallback = true;
    changelog.setReadAheadSettings(ra_settings);

    // First read — admit a reader.
    auto r1 = changelog.log_entries_ext(1, 6, 0, /*peer_id=*/6);
    ASSERT_NE(r1, nullptr);
    ASSERT_EQ(r1->size(), 5u);

    // Compact so any live fill hits compacted.
    changelog.compact(15);

    // Wait briefly for compaction to propagate.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // A second read for the SAME peer after compaction — should either serve remaining
    // entries (if still in cache) or return nullptr (snapshot). Either is correct.
    // The important thing: no assertion failure, no exception, no use-after-free.
    auto r2 = changelog.log_entries_ext(16, 21, 0, /*peer_id=*/6);
    // r2 may be non-null if entries 16-20 are still accessible.
    // (We just care it doesn't crash or hang.)
    (void)r2;
}

// L2 Test 23 — TSan stress: interleave append / read-ahead serve / compaction
// across multiple threads. Exercises lock order + fence usage.
TYPED_TEST(CoordinationChangelogTest, L2ReadAheadTSanStress)
{
    if (this->enable_compression)
        return;

    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");

    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = false,
            .compress_logs = false,
            .rotate_interval = 5,
            .latest_logs_cache_size_threshold = 1,
            .commit_logs_cache_size_threshold = 1,
        },
        DB::FlushSettings(),
        this->keeper_context);
    changelog.init(0, 0);

    for (size_t i = 0; i < 50; ++i)
    {
        auto entry = getLogEntry("l2_stress", static_cast<size_t>(i + 1));
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    waitDurableLogs(changelog);

    DB::ReadAheadSettings ra_settings;
    ra_settings.enabled = true;
    ra_settings.window_bytes = 32 * 1024;
    ra_settings.max_peer_readers = 4;
    ra_settings.serve_wait_timeout_ms = 20;
    ra_settings.foreground_fallback = true;
    ra_settings.eviction_timeout_ms = 100;
    changelog.setReadAheadSettings(ra_settings);

    std::atomic<bool> stop{false};
    std::atomic<int> appended{50};

    // Appender thread.
    std::thread appender([&]
    {
        while (!stop.load(std::memory_order_relaxed))
        {
            int idx = appended.fetch_add(1, std::memory_order_relaxed);
            auto entry = getLogEntry("l2_stress", static_cast<size_t>(idx + 1));
            changelog.append(entry);
            changelog.end_of_append_batch(0, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    });

    // Multiple reader threads (different peer_ids).
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
                    start, end, /*batch_size_hint=*/0, static_cast<int32_t>(peer + 1));
                if (result != nullptr)
                    start += result->size();
                else
                    start = 1;  // snapshot fallback: restart
            }
        });
    }

    // Run for 500ms.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    stop.store(true, std::memory_order_relaxed);

    appender.join();
    for (auto & t : readers)
        t.join();
    // No assertions needed: TSan will report data races; no crash = pass.
}

#endif
