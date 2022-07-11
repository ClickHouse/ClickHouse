#include <chrono>
#include <gtest/gtest.h>

#include "config_core.h"

#if USE_NURAFT
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/SummingStateMachine.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Exception.h>
#include <base/logger_useful.h>
#include <libnuraft/nuraft.hxx>
#include <thread>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/Changelog.h>
#include <filesystem>

#include <Coordination/SnapshotableHashTable.h>

namespace fs = std::filesystem;
struct ChangelogDirTest
{
    std::string path;
    bool drop;
    explicit ChangelogDirTest(std::string path_, bool drop_ = true)
        : path(path_)
        , drop(drop_)
    {
        if (fs::exists(path))
        {
            EXPECT_TRUE(false) << "Path " << path << " already exists, remove it to run test";
        }
        fs::create_directory(path);
    }

    ~ChangelogDirTest()
    {
        if (fs::exists(path) && drop)
            fs::remove_all(path);
    }
};

struct CompressionParam
{
    bool enable_compression;
    std::string extension;
};

class CoordinationTest : public ::testing::TestWithParam<CompressionParam>
{};

TEST_P(CoordinationTest, BuildTest)
{
    DB::InMemoryLogStore store;
    DB::SummingStateMachine machine;
    EXPECT_EQ(1, 1);
}

TEST_P(CoordinationTest, BufferSerde)
{
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Get);
    request->xid = 3;
    dynamic_cast<Coordination::ZooKeeperGetRequest &>(*request).path = "/path/value";

    DB::WriteBufferFromNuraftBuffer wbuf;
    request->write(wbuf);
    auto nuraft_buffer = wbuf.getBuffer();
    EXPECT_EQ(nuraft_buffer->size(), 28);

    DB::ReadBufferFromNuraftBuffer rbuf(nuraft_buffer);

    int32_t length;
    Coordination::read(length, rbuf);
    EXPECT_EQ(length + sizeof(length), nuraft_buffer->size());

    int32_t xid;
    Coordination::read(xid, rbuf);
    EXPECT_EQ(xid, request->xid);

    Coordination::OpNum opnum;
    Coordination::read(opnum, rbuf);

    Coordination::ZooKeeperRequestPtr request_read = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_read->xid = xid;
    request_read->readImpl(rbuf);

    EXPECT_EQ(request_read->getOpNum(), Coordination::OpNum::Get);
    EXPECT_EQ(request_read->xid, 3);
    EXPECT_EQ(dynamic_cast<Coordination::ZooKeeperGetRequest &>(*request_read).path, "/path/value");
}

template <typename StateMachine>
struct SimpliestRaftServer
{
    SimpliestRaftServer(int server_id_, const std::string & hostname_, int port_, const std::string & logs_path)
        : server_id(server_id_)
        , hostname(hostname_)
        , port(port_)
        , endpoint(hostname + ":" + std::to_string(port))
        , state_machine(nuraft::cs_new<StateMachine>())
        , state_manager(nuraft::cs_new<DB::KeeperStateManager>(server_id, hostname, port, logs_path))
    {
        state_manager->loadLogStore(1, 0);
        nuraft::raft_params params;
        params.heart_beat_interval_ = 100;
        params.election_timeout_lower_bound_ = 200;
        params.election_timeout_upper_bound_ = 400;
        params.reserved_log_items_ = 5;
        params.snapshot_distance_ = 1; /// forcefully send snapshots
        params.client_req_timeout_ = 3000;
        params.return_method_ = nuraft::raft_params::blocking;

        raft_instance = launcher.init(
            state_machine, state_manager, nuraft::cs_new<DB::LoggerWrapper>("ToyRaftLogger", DB::LogsLevel::trace), port,
            nuraft::asio_service::options{}, params);

        if (!raft_instance)
        {
            std::cerr << "Failed to initialize launcher" << std::endl;
            exit(-1);
        }

        std::cout << "init Raft instance " << server_id;
        for (size_t ii = 0; ii < 20; ++ii)
        {
            if (raft_instance->is_initialized())
            {
                std::cout << " done" << std::endl;
                break;
            }
            std::cout << ".";
            fflush(stdout);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // Server ID.
    int server_id;

    // Server address.
    std::string hostname;

    // Server port.
    int port;

    std::string endpoint;

    // State machine.
    nuraft::ptr<StateMachine> state_machine;

    // State manager.
    nuraft::ptr<DB::KeeperStateManager> state_manager;

    // Raft launcher.
    nuraft::raft_launcher launcher;

    // Raft server instance.
    nuraft::ptr<nuraft::raft_server> raft_instance;
};

using SummingRaftServer = SimpliestRaftServer<DB::SummingStateMachine>;

nuraft::ptr<nuraft::buffer> getBuffer(int64_t number)
{
    nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(sizeof(number));
    nuraft::buffer_serializer bs(ret);
    bs.put_raw(&number, sizeof(number));
    return ret;
}


TEST_P(CoordinationTest, TestSummingRaft1)
{
    ChangelogDirTest test("./logs");
    SummingRaftServer s1(1, "localhost", 44444, "./logs");

    /// Single node is leader
    EXPECT_EQ(s1.raft_instance->get_leader(), 1);

    auto entry1 = getBuffer(143);
    auto ret = s1.raft_instance->append_entries({entry1});
    EXPECT_TRUE(ret->get_accepted()) << "failed to replicate: entry 1" << ret->get_result_code();
    EXPECT_EQ(ret->get_result_code(), nuraft::cmd_result_code::OK) << "failed to replicate: entry 1" << ret->get_result_code();

    while (s1.state_machine->getValue() != 143)
    {
        std::cout << "Waiting s1 to apply entry\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(s1.state_machine->getValue(), 143);

    s1.launcher.shutdown(5);
}

DB::LogEntryPtr getLogEntry(const std::string & s, size_t term)
{
    DB::WriteBufferFromNuraftBuffer bufwriter;
    writeText(s, bufwriter);
    return nuraft::cs_new<nuraft::log_entry>(term, bufwriter.getBuffer());
}

TEST_P(CoordinationTest, ChangelogTestSimple)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);
    auto entry = getLogEntry("hello world", 77);
    changelog.append(entry);
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.next_slot(), 2);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.last_entry()->get_term(), 77);
    EXPECT_EQ(changelog.entry_at(1)->get_term(), 77);
    EXPECT_EQ(changelog.log_entries(1, 2)->size(), 1);
}


TEST_P(CoordinationTest, ChangelogTestFile)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);
    auto entry = getLogEntry("hello world", 77);
    changelog.append(entry);
    changelog.end_of_append_batch(0, 0);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    for (const auto & p : fs::directory_iterator("./logs"))
        EXPECT_EQ(p.path(), "./logs/changelog_1_5.bin" + params.extension);

    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.end_of_append_batch(0, 0);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
}

TEST_P(CoordinationTest, ChangelogReadWrite)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 1000, true, params.enable_compression);
    changelog.init(1, 0);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 10);

    DB::KeeperLogStore changelog_reader("./logs", 1000, true, params.enable_compression);
    changelog_reader.init(1, 0);
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

TEST_P(CoordinationTest, ChangelogWriteAt)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 1000, true, params.enable_compression);
    changelog.init(1, 0);
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

    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.last_entry()->get_term(), 77);
    EXPECT_EQ(changelog.entry_at(7)->get_term(), 77);
    EXPECT_EQ(changelog.next_slot(), 8);

    DB::KeeperLogStore changelog_reader("./logs", 1000, true, params.enable_compression);
    changelog_reader.init(1, 0);

    EXPECT_EQ(changelog_reader.size(), changelog.size());
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), changelog.last_entry()->get_term());
    EXPECT_EQ(changelog_reader.start_index(), changelog.start_index());
    EXPECT_EQ(changelog_reader.next_slot(), changelog.next_slot());
}


TEST_P(CoordinationTest, ChangelogTestAppendAfterRead)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);
    for (size_t i = 0; i < 7; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 7);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));

    DB::KeeperLogStore changelog_reader("./logs", 5, true, params.enable_compression);
    changelog_reader.init(1, 0);

    EXPECT_EQ(changelog_reader.size(), 7);
    for (size_t i = 7; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog_reader.append(entry);
    }
    changelog_reader.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));

    size_t logs_count = 0;
    for (const auto & _ [[maybe_unused]]: fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 2);

    auto entry = getLogEntry("someentry", 77);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog_reader.size(), 11);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));

    logs_count = 0;
    for (const auto & _ [[maybe_unused]]: fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 3);
}

TEST_P(CoordinationTest, ChangelogTestCompaction)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 3; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 3);

    changelog.compact(2);

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 3);
    EXPECT_EQ(changelog.next_slot(), 4);
    EXPECT_EQ(changelog.last_entry()->get_term(), 20);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));

    auto e1 = getLogEntry("hello world", 30);
    changelog.append(e1);
    auto e2 = getLogEntry("hello world", 40);
    changelog.append(e2);
    auto e3 = getLogEntry("hello world", 50);
    changelog.append(e3);
    auto e4 = getLogEntry("hello world", 60);
    changelog.append(e4);
    changelog.end_of_append_batch(0, 0);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));

    changelog.compact(6);
    std::this_thread::sleep_for(std::chrono::microseconds(200));

    EXPECT_FALSE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 7);
    EXPECT_EQ(changelog.next_slot(), 8);
    EXPECT_EQ(changelog.last_entry()->get_term(), 60);
    /// And we able to read it
    DB::KeeperLogStore changelog_reader("./logs", 5, true, params.enable_compression);
    changelog_reader.init(7, 0);

    EXPECT_EQ(changelog_reader.size(), 1);
    EXPECT_EQ(changelog_reader.start_index(), 7);
    EXPECT_EQ(changelog_reader.next_slot(), 8);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 60);
}

TEST_P(CoordinationTest, ChangelogTestBatchOperations)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
    changelog.init(1, 0);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 10);

    auto entries = changelog.pack(1, 5);

    DB::KeeperLogStore apply_changelog("./logs", 100, true, params.enable_compression);
    apply_changelog.init(1, 0);

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

TEST_P(CoordinationTest, ChangelogTestBatchOperationsEmpty)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
    changelog.init(1, 0);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog.size(), 10);

    auto entries = changelog.pack(5, 5);

    ChangelogDirTest test1("./logs1");
    DB::KeeperLogStore changelog_new("./logs1", 100, true, params.enable_compression);
    changelog_new.init(1, 0);
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

    DB::KeeperLogStore changelog_reader("./logs1", 100, true, params.enable_compression);
    changelog_reader.init(5, 0);
}


TEST_P(CoordinationTest, ChangelogTestWriteAtPreviousFile)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(7, e1);
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 8);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    DB::KeeperLogStore changelog_read("./logs", 5, true, params.enable_compression);
    changelog_read.init(1, 0);
    EXPECT_EQ(changelog_read.size(), 7);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 8);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TEST_P(CoordinationTest, ChangelogTestWriteAtFileBorder)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(11, e1);
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 11);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 12);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    DB::KeeperLogStore changelog_read("./logs", 5, true, params.enable_compression);
    changelog_read.init(1, 0);
    EXPECT_EQ(changelog_read.size(), 11);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 12);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TEST_P(CoordinationTest, ChangelogTestWriteAtAllFiles)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(1, e1);
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 2);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + params.extension));
}

TEST_P(CoordinationTest, ChangelogTestStartNewLogAfterRead)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 35);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_36_40.bin" + params.extension));


    DB::KeeperLogStore changelog_reader("./logs", 5, true, params.enable_compression);
    changelog_reader.init(1, 0);

    auto entry = getLogEntry("36_hello_world", 360);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog_reader.size(), 36);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_36_40.bin" + params.extension));
}


TEST_P(CoordinationTest, ChangelogTestReadAfterBrokenTruncate)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");

    DB::KeeperLogStore changelog("./logs", 5, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog.size(), 35);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    DB::WriteBufferFromFile plain_buf("./logs/changelog_11_15.bin" + params.extension, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(0);

    DB::KeeperLogStore changelog_reader("./logs", 5, true, params.enable_compression);
    changelog_reader.init(1, 0);
    changelog_reader.end_of_append_batch(0, 0);

    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 90);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    auto entry = getLogEntry("h", 7777);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog_reader.size(), 11);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 7777);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin" + params.extension));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin" + params.extension));

    DB::KeeperLogStore changelog_reader2("./logs", 5, true, params.enable_compression);
    changelog_reader2.init(1, 0);
    EXPECT_EQ(changelog_reader2.size(), 11);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

TEST_P(CoordinationTest, ChangelogTestReadAfterBrokenTruncate2)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");

    DB::KeeperLogStore changelog("./logs", 20, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_40.bin" + params.extension));

    DB::WriteBufferFromFile plain_buf("./logs/changelog_1_20.bin" + params.extension, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(140);

    DB::KeeperLogStore changelog_reader("./logs", 20, true, params.enable_compression);
    changelog_reader.init(1, 0);

    EXPECT_EQ(changelog_reader.size(), 0);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_40.bin" + params.extension));
    auto entry = getLogEntry("hello_world", 7777);
    changelog_reader.append(entry);
    changelog_reader.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog_reader.size(), 1);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 7777);


    DB::KeeperLogStore changelog_reader2("./logs", 1, true, params.enable_compression);
    changelog_reader2.init(1, 0);
    EXPECT_EQ(changelog_reader2.size(), 1);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

TEST_P(CoordinationTest, ChangelogTestLostFiles)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");

    DB::KeeperLogStore changelog("./logs", 20, true, params.enable_compression);
    changelog.init(1, 0);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }
    changelog.end_of_append_batch(0, 0);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_40.bin" + params.extension));

    fs::remove("./logs/changelog_1_20.bin" + params.extension);

    DB::KeeperLogStore changelog_reader("./logs", 20, true, params.enable_compression);
    /// It should print error message, but still able to start
    changelog_reader.init(5, 0);
    EXPECT_FALSE(fs::exists("./logs/changelog_1_20.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_40.bin" + params.extension));
}

struct IntNode
{
    int value;
    IntNode(int value_) : value(value_) { } // NOLINT(google-explicit-constructor)
    UInt64 sizeInBytes() const { return sizeof value; }
    IntNode & operator=(int rhs)
    {
        this->value = rhs;
        return *this;
    }
    bool operator==(const int & rhs) const { return value == rhs; }
    bool operator!=(const int & rhs) const { return rhs != this->value; }
};

TEST_P(CoordinationTest, SnapshotableHashMapSimple)
{
    DB::SnapshotableHashTable<IntNode> hello;
    EXPECT_TRUE(hello.insert("hello", 5).second);
    EXPECT_TRUE(hello.contains("hello"));
    EXPECT_EQ(hello.getValue("hello"), 5);
    EXPECT_FALSE(hello.insert("hello", 145).second);
    EXPECT_EQ(hello.getValue("hello"), 5);
    hello.updateValue("hello", [](IntNode & value) { value = 7; });
    EXPECT_EQ(hello.getValue("hello"), 7);
    EXPECT_EQ(hello.size(), 1);
    EXPECT_TRUE(hello.erase("hello"));
    EXPECT_EQ(hello.size(), 0);
}

TEST_P(CoordinationTest, SnapshotableHashMapTrySnapshot)
{
    DB::SnapshotableHashTable<IntNode> map_snp;
    EXPECT_TRUE(map_snp.insert("/hello", 7).second);
    EXPECT_FALSE(map_snp.insert("/hello", 145).second);
    map_snp.enableSnapshotMode(100000);
    EXPECT_FALSE(map_snp.insert("/hello", 145).second);
    map_snp.updateValue("/hello", [](IntNode & value) { value = 554; });
    EXPECT_EQ(map_snp.getValue("/hello"), 554);
    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 2);
    EXPECT_EQ(map_snp.size(), 1);

    auto itr = map_snp.begin();
    EXPECT_EQ(itr->key, "/hello");
    EXPECT_EQ(itr->value, 7);
    EXPECT_EQ(itr->active_in_map, false);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello");
    EXPECT_EQ(itr->value, 554);
    EXPECT_EQ(itr->active_in_map, true);
    itr = std::next(itr);
    EXPECT_EQ(itr, map_snp.end());
    for (size_t i = 0; i < 5; ++i)
    {
        EXPECT_TRUE(map_snp.insert("/hello" + std::to_string(i), i).second);
    }
    EXPECT_EQ(map_snp.getValue("/hello3"), 3);

    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 7);
    EXPECT_EQ(map_snp.size(), 6);
    itr = std::next(map_snp.begin(), 2);
    for (size_t i = 0; i < 5; ++i)
    {
        EXPECT_EQ(itr->key, "/hello" + std::to_string(i));
        EXPECT_EQ(itr->value, i);
        EXPECT_EQ(itr->active_in_map, true);
        itr = std::next(itr);
    }

    EXPECT_TRUE(map_snp.erase("/hello3"));
    EXPECT_TRUE(map_snp.erase("/hello2"));

    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 7);
    EXPECT_EQ(map_snp.size(), 4);
    itr = std::next(map_snp.begin(), 2);
    for (size_t i = 0; i < 5; ++i)
    {
        EXPECT_EQ(itr->key, "/hello" + std::to_string(i));
        EXPECT_EQ(itr->value, i);
        EXPECT_EQ(itr->active_in_map, i != 3 && i != 2);
        itr = std::next(itr);
    }
    map_snp.clearOutdatedNodes();

    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 4);
    EXPECT_EQ(map_snp.size(), 4);
    itr = map_snp.begin();
    EXPECT_EQ(itr->key, "/hello");
    EXPECT_EQ(itr->value, 554);
    EXPECT_EQ(itr->active_in_map, true);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello0");
    EXPECT_EQ(itr->value, 0);
    EXPECT_EQ(itr->active_in_map, true);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello1");
    EXPECT_EQ(itr->value, 1);
    EXPECT_EQ(itr->active_in_map, true);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello4");
    EXPECT_EQ(itr->value, 4);
    EXPECT_EQ(itr->active_in_map, true);
    itr = std::next(itr);
    EXPECT_EQ(itr, map_snp.end());
    map_snp.disableSnapshotMode();
}

TEST_P(CoordinationTest, SnapshotableHashMapDataSize)
{
    /// int
    DB::SnapshotableHashTable<IntNode> hello;
    hello.disableSnapshotMode();
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    hello.insert("hello", 1);
    EXPECT_EQ(hello.getApproximateDataSize(), 9);
    hello.updateValue("hello", [](IntNode & value) { value = 2; });
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    hello.erase("hello");
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    hello.clear();
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    hello.enableSnapshotMode(10000);
    hello.insert("hello", 1);
    EXPECT_EQ(hello.getApproximateDataSize(), 9);
    hello.updateValue("hello", [](IntNode & value) { value = 2; });
    EXPECT_EQ(hello.getApproximateDataSize(), 18);

    hello.clearOutdatedNodes();
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    hello.erase("hello");
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    hello.clearOutdatedNodes();
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    /// Node
    using Node = DB::KeeperStorage::Node;
    DB::SnapshotableHashTable<Node> world;
    Node n1;
    n1.data = "1234";
    Node n2;
    n2.data = "123456";
    n2.children.insert("");

    world.disableSnapshotMode();
    world.insert("world", n1);
    EXPECT_EQ(world.getApproximateDataSize(), 98);
    world.updateValue("world", [&](Node & value) { value = n2; });
    EXPECT_EQ(world.getApproximateDataSize(), 98);

    world.erase("world");
    EXPECT_EQ(world.getApproximateDataSize(), 0);

    world.enableSnapshotMode(100000);
    world.insert("world", n1);
    EXPECT_EQ(world.getApproximateDataSize(), 98);
    world.updateValue("world", [&](Node & value) { value = n2; });
    EXPECT_EQ(world.getApproximateDataSize(), 196);

    world.clearOutdatedNodes();
    EXPECT_EQ(world.getApproximateDataSize(), 98);

    world.erase("world");
    EXPECT_EQ(world.getApproximateDataSize(), 98);

    world.clear();
    EXPECT_EQ(world.getApproximateDataSize(), 0);
}

void addNode(DB::KeeperStorage & storage, const std::string & path, const std::string & data, int64_t ephemeral_owner=0)
{
    using Node = DB::KeeperStorage::Node;
    Node node{};
    node.data = data;
    node.stat.ephemeralOwner = ephemeral_owner;
    storage.container.insertOrReplace(path, node);
}

TEST_P(CoordinationTest, TestStorageSnapshotSimple)
{
    auto params = GetParam();
    ChangelogDirTest test("./snapshots");
    DB::KeeperSnapshotManager manager("./snapshots", 3, params.enable_compression);

    DB::KeeperStorage storage(500, "");
    addNode(storage, "/hello", "world", 1);
    addNode(storage, "/hello/somepath", "somedata", 3);
    storage.session_id_counter = 5;
    storage.zxid = 2;
    storage.ephemerals[3] = {"/hello"};
    storage.ephemerals[1] = {"/hello/somepath"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot snapshot(&storage, 2);

    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 2);
    EXPECT_EQ(snapshot.session_id, 7);
    EXPECT_EQ(snapshot.snapshot_container_size, 3);
    EXPECT_EQ(snapshot.session_and_timeout.size(), 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin" + params.extension));


    auto debuf = manager.deserializeSnapshotBufferFromDisk(2);

    auto [restored_storage, snapshot_meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

    EXPECT_EQ(restored_storage->container.size(), 3);
    EXPECT_EQ(restored_storage->container.getValue("/").children.size(), 1);
    EXPECT_EQ(restored_storage->container.getValue("/hello").children.size(), 1);
    EXPECT_EQ(restored_storage->container.getValue("/hello/somepath").children.size(), 0);

    EXPECT_EQ(restored_storage->container.getValue("/").data, "");
    EXPECT_EQ(restored_storage->container.getValue("/hello").data, "world");
    EXPECT_EQ(restored_storage->container.getValue("/hello/somepath").data, "somedata");
    EXPECT_EQ(restored_storage->session_id_counter, 7);
    EXPECT_EQ(restored_storage->zxid, 2);
    EXPECT_EQ(restored_storage->ephemerals.size(), 2);
    EXPECT_EQ(restored_storage->ephemerals[3].size(), 1);
    EXPECT_EQ(restored_storage->ephemerals[1].size(), 1);
    EXPECT_EQ(restored_storage->session_and_timeout.size(), 2);
}

TEST_P(CoordinationTest, TestStorageSnapshotMoreWrites)
{
    auto params = GetParam();
    ChangelogDirTest test("./snapshots");
    DB::KeeperSnapshotManager manager("./snapshots", 3, params.enable_compression);

    DB::KeeperStorage storage(500, "");
    storage.getSessionID(130);

    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    DB::KeeperStorageSnapshot snapshot(&storage, 50);
    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 50);
    EXPECT_EQ(snapshot.snapshot_container_size, 51);

    for (size_t i = 50; i < 100; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    EXPECT_EQ(storage.container.size(), 101);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 50);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + params.extension));


    auto debuf = manager.deserializeSnapshotBufferFromDisk(50);
    auto [restored_storage, meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

    EXPECT_EQ(restored_storage->container.size(), 51);
    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_EQ(restored_storage->container.getValue("/hello_" + std::to_string(i)).data, "world_" + std::to_string(i));
    }
}


TEST_P(CoordinationTest, TestStorageSnapshotManySnapshots)
{
    auto params = GetParam();
    ChangelogDirTest test("./snapshots");
    DB::KeeperSnapshotManager manager("./snapshots", 3, params.enable_compression);

    DB::KeeperStorage storage(500, "");
    storage.getSessionID(130);

    for (size_t j = 1; j <= 5; ++j)
    {
        for (size_t i = (j - 1) * 50; i < j * 50; ++i)
        {
            addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
        }

        DB::KeeperStorageSnapshot snapshot(&storage, j * 50);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, j * 50);
        EXPECT_TRUE(fs::exists(std::string{"./snapshots/snapshot_"} + std::to_string(j * 50) + ".bin" + params.extension));
    }

    EXPECT_FALSE(fs::exists("./snapshots/snapshot_50.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_100.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_150.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_200.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_250.bin" + params.extension));


    auto [restored_storage, meta, _] = manager.restoreFromLatestSnapshot();

    EXPECT_EQ(restored_storage->container.size(), 251);

    for (size_t i = 0; i < 250; ++i)
    {
        EXPECT_EQ(restored_storage->container.getValue("/hello_" + std::to_string(i)).data, "world_" + std::to_string(i));
    }
}

TEST_P(CoordinationTest, TestStorageSnapshotMode)
{
    auto params = GetParam();
    ChangelogDirTest test("./snapshots");
    DB::KeeperSnapshotManager manager("./snapshots", 3, params.enable_compression);
    DB::KeeperStorage storage(500, "");
    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    {
        DB::KeeperStorageSnapshot snapshot(&storage, 50);
        for (size_t i = 0; i < 50; ++i)
        {
            addNode(storage, "/hello_" + std::to_string(i), "wlrd_" + std::to_string(i));
        }
        for (size_t i = 0; i < 50; ++i)
        {
            EXPECT_EQ(storage.container.getValue("/hello_" + std::to_string(i)).data, "wlrd_" + std::to_string(i));
        }
        for (size_t i = 0; i < 50; ++i)
        {
            if (i % 2 == 0)
                storage.container.erase("/hello_" + std::to_string(i));
        }
        EXPECT_EQ(storage.container.size(), 26);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 101);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().second, 1);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + params.extension));
    EXPECT_EQ(storage.container.size(), 26);
    storage.clearGarbageAfterSnapshot();
    EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 26);
    for (size_t i = 0; i < 50; ++i)
    {
        if (i % 2 != 0)
            EXPECT_EQ(storage.container.getValue("/hello_" + std::to_string(i)).data, "wlrd_" + std::to_string(i));
        else
            EXPECT_FALSE(storage.container.contains("/hello_" + std::to_string(i)));
    }

    auto [restored_storage, meta, _] = manager.restoreFromLatestSnapshot();

    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_EQ(restored_storage->container.getValue("/hello_" + std::to_string(i)).data, "world_" + std::to_string(i));
    }

}

TEST_P(CoordinationTest, TestStorageSnapshotBroken)
{
    auto params = GetParam();
    ChangelogDirTest test("./snapshots");
    DB::KeeperSnapshotManager manager("./snapshots", 3, params.enable_compression);
    DB::KeeperStorage storage(500, "");
    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }
    {
        DB::KeeperStorageSnapshot snapshot(&storage, 50);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + params.extension));

    /// Let's corrupt file
    DB::WriteBufferFromFile plain_buf("./snapshots/snapshot_50.bin" + params.extension, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(34);
    plain_buf.sync();

    EXPECT_THROW(manager.restoreFromLatestSnapshot(), DB::Exception);
}

nuraft::ptr<nuraft::buffer> getBufferFromZKRequest(int64_t session_id, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::WriteBufferFromNuraftBuffer buf;
    DB::writeIntBinary(session_id, buf);
    request->write(buf);
    using namespace std::chrono;
    auto time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    DB::writeIntBinary(time, buf);
    return buf.getBuffer();
}

nuraft::ptr<nuraft::log_entry> getLogEntryFromZKRequest(size_t term, int64_t session_id, const Coordination::ZooKeeperRequestPtr & request)
{
    auto buffer = getBufferFromZKRequest(session_id, request);
    return nuraft::cs_new<nuraft::log_entry>(term, buffer);
}

void testLogAndStateMachine(Coordination::CoordinationSettingsPtr settings, uint64_t total_logs, bool enable_compression)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest logs("./logs");

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<KeeperStateMachine>(queue, snapshots_queue, "./snapshots", settings);
    state_machine->init();
    DB::KeeperLogStore changelog("./logs", settings->rotate_log_storage_interval, true, enable_compression);
    changelog.init(state_machine->last_commit_index() + 1, settings->reserved_log_items);
    for (size_t i = 1; i < total_logs + 1; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);

        state_machine->commit(i, changelog.entry_at(i)->get_buf());
        bool snapshot_created = false;
        if (i % settings->snapshot_distance == 0)
        {
            nuraft::snapshot s(i, 0, std::make_shared<nuraft::cluster_config>());
            nuraft::async_result<bool>::handler_type when_done = [&snapshot_created] (bool & ret, nuraft::ptr<std::exception> &/*exception*/)
            {
                snapshot_created = ret;
                std::cerr << "Snapshot finised\n";
            };

            state_machine->create_snapshot(s, when_done);
            CreateSnapshotTask snapshot_task;
            bool pop_result = snapshots_queue.pop(snapshot_task);
            EXPECT_TRUE(pop_result);

            snapshot_task.create_snapshot(std::move(snapshot_task.snapshot));
        }
        if (snapshot_created)
        {
            if (changelog.size() > settings->reserved_log_items)
            {
                changelog.compact(i - settings->reserved_log_items);
            }
        }
    }

    SnapshotsQueue snapshots_queue1{1};
    auto restore_machine = std::make_shared<KeeperStateMachine>(queue, snapshots_queue1, "./snapshots", settings);
    restore_machine->init();
    EXPECT_EQ(restore_machine->last_commit_index(), total_logs - total_logs % settings->snapshot_distance);

    DB::KeeperLogStore restore_changelog("./logs", settings->rotate_log_storage_interval, true, enable_compression);
    restore_changelog.init(restore_machine->last_commit_index() + 1, settings->reserved_log_items);

    EXPECT_EQ(restore_changelog.size(), std::min(settings->reserved_log_items + total_logs % settings->snapshot_distance, total_logs));
    EXPECT_EQ(restore_changelog.next_slot(), total_logs + 1);
    if (total_logs > settings->reserved_log_items + 1)
        EXPECT_EQ(restore_changelog.start_index(), total_logs - total_logs % settings->snapshot_distance - settings->reserved_log_items + 1);
    else
        EXPECT_EQ(restore_changelog.start_index(), 1);

    for (size_t i = restore_machine->last_commit_index() + 1; i < restore_changelog.next_slot(); ++i)
    {
        restore_machine->commit(i, changelog.entry_at(i)->get_buf());
    }

    auto & source_storage = state_machine->getStorage();
    auto & restored_storage = restore_machine->getStorage();

    EXPECT_EQ(source_storage.container.size(), restored_storage.container.size());
    for (size_t i = 1; i < total_logs + 1; ++i)
    {
        auto path = "/hello_" + std::to_string(i);
        EXPECT_EQ(source_storage.container.getValue(path).data, restored_storage.container.getValue(path).data);
    }
}

TEST_P(CoordinationTest, TestStateMachineAndLogStore)
{
    using namespace Coordination;
    using namespace DB;
    auto params = GetParam();

    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine(settings, 37, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine(settings, 11, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine(settings, 40, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 20;
        settings->rotate_log_storage_interval = 30;
        testLogAndStateMachine(settings, 40, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 0;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine(settings, 40, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 1;
        settings->reserved_log_items = 1;
        settings->rotate_log_storage_interval = 32;
        testLogAndStateMachine(settings, 32, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 7;
        settings->rotate_log_storage_interval = 1;
        testLogAndStateMachine(settings, 33, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 37;
        settings->reserved_log_items = 1000;
        settings->rotate_log_storage_interval = 5000;
        testLogAndStateMachine(settings, 33, params.enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 37;
        settings->reserved_log_items = 1000;
        settings->rotate_log_storage_interval = 5000;
        testLogAndStateMachine(settings, 45, params.enable_compression);
    }
}

TEST_P(CoordinationTest, TestEphemeralNodeRemove)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<KeeperStateMachine>(queue, snapshots_queue, "./snapshots", settings);
    state_machine->init();

    std::shared_ptr<ZooKeeperCreateRequest> request_c = std::make_shared<ZooKeeperCreateRequest>();
    request_c->path = "/hello";
    request_c->is_ephemeral = true;
    auto entry_c = getLogEntryFromZKRequest(0, 1, request_c);
    state_machine->commit(1, entry_c->get_buf());
    const auto & storage = state_machine->getStorage();

    EXPECT_EQ(storage.ephemerals.size(), 1);
    std::shared_ptr<ZooKeeperRemoveRequest> request_d = std::make_shared<ZooKeeperRemoveRequest>();
    request_d->path = "/hello";
    /// Delete from other session
    auto entry_d = getLogEntryFromZKRequest(0, 2, request_d);
    state_machine->commit(2, entry_d->get_buf());

    EXPECT_EQ(storage.ephemerals.size(), 0);
}


TEST_P(CoordinationTest, TestRotateIntervalChanges)
{
    using namespace Coordination;
    auto params = GetParam();
    ChangelogDirTest snapshots("./logs");
    {
        DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);

        changelog.init(0, 3);
        for (size_t i = 1; i < 55; ++i)
        {
            std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
            request->path = "/hello_" + std::to_string(i);
            auto entry = getLogEntryFromZKRequest(0, 1, request);
            changelog.append(entry);
            changelog.end_of_append_batch(0, 0);
        }
    }


    EXPECT_TRUE(fs::exists("./logs/changelog_1_100.bin" + params.extension));

    DB::KeeperLogStore changelog_1("./logs", 10, true, params.enable_compression);
    changelog_1.init(0, 50);
    for (size_t i = 0; i < 55; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(100 + i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog_1.append(entry);
        changelog_1.end_of_append_batch(0, 0);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_100.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_101_110.bin" + params.extension));

    DB::KeeperLogStore changelog_2("./logs", 7, true, params.enable_compression);
    changelog_2.init(98, 55);

    for (size_t i = 0; i < 17; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(200 + i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog_2.append(entry);
        changelog_2.end_of_append_batch(0, 0);
    }

    changelog_2.compact(105);
    std::this_thread::sleep_for(std::chrono::microseconds(200));

    EXPECT_FALSE(fs::exists("./logs/changelog_1_100.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_101_110.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_111_117.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_118_124.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_125_131.bin" + params.extension));

    DB::KeeperLogStore changelog_3("./logs", 5, true, params.enable_compression);
    changelog_3.init(116, 3);
    for (size_t i = 0; i < 17; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(300 + i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog_3.append(entry);
        changelog_3.end_of_append_batch(0, 0);
    }

    changelog_3.compact(125);
    std::this_thread::sleep_for(std::chrono::microseconds(200));
    EXPECT_FALSE(fs::exists("./logs/changelog_101_110.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_111_117.bin" + params.extension));
    EXPECT_FALSE(fs::exists("./logs/changelog_118_124.bin" + params.extension));

    EXPECT_TRUE(fs::exists("./logs/changelog_125_131.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_132_136.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_137_141.bin" + params.extension));
    EXPECT_TRUE(fs::exists("./logs/changelog_142_146.bin" + params.extension));
}

TEST_P(CoordinationTest, TestSessionExpiryQueue)
{
    using namespace Coordination;
    SessionExpiryQueue queue(500);

    queue.addNewSessionOrUpdate(1, 1000);

    for (size_t i = 0; i < 2; ++i)
    {
        EXPECT_EQ(queue.getExpiredSessions(), std::vector<int64_t>({}));
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    EXPECT_EQ(queue.getExpiredSessions(), std::vector<int64_t>({1}));
}


TEST_P(CoordinationTest, TestCompressedLogsMultipleRewrite)
{
    using namespace Coordination;
    auto test_params = GetParam();
    ChangelogDirTest snapshots("./logs");
    DB::KeeperLogStore changelog("./logs", 100, true, test_params.enable_compression);

    changelog.init(0, 3);
    for (size_t i = 1; i < 55; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
    }


    DB::KeeperLogStore changelog1("./logs", 100, true, test_params.enable_compression);
    changelog1.init(0, 3);
    for (size_t i = 55; i < 70; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog1.append(entry);
        changelog1.end_of_append_batch(0, 0);
    }

    DB::KeeperLogStore changelog2("./logs", 100, true, test_params.enable_compression);
    changelog2.init(0, 3);
    for (size_t i = 70; i < 80; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog2.append(entry);
        changelog2.end_of_append_batch(0, 0);
    }
}

TEST_P(CoordinationTest, TestStorageSnapshotDifferentCompressions)
{
    auto params = GetParam();

    ChangelogDirTest test("./snapshots");
    DB::KeeperSnapshotManager manager("./snapshots", 3, params.enable_compression);

    DB::KeeperStorage storage(500, "");
    addNode(storage, "/hello", "world", 1);
    addNode(storage, "/hello/somepath", "somedata", 3);
    storage.session_id_counter = 5;
    storage.zxid = 2;
    storage.ephemerals[3] = {"/hello"};
    storage.ephemerals[1] = {"/hello/somepath"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot snapshot(&storage, 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin" + params.extension));

    DB::KeeperSnapshotManager new_manager("./snapshots", 3, !params.enable_compression);

    auto debuf = new_manager.deserializeSnapshotBufferFromDisk(2);

    auto [restored_storage, snapshot_meta, _] = new_manager.deserializeSnapshotFromBuffer(debuf);

    EXPECT_EQ(restored_storage->container.size(), 3);
    EXPECT_EQ(restored_storage->container.getValue("/").children.size(), 1);
    EXPECT_EQ(restored_storage->container.getValue("/hello").children.size(), 1);
    EXPECT_EQ(restored_storage->container.getValue("/hello/somepath").children.size(), 0);

    EXPECT_EQ(restored_storage->container.getValue("/").data, "");
    EXPECT_EQ(restored_storage->container.getValue("/hello").data, "world");
    EXPECT_EQ(restored_storage->container.getValue("/hello/somepath").data, "somedata");
    EXPECT_EQ(restored_storage->session_id_counter, 7);
    EXPECT_EQ(restored_storage->zxid, 2);
    EXPECT_EQ(restored_storage->ephemerals.size(), 2);
    EXPECT_EQ(restored_storage->ephemerals[3].size(), 1);
    EXPECT_EQ(restored_storage->ephemerals[1].size(), 1);
    EXPECT_EQ(restored_storage->session_and_timeout.size(), 2);
}

TEST_P(CoordinationTest, ChangelogInsertThreeTimesSmooth)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    {
        std::cerr << "================First time=====================\n";
        DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
        changelog.init(1, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 2);
    }

    {
        std::cerr << "================Second time=====================\n";
        DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
        changelog.init(1, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 3);
    }

    {
        std::cerr << "================Third time=====================\n";
        DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
        changelog.init(1, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 4);
    }

    {
        std::cerr << "================Fourth time=====================\n";
        DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
        changelog.init(1, 0);
        auto entry = getLogEntry("hello_world", 1000);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
        EXPECT_EQ(changelog.next_slot(), 5);
    }
}


TEST_P(CoordinationTest, ChangelogInsertMultipleTimesSmooth)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    for (size_t i = 0; i < 36; ++i)
    {
        std::cerr << "================First time=====================\n";
        DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
        changelog.init(1, 0);
        for (size_t j = 0; j < 7; ++j)
        {
            auto entry = getLogEntry("hello_world", 7);
            changelog.append(entry);
        }
        changelog.end_of_append_batch(0, 0);
    }

    DB::KeeperLogStore changelog("./logs", 100, true, params.enable_compression);
    changelog.init(1, 0);
    EXPECT_EQ(changelog.next_slot(), 36 * 7 + 1);
}

TEST_P(CoordinationTest, ChangelogInsertThreeTimesHard)
{
    auto params = GetParam();
    ChangelogDirTest test("./logs");
    std::cerr << "================First time=====================\n";
    DB::KeeperLogStore changelog1("./logs", 100, true, params.enable_compression);
    changelog1.init(1, 0);
    auto entry = getLogEntry("hello_world", 1000);
    changelog1.append(entry);
    changelog1.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog1.next_slot(), 2);

    std::cerr << "================Second time=====================\n";
    DB::KeeperLogStore changelog2("./logs", 100, true, params.enable_compression);
    changelog2.init(1, 0);
    entry = getLogEntry("hello_world", 1000);
    changelog2.append(entry);
    changelog2.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog2.next_slot(), 3);

    std::cerr << "================Third time=====================\n";
    DB::KeeperLogStore changelog3("./logs", 100, true, params.enable_compression);
    changelog3.init(1, 0);
    entry = getLogEntry("hello_world", 1000);
    changelog3.append(entry);
    changelog3.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog3.next_slot(), 4);

    std::cerr << "================Fourth time=====================\n";
    DB::KeeperLogStore changelog4("./logs", 100, true, params.enable_compression);
    changelog4.init(1, 0);
    entry = getLogEntry("hello_world", 1000);
    changelog4.append(entry);
    changelog4.end_of_append_batch(0, 0);
    EXPECT_EQ(changelog4.next_slot(), 5);
}


TEST_P(CoordinationTest, TestLogGap)
{
    using namespace Coordination;
    auto test_params = GetParam();
    ChangelogDirTest logs("./logs");
    DB::KeeperLogStore changelog("./logs", 100, true, test_params.enable_compression);

    changelog.init(0, 3);
    for (size_t i = 1; i < 55; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, request);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);
    }

    DB::KeeperLogStore changelog1("./logs", 100, true, test_params.enable_compression);
    changelog1.init(61, 3);

    /// Logs discarded
    EXPECT_FALSE(fs::exists("./logs/changelog_1_100.bin" + test_params.extension));
    EXPECT_EQ(changelog1.start_index(), 61);
    EXPECT_EQ(changelog1.next_slot(), 61);
}


INSTANTIATE_TEST_SUITE_P(CoordinationTestSuite,
    CoordinationTest,
    ::testing::ValuesIn(std::initializer_list<CompressionParam>{
            CompressionParam{true, ".zstd"},
            CompressionParam{false, ""}
    })
);

int main(int argc, char ** argv)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#endif
