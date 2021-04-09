#include <gtest/gtest.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/NuKeeperStateManager.h>
#include <Coordination/NuKeeperStorageSerializer.h>
#include <Coordination/SummingStateMachine.h>
#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <thread>
#include <Coordination/NuKeeperLogStore.h>
#include <Coordination/Changelog.h>
#include <filesystem>

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

TEST(CoordinationTest, BuildTest)
{
    DB::InMemoryLogStore store;
    DB::SummingStateMachine machine;
    EXPECT_EQ(1, 1);
}

TEST(CoordinationTest, BufferSerde)
{
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Get);
    request->xid = 3;
    dynamic_cast<Coordination::ZooKeeperGetRequest *>(request.get())->path = "/path/value";

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
    EXPECT_EQ(dynamic_cast<Coordination::ZooKeeperGetRequest *>(request_read.get())->path, "/path/value");
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
        , state_manager(nuraft::cs_new<DB::NuKeeperStateManager>(server_id, hostname, port, logs_path))
    {
        state_manager->loadLogStore(1);
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
    nuraft::ptr<DB::NuKeeperStateManager> state_manager;

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


TEST(CoordinationTest, TestSummingRaft1)
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

nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::WriteBufferFromNuraftBuffer buf;
    DB::writeIntBinary(session_id, buf);
    request->write(buf);
    return buf.getBuffer();
}

DB::NuKeeperStorage::ResponsesForSessions getZooKeeperResponses(nuraft::ptr<nuraft::buffer> & buffer, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::NuKeeperStorage::ResponsesForSessions results;
    DB::ReadBufferFromNuraftBuffer buf(buffer);
    while (!buf.eof())
    {
        int64_t session_id;
        DB::readIntBinary(session_id, buf);

        int32_t length;
        Coordination::XID xid;
        int64_t zxid;
        Coordination::Error err;

        Coordination::read(length, buf);
        Coordination::read(xid, buf);
        Coordination::read(zxid, buf);
        Coordination::read(err, buf);
        auto response = request->makeResponse();
        response->readImpl(buf);
        results.push_back(DB::NuKeeperStorage::ResponseForSession{session_id, response});
    }
    return results;
}

TEST(CoordinationTest, TestStorageSerialization)
{
    DB::NuKeeperStorage storage(500);
    storage.container["/hello"] = DB::NuKeeperStorage::Node{.data="world"};
    storage.container["/hello/somepath"] =  DB::NuKeeperStorage::Node{.data="somedata"};
    storage.session_id_counter = 5;
    storage.zxid = 156;
    storage.ephemerals[3] = {"/hello", "/"};
    storage.ephemerals[1] = {"/hello/somepath"};

    DB::WriteBufferFromOwnString buffer;
    DB::NuKeeperStorageSerializer serializer;
    serializer.serialize(storage, buffer);
    std::string serialized = buffer.str();
    EXPECT_NE(serialized.size(), 0);
    DB::ReadBufferFromString read(serialized);
    DB::NuKeeperStorage new_storage(500);
    serializer.deserialize(new_storage, read);

    EXPECT_EQ(new_storage.container.size(), 3);
    EXPECT_EQ(new_storage.container["/hello"].data, "world");
    EXPECT_EQ(new_storage.container["/hello/somepath"].data, "somedata");
    EXPECT_EQ(new_storage.session_id_counter, 5);
    EXPECT_EQ(new_storage.zxid, 156);
    EXPECT_EQ(new_storage.ephemerals.size(), 2);
    EXPECT_EQ(new_storage.ephemerals[3].size(), 2);
    EXPECT_EQ(new_storage.ephemerals[1].size(), 1);
}

DB::LogEntryPtr getLogEntry(const std::string & s, size_t term)
{
    DB::WriteBufferFromNuraftBuffer bufwriter;
    writeText(s, bufwriter);
    return nuraft::cs_new<nuraft::log_entry>(term, bufwriter.getBuffer());
}

TEST(CoordinationTest, ChangelogTestSimple)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);
    auto entry = getLogEntry("hello world", 77);
    changelog.append(entry);
    EXPECT_EQ(changelog.next_slot(), 2);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.last_entry()->get_term(), 77);
    EXPECT_EQ(changelog.entry_at(1)->get_term(), 77);
    EXPECT_EQ(changelog.log_entries(1, 2)->size(), 1);
}

TEST(CoordinationTest, ChangelogTestFile)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);
    auto entry = getLogEntry("hello world", 77);
    changelog.append(entry);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    for (const auto & p : fs::directory_iterator("./logs"))
        EXPECT_EQ(p.path(), "./logs/changelog_1_5.bin");

    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);
    changelog.append(entry);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
}

TEST(CoordinationTest, ChangelogReadWrite)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 1000, true);
    changelog.init(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    EXPECT_EQ(changelog.size(), 10);
    DB::NuKeeperLogStore changelog_reader("./logs", 1000, true);
    changelog_reader.init(1);
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

TEST(CoordinationTest, ChangelogWriteAt)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 1000, true);
    changelog.init(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }
    EXPECT_EQ(changelog.size(), 10);

    auto entry = getLogEntry("writer", 77);
    changelog.write_at(7, entry);
    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.last_entry()->get_term(), 77);
    EXPECT_EQ(changelog.entry_at(7)->get_term(), 77);
    EXPECT_EQ(changelog.next_slot(), 8);

    DB::NuKeeperLogStore changelog_reader("./logs", 1000, true);
    changelog_reader.init(1);

    EXPECT_EQ(changelog_reader.size(), changelog.size());
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), changelog.last_entry()->get_term());
    EXPECT_EQ(changelog_reader.start_index(), changelog.start_index());
    EXPECT_EQ(changelog_reader.next_slot(), changelog.next_slot());
}


TEST(CoordinationTest, ChangelogTestAppendAfterRead)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);
    for (size_t i = 0; i < 7; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }

    EXPECT_EQ(changelog.size(), 7);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    DB::NuKeeperLogStore changelog_reader("./logs", 5, true);
    changelog_reader.init(1);

    EXPECT_EQ(changelog_reader.size(), 7);
    for (size_t i = 7; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog_reader.append(entry);
    }
    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    size_t logs_count = 0;
    for (const auto & _ [[maybe_unused]]: fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 2);

    auto entry = getLogEntry("someentry", 77);
    changelog_reader.append(entry);
    EXPECT_EQ(changelog_reader.size(), 11);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));

    logs_count = 0;
    for (const auto & _ [[maybe_unused]]: fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 3);
}

TEST(CoordinationTest, ChangelogTestCompaction)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);

    for (size_t i = 0; i < 3; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.append(entry);
    }

    EXPECT_EQ(changelog.size(), 3);

    changelog.compact(2);

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 3);
    EXPECT_EQ(changelog.next_slot(), 4);
    EXPECT_EQ(changelog.last_entry()->get_term(), 20);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));

    auto e1 = getLogEntry("hello world", 30);
    changelog.append(e1);
    auto e2 = getLogEntry("hello world", 40);
    changelog.append(e2);
    auto e3 = getLogEntry("hello world", 50);
    changelog.append(e3);
    auto e4 = getLogEntry("hello world", 60);
    changelog.append(e4);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    changelog.compact(6);

    EXPECT_FALSE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 7);
    EXPECT_EQ(changelog.next_slot(), 8);
    EXPECT_EQ(changelog.last_entry()->get_term(), 60);
    /// And we able to read it
    DB::NuKeeperLogStore changelog_reader("./logs", 5, true);
    changelog_reader.init(7);
    EXPECT_EQ(changelog_reader.size(), 1);
    EXPECT_EQ(changelog_reader.start_index(), 7);
    EXPECT_EQ(changelog_reader.next_slot(), 8);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 60);
}

TEST(CoordinationTest, ChangelogTestBatchOperations)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 100, true);
    changelog.init(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }

    EXPECT_EQ(changelog.size(), 10);

    auto entries = changelog.pack(1, 5);

    DB::NuKeeperLogStore apply_changelog("./logs", 100, true);
    apply_changelog.init(1);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(apply_changelog.entry_at(i + 1)->get_term(), i * 10);
    }
    EXPECT_EQ(apply_changelog.size(), 10);

    apply_changelog.apply_pack(8, *entries);

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

TEST(CoordinationTest, ChangelogTestBatchOperationsEmpty)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 100, true);
    changelog.init(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }

    EXPECT_EQ(changelog.size(), 10);

    auto entries = changelog.pack(5, 5);

    ChangelogDirTest test1("./logs1");
    DB::NuKeeperLogStore changelog_new("./logs1", 100, true);
    changelog_new.init(1);
    EXPECT_EQ(changelog_new.size(), 0);

    changelog_new.apply_pack(5, *entries);

    EXPECT_EQ(changelog_new.size(), 5);
    EXPECT_EQ(changelog_new.start_index(), 5);
    EXPECT_EQ(changelog_new.next_slot(), 10);

    for (size_t i = 4; i < 9; ++i)
        EXPECT_EQ(changelog_new.entry_at(i + 1)->get_term(), i * 10);

    auto e = getLogEntry("hello_world", 110);
    changelog_new.append(e);
    EXPECT_EQ(changelog_new.size(), 6);
    EXPECT_EQ(changelog_new.start_index(), 5);
    EXPECT_EQ(changelog_new.next_slot(), 11);

    DB::NuKeeperLogStore changelog_reader("./logs1", 100, true);
    changelog_reader.init(5);
}


TEST(CoordinationTest, ChangelogTestWriteAtPreviousFile)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(7, e1);
    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 8);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    EXPECT_FALSE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin"));

    DB::NuKeeperLogStore changelog_read("./logs", 5, true);
    changelog_read.init(1);
    EXPECT_EQ(changelog_read.size(), 7);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 8);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TEST(CoordinationTest, ChangelogTestWriteAtFileBorder)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(11, e1);
    EXPECT_EQ(changelog.size(), 11);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 12);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin"));

    DB::NuKeeperLogStore changelog_read("./logs", 5, true);
    changelog_read.init(1);
    EXPECT_EQ(changelog_read.size(), 11);
    EXPECT_EQ(changelog_read.start_index(), 1);
    EXPECT_EQ(changelog_read.next_slot(), 12);
    EXPECT_EQ(changelog_read.last_entry()->get_term(), 5555);
}

TEST(CoordinationTest, ChangelogTestWriteAtAllFiles)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));

    EXPECT_EQ(changelog.size(), 33);

    auto e1 = getLogEntry("helloworld", 5555);
    changelog.write_at(1, e1);
    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.start_index(), 1);
    EXPECT_EQ(changelog.next_slot(), 2);
    EXPECT_EQ(changelog.last_entry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));

    EXPECT_FALSE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin"));
}

TEST(CoordinationTest, ChangelogTestStartNewLogAfterRead)
{
    ChangelogDirTest test("./logs");
    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    EXPECT_EQ(changelog.size(), 35);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_36_40.bin"));


    DB::NuKeeperLogStore changelog_reader("./logs", 5, true);
    changelog_reader.init(1);

    auto entry = getLogEntry("36_hello_world", 360);
    changelog_reader.append(entry);

    EXPECT_EQ(changelog_reader.size(), 36);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_36_40.bin"));
}


TEST(CoordinationTest, ChangelogTestReadAfterBrokenTruncate)
{
    ChangelogDirTest test("./logs");

    DB::NuKeeperLogStore changelog("./logs", 5, true);
    changelog.init(1);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.append(entry);
    }
    EXPECT_EQ(changelog.size(), 35);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));

    DB::WriteBufferFromFile plain_buf("./logs/changelog_11_15.bin", DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(0);

    DB::NuKeeperLogStore changelog_reader("./logs", 5, true);
    changelog_reader.init(1);

    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 90);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin"));

    auto entry = getLogEntry("h", 7777);
    changelog_reader.append(entry);
    EXPECT_EQ(changelog_reader.size(), 11);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 7777);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin"));

    DB::NuKeeperLogStore changelog_reader2("./logs", 5, true);
    changelog_reader2.init(1);
    EXPECT_EQ(changelog_reader2.size(), 11);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

TEST(CoordinationTest, ChangelogTestReadAfterBrokenTruncate2)
{
    ChangelogDirTest test("./logs");

    DB::NuKeeperLogStore changelog("./logs", 20, true);
    changelog.init(1);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_40.bin"));

    DB::WriteBufferFromFile plain_buf("./logs/changelog_1_20.bin", DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(140);

    DB::NuKeeperLogStore changelog_reader("./logs", 20, true);
    changelog_reader.init(1);

    EXPECT_EQ(changelog_reader.size(), 2);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 450);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_40.bin"));
    auto entry = getLogEntry("hello_world", 7777);
    changelog_reader.append(entry);
    EXPECT_EQ(changelog_reader.size(), 3);
    EXPECT_EQ(changelog_reader.last_entry()->get_term(), 7777);


    DB::NuKeeperLogStore changelog_reader2("./logs", 20, true);
    changelog_reader2.init(1);
    EXPECT_EQ(changelog_reader2.size(), 3);
    EXPECT_EQ(changelog_reader2.last_entry()->get_term(), 7777);
}

TEST(CoordinationTest, ChangelogTestLostFiles)
{
    ChangelogDirTest test("./logs");

    DB::NuKeeperLogStore changelog("./logs", 20, true);
    changelog.init(1);

    for (size_t i = 0; i < 35; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", (i + 44) * 10);
        changelog.append(entry);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_40.bin"));

    fs::remove("./logs/changelog_1_20.bin");

    DB::NuKeeperLogStore changelog_reader("./logs", 20, true);
    EXPECT_THROW(changelog_reader.init(5), DB::Exception);

    fs::remove("./logs/changelog_21_40.bin");
    EXPECT_THROW(changelog_reader.init(3), DB::Exception);
}

int main(int argc, char ** argv)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#endif
