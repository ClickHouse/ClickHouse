#include <gtest/gtest.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Coordination/InMemoryLogStore.h>
#include <Coordination/InMemoryStateManager.h>
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
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <thread>
#include <Coordination/NuKeeperLogStore.h>
#include <Coordination/Changelog.h>
#include <filesystem>


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
    SimpliestRaftServer(int server_id_, const std::string & hostname_, int port_)
        : server_id(server_id_)
        , hostname(hostname_)
        , port(port_)
        , endpoint(hostname + ":" + std::to_string(port))
        , state_machine(nuraft::cs_new<StateMachine>())
        , state_manager(nuraft::cs_new<DB::InMemoryStateManager>(server_id, hostname, port))
    {
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
            std::cerr << "Failed to initialize launcher (see the message "
                         "in the log file)." << std::endl;
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
    nuraft::ptr<nuraft::state_mgr> state_manager;

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
    // WARNING: We don't consider endian-safety in this example.
    bs.put_raw(&number, sizeof(number));
    return ret;
}


TEST(CoordinationTest, TestSummingRaft1)
{
    SummingRaftServer s1(1, "localhost", 44444);

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

TEST(CoordinationTest, TestSummingRaft3)
{
    SummingRaftServer s1(1, "localhost", 44444);
    SummingRaftServer s2(2, "localhost", 44445);
    SummingRaftServer s3(3, "localhost", 44446);

    nuraft::srv_config first_config(1, "localhost:44444");
    auto ret1 = s2.raft_instance->add_srv(first_config);
    if (!ret1->get_accepted())
    {
        std::cout << "failed to add server: "
                  << ret1->get_result_str() << std::endl;
        EXPECT_TRUE(false);
    }

    while (s1.raft_instance->get_leader() != 2)
    {
        std::cout << "Waiting s1 to join to s2 quorum\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    nuraft::srv_config third_config(3, "localhost:44446");
    auto ret3 = s2.raft_instance->add_srv(third_config);
    if (!ret3->get_accepted())
    {
        std::cout << "failed to add server: "
                  << ret3->get_result_str() << std::endl;
        EXPECT_TRUE(false);
    }

    while (s3.raft_instance->get_leader() != 2)
    {
        std::cout << "Waiting s3 to join to s2 quorum\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    /// S2 is leader
    EXPECT_EQ(s1.raft_instance->get_leader(), 2);
    EXPECT_EQ(s2.raft_instance->get_leader(), 2);
    EXPECT_EQ(s3.raft_instance->get_leader(), 2);

    std::cerr << "Starting to add entries\n";
    auto entry = getBuffer(1);
    auto ret = s2.raft_instance->append_entries({entry});
    EXPECT_TRUE(ret->get_accepted()) << "failed to replicate: entry 1" << ret->get_result_code();
    EXPECT_EQ(ret->get_result_code(), nuraft::cmd_result_code::OK) << "failed to replicate: entry 1" << ret->get_result_code();

    while (s1.state_machine->getValue() != 1)
    {
        std::cout << "Waiting s1 to apply entry\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    while (s2.state_machine->getValue() != 1)
    {
        std::cout << "Waiting s2 to apply entry\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    while (s3.state_machine->getValue() != 1)
    {
        std::cout << "Waiting s3 to apply entry\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(s1.state_machine->getValue(), 1);
    EXPECT_EQ(s2.state_machine->getValue(), 1);
    EXPECT_EQ(s3.state_machine->getValue(), 1);

    auto non_leader_entry = getBuffer(3);
    auto ret_non_leader1 = s1.raft_instance->append_entries({non_leader_entry});

    EXPECT_FALSE(ret_non_leader1->get_accepted());

    auto ret_non_leader3 = s3.raft_instance->append_entries({non_leader_entry});

    EXPECT_FALSE(ret_non_leader3->get_accepted());

    auto leader_entry = getBuffer(77);
    auto ret_leader = s2.raft_instance->append_entries({leader_entry});
    EXPECT_TRUE(ret_leader->get_accepted()) << "failed to replicate: entry 78" << ret_leader->get_result_code();
    EXPECT_EQ(ret_leader->get_result_code(), nuraft::cmd_result_code::OK) << "failed to replicate: entry 78" << ret_leader->get_result_code();

    while (s1.state_machine->getValue() != 78)
    {
        std::cout << "Waiting s1 to apply entry\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    while (s3.state_machine->getValue() != 78)
    {
        std::cout << "Waiting s3 to apply entry\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(s1.state_machine->getValue(), 78);
    EXPECT_EQ(s2.state_machine->getValue(), 78);
    EXPECT_EQ(s3.state_machine->getValue(), 78);

    s1.launcher.shutdown(5);
    s2.launcher.shutdown(5);
    s3.launcher.shutdown(5);
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

namespace fs = std::filesystem;
struct ChangelogDirTest
{
    std::string path;
    bool drop;
    ChangelogDirTest(std::string path_, bool drop_ = true)
        : path(path_)
        , drop(drop_)
    {
        if (fs::exists(path))
            EXPECT_TRUE(false) << "Path " << path << " already exists, remove it to run test";
        fs::create_directory(path);
    }

    ~ChangelogDirTest()
    {
        if (fs::exists(path) && drop)
            fs::remove_all(path);
    }
};

TEST(CoordinationTest, ChangelogTestSimple)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 5);
    changelog.readChangelogAndInitWriter(1);
    auto entry = getLogEntry("hello world", 77);
    changelog.appendEntry(1, entry);
    EXPECT_EQ(changelog.getNextEntryIndex(), 2);
    EXPECT_EQ(changelog.getStartIndex(), 1);
    EXPECT_EQ(changelog.getLastEntry()->get_term(), 77);
    EXPECT_EQ(changelog.entryAt(1)->get_term(), 77);
    EXPECT_EQ(changelog.getLogEntriesBetween(1, 2)->size(), 1);
}

TEST(CoordinationTest, ChangelogTestFile)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 5);
    changelog.readChangelogAndInitWriter(1);
    auto entry = getLogEntry("hello world", 77);
    changelog.appendEntry(1, entry);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    for(const auto & p : fs::directory_iterator("./logs"))
        EXPECT_EQ(p.path(), "./logs/changelog_1_5.bin");

    changelog.appendEntry(2, entry);
    changelog.appendEntry(3, entry);
    changelog.appendEntry(4, entry);
    changelog.appendEntry(5, entry);
    changelog.appendEntry(6, entry);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
}

TEST(CoordinationTest, ChangelogReadWrite)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 1000);
    changelog.readChangelogAndInitWriter(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }
    EXPECT_EQ(changelog.size(), 10);
    DB::Changelog changelog_reader("./logs", 1000);
    changelog_reader.readChangelogAndInitWriter(1);
    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_EQ(changelog_reader.getLastEntry()->get_term(), changelog.getLastEntry()->get_term());
    EXPECT_EQ(changelog_reader.getStartIndex(), changelog.getStartIndex());
    EXPECT_EQ(changelog_reader.getNextEntryIndex(), changelog.getNextEntryIndex());

    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ(changelog_reader.entryAt(i + 1)->get_term(), changelog.entryAt(i + 1)->get_term());

    auto entries_from_range_read = changelog_reader.getLogEntriesBetween(1, 11);
    auto entries_from_range = changelog.getLogEntriesBetween(1, 11);
    EXPECT_EQ(entries_from_range_read->size(), entries_from_range->size());
    EXPECT_EQ(10, entries_from_range->size());
}

TEST(CoordinationTest, ChangelogWriteAt)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 1000);
    changelog.readChangelogAndInitWriter(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }
    EXPECT_EQ(changelog.size(), 10);

    auto entry = getLogEntry("writer", 77);
    changelog.writeAt(7, entry);
    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.getLastEntry()->get_term(), 77);
    EXPECT_EQ(changelog.entryAt(7)->get_term(), 77);
    EXPECT_EQ(changelog.getNextEntryIndex(), 8);

    DB::Changelog changelog_reader("./logs", 1000);
    changelog_reader.readChangelogAndInitWriter(1);

    EXPECT_EQ(changelog_reader.size(), changelog.size());
    EXPECT_EQ(changelog_reader.getLastEntry()->get_term(), changelog.getLastEntry()->get_term());
    EXPECT_EQ(changelog_reader.getStartIndex(), changelog.getStartIndex());
    EXPECT_EQ(changelog_reader.getNextEntryIndex(), changelog.getNextEntryIndex());
}


TEST(CoordinationTest, ChangelogTestAppendAfterRead)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 5);
    changelog.readChangelogAndInitWriter(1);
    for (size_t i = 0; i < 7; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }

    EXPECT_EQ(changelog.size(), 7);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    DB::Changelog changelog_reader("./logs", 5);
    changelog_reader.readChangelogAndInitWriter(1);

    EXPECT_EQ(changelog_reader.size(), 7);
    for (size_t i = 7; i < 10; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog_reader.appendEntry(changelog_reader.getNextEntryIndex(), entry);
    }
    EXPECT_EQ(changelog_reader.size(), 10);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    size_t logs_count = 0;
    for(const auto & _ [[maybe_unused]]: fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 2);

    auto entry = getLogEntry("someentry", 77);
    changelog_reader.appendEntry(changelog_reader.getNextEntryIndex(), entry);
    EXPECT_EQ(changelog_reader.size(), 11);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));

    logs_count = 0;
    for(const auto & _ [[maybe_unused]]: fs::directory_iterator("./logs"))
        logs_count++;

    EXPECT_EQ(logs_count, 3);
}

TEST(CoordinationTest, ChangelogTestCompaction)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 5);
    changelog.readChangelogAndInitWriter(1);

    for (size_t i = 0; i < 3; ++i)
    {
        auto entry = getLogEntry("hello world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }

    EXPECT_EQ(changelog.size(), 3);

    changelog.compact(2);

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.getStartIndex(), 3);
    EXPECT_EQ(changelog.getNextEntryIndex(), 4);
    EXPECT_EQ(changelog.getLastEntry()->get_term(), 20);
    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));

    changelog.appendEntry(changelog.getNextEntryIndex(), getLogEntry("hello world", 30));
    changelog.appendEntry(changelog.getNextEntryIndex(), getLogEntry("hello world", 40));
    changelog.appendEntry(changelog.getNextEntryIndex(), getLogEntry("hello world", 50));
    changelog.appendEntry(changelog.getNextEntryIndex(), getLogEntry("hello world", 60));

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    changelog.compact(6);

    EXPECT_FALSE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    EXPECT_EQ(changelog.size(), 1);
    EXPECT_EQ(changelog.getStartIndex(), 7);
    EXPECT_EQ(changelog.getNextEntryIndex(), 8);
    EXPECT_EQ(changelog.getLastEntry()->get_term(), 60);
    /// And we able to read it
    DB::Changelog changelog_reader("./logs", 5);
    changelog_reader.readChangelogAndInitWriter(7);
    EXPECT_EQ(changelog_reader.size(), 1);
    EXPECT_EQ(changelog_reader.getStartIndex(), 7);
    EXPECT_EQ(changelog_reader.getNextEntryIndex(), 8);
    EXPECT_EQ(changelog_reader.getLastEntry()->get_term(), 60);
}

TEST(CoordinationTest, ChangelogTestBatchOperations)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 100);
    changelog.readChangelogAndInitWriter(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }

    EXPECT_EQ(changelog.size(), 10);

    auto entries = changelog.serializeEntriesToBuffer(1, 5);

    DB::Changelog apply_changelog("./logs", 100);
    apply_changelog.readChangelogAndInitWriter(1);

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_EQ(apply_changelog.entryAt(i + 1)->get_term(), i * 10);
    }
    EXPECT_EQ(apply_changelog.size(), 10);

    apply_changelog.applyEntriesFromBuffer(8, *entries);

    EXPECT_EQ(apply_changelog.size(), 12);
    EXPECT_EQ(apply_changelog.getStartIndex(), 1);
    EXPECT_EQ(apply_changelog.getNextEntryIndex(), 13);

    for (size_t i = 0; i < 7; ++i)
    {
        EXPECT_EQ(apply_changelog.entryAt(i + 1)->get_term(), i * 10);
    }

    EXPECT_EQ(apply_changelog.entryAt(8)->get_term(), 0);
    EXPECT_EQ(apply_changelog.entryAt(9)->get_term(), 10);
    EXPECT_EQ(apply_changelog.entryAt(10)->get_term(), 20);
    EXPECT_EQ(apply_changelog.entryAt(11)->get_term(), 30);
    EXPECT_EQ(apply_changelog.entryAt(12)->get_term(), 40);
}

TEST(CoordinationTest, ChangelogTestBatchOperationsEmpty)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 100);
    changelog.readChangelogAndInitWriter(1);
    for (size_t i = 0; i < 10; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }

    EXPECT_EQ(changelog.size(), 10);

    auto entries = changelog.serializeEntriesToBuffer(5, 5);

    ChangelogDirTest test1("./logs1");
    DB::Changelog changelog_new("./logs1", 100);
    changelog_new.readChangelogAndInitWriter(1);
    EXPECT_EQ(changelog_new.size(), 0);

    changelog_new.applyEntriesFromBuffer(5, *entries);

    EXPECT_EQ(changelog_new.size(), 5);
    EXPECT_EQ(changelog_new.getStartIndex(), 5);
    EXPECT_EQ(changelog_new.getNextEntryIndex(), 10);

    for (size_t i = 4; i < 9; ++i)
        EXPECT_EQ(changelog_new.entryAt(i + 1)->get_term(), i * 10);

    changelog_new.appendEntry(changelog_new.getNextEntryIndex(), getLogEntry("hello_world", 110));
    EXPECT_EQ(changelog_new.size(), 6);
    EXPECT_EQ(changelog_new.getStartIndex(), 5);
    EXPECT_EQ(changelog_new.getNextEntryIndex(), 11);

    DB::Changelog changelog_reader("./logs1", 100);
    changelog_reader.readChangelogAndInitWriter(5);
}


TEST(CoordinationTest, ChangelogTestWriteAtPreviousFile)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 5);
    changelog.readChangelogAndInitWriter(1);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));

    EXPECT_EQ(changelog.size(), 33);

    changelog.writeAt(7, getLogEntry("helloworld", 5555));
    EXPECT_EQ(changelog.size(), 7);
    EXPECT_EQ(changelog.getStartIndex(), 1);
    EXPECT_EQ(changelog.getNextEntryIndex(), 8);
    EXPECT_EQ(changelog.getLastEntry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));

    EXPECT_FALSE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin"));

    DB::Changelog changelog_read("./logs", 5);
    changelog_read.readChangelogAndInitWriter(1);
    EXPECT_EQ(changelog_read.size(), 7);
    EXPECT_EQ(changelog_read.getStartIndex(), 1);
    EXPECT_EQ(changelog_read.getNextEntryIndex(), 8);
    EXPECT_EQ(changelog_read.getLastEntry()->get_term(), 5555);
}

TEST(CoordinationTest, ChangelogTestWriteAtFileBorder)
{
    ChangelogDirTest test("./logs");
    DB::Changelog changelog("./logs", 5);
    changelog.readChangelogAndInitWriter(1);

    for (size_t i = 0; i < 33; ++i)
    {
        auto entry = getLogEntry(std::to_string(i) + "_hello_world", i * 10);
        changelog.appendEntry(changelog.getNextEntryIndex(), entry);
    }

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_31_35.bin"));

    EXPECT_EQ(changelog.size(), 33);

    changelog.writeAt(11, getLogEntry("helloworld", 5555));
    EXPECT_EQ(changelog.size(), 11);
    EXPECT_EQ(changelog.getStartIndex(), 1);
    EXPECT_EQ(changelog.getNextEntryIndex(), 12);
    EXPECT_EQ(changelog.getLastEntry()->get_term(), 5555);

    EXPECT_TRUE(fs::exists("./logs/changelog_1_5.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_6_10.bin"));
    EXPECT_TRUE(fs::exists("./logs/changelog_11_15.bin"));

    EXPECT_FALSE(fs::exists("./logs/changelog_16_20.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_21_25.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_26_30.bin"));
    EXPECT_FALSE(fs::exists("./logs/changelog_31_35.bin"));

    DB::Changelog changelog_read("./logs", 5);
    changelog_read.readChangelogAndInitWriter(1);
    EXPECT_EQ(changelog_read.size(), 11);
    EXPECT_EQ(changelog_read.getStartIndex(), 1);
    EXPECT_EQ(changelog_read.getNextEntryIndex(), 12);
    EXPECT_EQ(changelog_read.getLastEntry()->get_term(), 5555);
}

#endif
