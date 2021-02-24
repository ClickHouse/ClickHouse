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

nuraft::ptr<nuraft::buffer> getLogEntry(int64_t number)
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

    auto entry1 = getLogEntry(143);
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
    auto entry = getLogEntry(1);
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

    auto non_leader_entry = getLogEntry(3);
    auto ret_non_leader1 = s1.raft_instance->append_entries({non_leader_entry});

    EXPECT_FALSE(ret_non_leader1->get_accepted());

    auto ret_non_leader3 = s3.raft_instance->append_entries({non_leader_entry});

    EXPECT_FALSE(ret_non_leader3->get_accepted());

    auto leader_entry = getLogEntry(77);
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

#endif
