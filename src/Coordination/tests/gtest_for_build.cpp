#include <gtest/gtest.h>

#include <Coordination/InMemoryLogStore.h>
#include <Coordination/InMemoryStateManager.h>
#include <Coordination/SummingStateMachine.h>
#include <Common/Exception.h>
#include <libnuraft/nuraft.hxx>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}

TEST(CoordinationTest, BuildTest)
{
    DB::InMemoryLogStore store;
    DB::InMemoryStateManager state_manager(1, "localhost:12345");
    DB::SummingStateMachine machine;
    EXPECT_EQ(1, 1);
}

struct SummingRaftServer
{
    SummingRaftServer(int server_id_, const std::string & hostname_, int port_)
        : server_id(server_id_)
        , hostname(hostname_)
        , port(port_)
        , endpoint(hostname + ":" + std::to_string(port))
        , state_machine(nuraft::cs_new<DB::SummingStateMachine>())
        , state_manager(nuraft::cs_new<DB::InMemoryStateManager>(server_id, endpoint))
    {
        nuraft::raft_params params;
        params.heart_beat_interval_ = 100;
        params.election_timeout_lower_bound_ = 200;
        params.election_timeout_upper_bound_ = 400;
        params.reserved_log_items_ = 5;
        params.snapshot_distance_ = 5;
        params.client_req_timeout_ = 3000;
        params.return_method_ = nuraft::raft_params::blocking;

        raft_instance = launcher.init(
            state_machine, state_manager, nuraft::cs_new<nuraft::logger>(), port,
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
    nuraft::ptr<DB::SummingStateMachine> state_machine;

    // State manager.
    nuraft::ptr<nuraft::state_mgr> state_manager;

    // Raft launcher.
    nuraft::raft_launcher launcher;

    // Raft server instance.
    nuraft::ptr<nuraft::raft_server> raft_instance;
};

nuraft::ptr<nuraft::buffer> getLogEntry(int64_t number)
{
    nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(sizeof(number));
    nuraft::buffer_serializer bs(ret);
    // WARNING: We don't consider endian-safety in this example.
    bs.put_raw(&number, sizeof(number));
    return ret;
}

TEST(CoordinationTest, TestSummingRaft)
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

    while(s1.raft_instance->get_leader() != 2)
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

    while(s3.raft_instance->get_leader() != 2)
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
    if (!ret->get_accepted())
    {
        // Log append rejected, usually because this node is not a leader.
        std::cout << "failed to replicate: entry 1" << ret->get_result_code() << std::endl;
        EXPECT_TRUE(false);
    }
    if (ret->get_result_code() != nuraft::cmd_result_code::OK)
    {
        // Something went wrong.
        // This means committing this log failed,
        // but the log itself is still in the log store.
        std::cout << "failed to replicate: entry 1" << ret->get_result_code() << std::endl;
        EXPECT_TRUE(false);
    }
    else
    {
        std::cout << "Append ok\n";
    }

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

    EXPECT_EQ(s1.state_machine->getValue(), 1);
    EXPECT_EQ(s2.state_machine->getValue(), 1);
    EXPECT_EQ(s3.state_machine->getValue(), 1);

    s1.launcher.shutdown(5);
    s2.launcher.shutdown(5);
    s3.launcher.shutdown(5);
}
