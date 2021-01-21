#pragma once

#include <libnuraft/nuraft.hxx>
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/InMemoryStateManager.h>
#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/TestKeeperStorage.h>

namespace DB
{

class NuKeeperServer
{
private:
    int server_id;

    std::string hostname;

    int port;

    std::string endpoint;

    nuraft::ptr<StateMachine> state_machine;

    nuraft::ptr<nuraft::state_mgr> state_manager;

    nuraft::raft_launcher launcher;

    nuraft::ptr<nuraft::raft_server> raft_instance;

public:
    NuKeeperServer(int server_id, const std::string & hostname, int port);

    void startup();

    TestKeeperStorage::ResponsesForSessions putRequests(const TestKeeperStorage::RequestsForSessions & requests);

    void addServer(int server_id_, const std::string & server_uri);

    void shutdown();
};

}
