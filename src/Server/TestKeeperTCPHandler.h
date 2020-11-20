#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include "IServer.h"
#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/TestKeeperStorage.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <future>

namespace DB
{

class TestKeeperTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    TestKeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
        : Poco::Net::TCPServerConnection(socket_)
        , server(server_)
        , log(&Poco::Logger::get("TestKeeperTCPHandler"))
        , global_context(server.context())
        , test_keeper_storage(global_context.getTestKeeperStorage())
        , operation_timeout(0, Coordination::DEFAULT_OPERATION_TIMEOUT_MS * 1000)
        , session_timeout(0, Coordination::DEFAULT_SESSION_TIMEOUT_MS * 1000)
        , session_id(test_keeper_storage->getSessionID())
    {
    }

    void run() override;
private:
    IServer & server;
    Poco::Logger * log;
    Context global_context;
    std::shared_ptr<zkutil::TestKeeperStorage> test_keeper_storage;
    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;
    int64_t session_id;
    Stopwatch session_stopwatch;

    std::queue<zkutil::TestKeeperStorage::AsyncResponse> responses;
    std::vector<zkutil::TestKeeperStorage::AsyncResponse> watch_responses;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    void runImpl();

    void sendHandshake();
    void receiveHandshake();

    Coordination::OpNum receiveRequest();
    void putCloseRequest();
};

}
