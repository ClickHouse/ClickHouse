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
#include <unordered_map>
#include <future>

namespace DB
{

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

class TestKeeperTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    TestKeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_);
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
    SocketInterruptablePollWrapperPtr poll_wrapper;

    size_t response_id_counter = 0;
    std::unordered_map<size_t, zkutil::TestKeeperStorage::AsyncResponse> responses;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    void runImpl();

    void sendHandshake();
    void receiveHandshake();

    Coordination::OpNum receiveRequest();
    zkutil::TestKeeperStorage::AsyncResponse putCloseRequest();
};

}
