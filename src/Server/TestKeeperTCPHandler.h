#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include "IServer.h"
#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Coordination/TestKeeperStorageDispatcher.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <unordered_map>

namespace DB
{

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;
class ThreadSafeResponseQueue;
using ThreadSafeResponseQueuePtr = std::unique_ptr<ThreadSafeResponseQueue>;

class TestKeeperTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    TestKeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_);
    void run() override;
private:
    IServer & server;
    Poco::Logger * log;
    Context global_context;
    std::shared_ptr<TestKeeperStorageDispatcher> test_keeper_storage_dispatcher;
    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;
    int64_t session_id;
    Stopwatch session_stopwatch;
    SocketInterruptablePollWrapperPtr poll_wrapper;

    ThreadSafeResponseQueuePtr responses;

    Coordination::XID close_xid = Coordination::CLOSE_XID;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    void runImpl();

    void sendHandshake(bool is_leader);
    void receiveHandshake();

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest();
    void finish();
};

}
