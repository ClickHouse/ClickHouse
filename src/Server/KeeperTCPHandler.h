#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Poco/Net/TCPServerConnection.h>
#include "IServer.h"
#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Coordination/KeeperStorageDispatcher.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Coordination/ThreadSafeQueue.h>
#include <unordered_map>

namespace DB
{

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

using ThreadSafeResponseQueue = ThreadSafeQueue<Coordination::ZooKeeperResponsePtr>;

using ThreadSafeResponseQueuePtr = std::unique_ptr<ThreadSafeResponseQueue>;

class KeeperTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    KeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_);
    void run() override;
private:
    IServer & server;
    Poco::Logger * log;
    ContextPtr global_context;
    std::shared_ptr<KeeperStorageDispatcher> keeper_dispatcher;
    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;
    int64_t session_id{-1};
    Stopwatch session_stopwatch;
    SocketInterruptablePollWrapperPtr poll_wrapper;

    ThreadSafeResponseQueuePtr responses;

    Coordination::XID close_xid = Coordination::CLOSE_XID;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    void runImpl();

    void sendHandshake(bool has_leader);
    Poco::Timespan receiveHandshake();

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest();
};

}
#endif
