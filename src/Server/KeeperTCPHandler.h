#pragma once

#include <Common/config.h>
#include "config_core.h"

#if USE_NURAFT

#include <Poco/Net/TCPServerConnection.h>
#include "IServer.h"
#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Coordination/KeeperDispatcher.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <unordered_map>
#include <Coordination/KeeperInfos.h>
#include <Coordination/KeeperStats.h>
#include <Poco/Timestamp.h>

namespace DB
{

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

using ThreadSafeResponseQueue = ConcurrentBoundedQueue<Coordination::ZooKeeperResponsePtr>;

using ThreadSafeResponseQueuePtr = std::unique_ptr<ThreadSafeResponseQueue>;

class KeeperTCPHandler : public Poco::Net::TCPServerConnection, IConnectionInfo
{
public:
    static void registerConnection(KeeperTCPHandler * conn);
    static void unregisterConnection(KeeperTCPHandler * conn);
    /// dump all connections statistics
    static void dumpConnections(WriteBufferFromOwnString & buf, bool brief);
    static void resetConnsStats();

private:
    static std::mutex conns_mutex;
    /// all connections
    static std::unordered_set<KeeperTCPHandler *> connections;

public:
    KeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_);
    void run() override;

    void dumpStats(WriteBufferFromOwnString & buf, bool brief);
    void resetStats();

    /// statistics methods
    Int64 getPacketsReceived() const override;
    Int64 getPacketsSent() const override;
    Int64 getSessionId() const override;
    Int64 getSessionTimeout() const override;
    Poco::Timestamp getEstablished() const override;
    LastOp getLastOp() const override;
    KeeperStatsPtr getSessionStats() const override;

    ~KeeperTCPHandler() override;

private:
    IServer & server;
    Poco::Logger * log;
    ContextPtr global_context;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
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
    Poco::Timespan receiveHandshake(int32_t handshake_length);

    static bool isHandShake(Int32 & handshake_length) ;
    bool tryExecuteFourLetterWordCmd(Int32 & command);

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest();

    void packageSent();
    void packageReceived();

    void updateStats(Coordination::ZooKeeperResponsePtr & response);

    Poco::Timestamp established;

    using Operations = std::map<Coordination::XID, Poco::Timestamp>;
    Operations operations;

    std::mutex last_op_mutex;
    LastOp last_op;

    KeeperStatsPtr conn_stats;

};

}
#endif
