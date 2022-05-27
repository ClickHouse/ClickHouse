#pragma once

#include <Common/config.h>
#include "config_core.h"

#if USE_NURAFT

#include <Poco/Net/TCPServerConnection.h>
#include <Common/MultiVersion.h>
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
#include <Coordination/KeeperConnectionStats.h>
#include <Poco/Timestamp.h>

namespace DB
{

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

using ThreadSafeResponseQueue = ConcurrentBoundedQueue<Coordination::ZooKeeperResponsePtr>;
using ThreadSafeResponseQueuePtr = std::unique_ptr<ThreadSafeResponseQueue>;

struct LastOp;
using LastOpMultiVersion = MultiVersion<LastOp>;
using LastOpPtr = LastOpMultiVersion::Version;

class KeeperTCPHandler : public Poco::Net::TCPServerConnection
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
    KeeperTCPHandler(
        const Poco::Util::AbstractConfiguration & config_ref,
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        Poco::Timespan receive_timeout_,
        Poco::Timespan send_timeout_,
        const Poco::Net::StreamSocket & socket_);
    void run() override;

    KeeperConnectionStats & getConnectionStats();
    void dumpStats(WriteBufferFromOwnString & buf, bool brief);
    void resetStats();

    ~KeeperTCPHandler() override;

private:
    Poco::Logger * log;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
    Poco::Timespan operation_timeout;
    Poco::Timespan min_session_timeout;
    Poco::Timespan max_session_timeout;
    Poco::Timespan session_timeout;
    int64_t session_id{-1};
    Stopwatch session_stopwatch;
    SocketInterruptablePollWrapperPtr poll_wrapper;
    Poco::Timespan send_timeout;
    Poco::Timespan receive_timeout;

    ThreadSafeResponseQueuePtr responses;

    Coordination::XID close_xid = Coordination::CLOSE_XID;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    void runImpl();

    void sendHandshake(bool has_leader);
    Poco::Timespan receiveHandshake(int32_t handshake_length);

    static bool isHandShake(int32_t handshake_length);
    bool tryExecuteFourLetterWordCmd(int32_t command);

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest();

    void packageSent();
    void packageReceived();

    void updateStats(Coordination::ZooKeeperResponsePtr & response);

    Poco::Timestamp established;

    using Operations = std::unordered_map<Coordination::XID, Poco::Timestamp>;
    Operations operations;

    LastOpMultiVersion last_op;

    KeeperConnectionStats conn_stats;

};

}
#endif
