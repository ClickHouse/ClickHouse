#pragma once

#include "config.h"

#if USE_NURAFT

#include <Poco/Net/TCPServerConnection.h>
#include <Common/MultiVersion.h>
#include "IServer.h"
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Coordination/KeeperDispatcher.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <unordered_map>
#include <Coordination/KeeperConnectionStats.h>
#include <Poco/Timestamp.h>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/detached.hpp>

namespace DB
{

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

using ThreadSafeResponseQueue = ConcurrentBoundedQueue<Coordination::ZooKeeperResponsePtr>;
using ThreadSafeResponseQueuePtr = std::shared_ptr<ThreadSafeResponseQueue>;

struct LastOp;
using LastOpMultiVersion = MultiVersion<LastOp>;
using LastOpPtr = LastOpMultiVersion::Version;

class KeeperSession;
using KeeperSessionPtr = std::shared_ptr<KeeperSession>;
using boost::asio::ip::tcp;

class KeeperSession : public std::enable_shared_from_this<KeeperSession>
{
private:
    static void registerConnection(KeeperSessionPtr conn);
    static void unregisterConnection(KeeperSessionPtr conn);
    /// dump all connections statistics
    //static void dumpConnections(WriteBufferFromOwnString & buf, bool brief);
    //static void resetConnsStats();

    static std::mutex conns_mutex;
    /// all connections
    static std::unordered_set<KeeperSessionPtr> sessions;

    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    tcp::socket socket;

    std::mutex response_mutex;
    bool writing_response = false;
    std::list<Coordination::ZooKeeperResponsePtr> responses;

    Poco::Timespan operation_timeout;
    Poco::Timespan min_session_timeout;
    Poco::Timespan max_session_timeout;
    Poco::Timespan session_timeout;

    int64_t session_id;

    using Operations = std::unordered_map<Coordination::XID, Poco::Timestamp>;
    Operations operations;

    boost::asio::strand<boost::asio::any_io_executor> strand;

    Poco::Logger * log = &Poco::Logger::get("KeeperSession");

    boost::asio::awaitable<void> requestReader();

    std::string tryExecuteFourLetterWordCmd(int32_t command);

    boost::asio::awaitable<Poco::Timespan> receiveHandshake(int32_t handshake_length);
    boost::asio::awaitable<void> sendHandshake(bool has_leader);

    boost::asio::awaitable<std::pair<Coordination::OpNum, Coordination::XID>> receiveRequest();

    static bool isHandShake(int32_t handshake_length);
public:
    static boost::asio::awaitable<void> listener(tcp::acceptor acceptor, std::shared_ptr<KeeperDispatcher> keeper_dispatcher);

    KeeperSession(tcp::socket socket_, std::shared_ptr<KeeperDispatcher> keeper_dispatcher_);

    void start();
    void stop();
};

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

    std::atomic<bool> connected{false};

    void runImpl();

    void sendHandshake(bool has_leader);
    Poco::Timespan receiveHandshake(int32_t handshake_length);

    static bool isHandShake(int32_t handshake_length);
    bool tryExecuteFourLetterWordCmd(int32_t command);
    std::string tryExecuteFourLetterWordCmd2(int32_t command);

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
