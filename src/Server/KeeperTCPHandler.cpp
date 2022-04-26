#include <Server/KeeperTCPHandler.h>

#if USE_NURAFT

#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Core/Types.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Poco/Net/NetException.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <Common/NetException.h>
#include <Common/setThreadName.h>
#include <base/logger_useful.h>
#include <chrono>
#include <Common/PipeFDs.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <queue>
#include <mutex>
#include <Coordination/FourLetterCommand.h>
#include <Common/hex.h>


#ifdef POCO_HAVE_FD_EPOLL
    #include <sys/epoll.h>
#else
    #include <poll.h>
#endif


namespace DB
{

struct LastOp
{
public:
    String name{"NA"};
    int64_t last_cxid{-1};
    int64_t last_zxid{-1};
    int64_t last_response_time{0};
};

static const LastOp EMPTY_LAST_OP {"NA", -1, -1, 0};

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
}

struct PollResult
{
    size_t responses_count{0};
    bool has_requests{false};
    bool error{false};
};

struct SocketInterruptablePollWrapper
{
    int sockfd;
    PipeFDs pipe;
    ReadBufferFromFileDescriptor response_in;

#if defined(POCO_HAVE_FD_EPOLL)
    int epollfd;
    epoll_event socket_event{};
    epoll_event pipe_event{};
#endif

    using InterruptCallback = std::function<void()>;

    explicit SocketInterruptablePollWrapper(const Poco::Net::StreamSocket & poco_socket_)
        : sockfd(poco_socket_.impl()->sockfd())
        , response_in(pipe.fds_rw[0])
    {
        pipe.setNonBlockingReadWrite();

#if defined(POCO_HAVE_FD_EPOLL)
        epollfd = epoll_create(2);
        if (epollfd < 0)
            throwFromErrno("Cannot epoll_create", ErrorCodes::SYSTEM_ERROR);

        socket_event.events = EPOLLIN | EPOLLERR | EPOLLPRI;
        socket_event.data.fd = sockfd;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sockfd, &socket_event) < 0)
        {
            ::close(epollfd);
            throwFromErrno("Cannot insert socket into epoll queue", ErrorCodes::SYSTEM_ERROR);
        }
        pipe_event.events = EPOLLIN | EPOLLERR | EPOLLPRI;
        pipe_event.data.fd = pipe.fds_rw[0];
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, pipe.fds_rw[0], &pipe_event) < 0)
        {
            ::close(epollfd);
            throwFromErrno("Cannot insert socket into epoll queue", ErrorCodes::SYSTEM_ERROR);
        }
#endif
    }

    int getResponseFD() const
    {
        return pipe.fds_rw[1];
    }

    PollResult poll(Poco::Timespan remaining_time, const std::shared_ptr<ReadBufferFromPocoSocket> & in)
    {

        bool socket_ready = false;
        bool fd_ready = false;

        if (in->available() != 0)
            socket_ready = true;

        if (response_in.available() != 0)
            fd_ready = true;

        int rc = 0;
        if (!fd_ready)
        {
#if defined(POCO_HAVE_FD_EPOLL)
            epoll_event evout[2];
            evout[0].data.fd = evout[1].data.fd = -1;
            do
            {
                Poco::Timestamp start;
                rc = epoll_wait(epollfd, evout, 2, remaining_time.totalMilliseconds());
                if (rc < 0 && errno == EINTR)
                {
                    Poco::Timestamp end;
                    Poco::Timespan waited = end - start;
                    if (waited < remaining_time)
                        remaining_time -= waited;
                    else
                        remaining_time = 0;
                }
            }
            while (rc < 0 && errno == EINTR);

            for (int i = 0; i < rc; ++i)
            {
                if (evout[i].data.fd == sockfd)
                    socket_ready = true;
                if (evout[i].data.fd == pipe.fds_rw[0])
                    fd_ready = true;
            }
#else
            pollfd poll_buf[2];
            poll_buf[0].fd = sockfd;
            poll_buf[0].events = POLLIN;
            poll_buf[1].fd = pipe.fds_rw[0];
            poll_buf[1].events = POLLIN;

            do
            {
                Poco::Timestamp start;
                rc = ::poll(poll_buf, 2, remaining_time.totalMilliseconds());
                if (rc < 0 && errno == POCO_EINTR)
                {
                    Poco::Timestamp end;
                    Poco::Timespan waited = end - start;
                    if (waited < remaining_time)
                        remaining_time -= waited;
                    else
                        remaining_time = 0;
                }
            }
            while (rc < 0 && errno == POCO_EINTR);

            if (rc >= 1 && poll_buf[0].revents & POLLIN)
                socket_ready = true;
            if (rc >= 2 && poll_buf[1].revents & POLLIN)
                fd_ready = true;
#endif
        }

        PollResult result{};
        result.has_requests = socket_ready;
        if (fd_ready)
        {
            UInt8 dummy;
            readIntBinary(dummy, response_in);
            result.responses_count = 1;
            auto available = response_in.available();
            response_in.ignore(available);
            result.responses_count += available;
        }

        if (rc < 0)
            result.error = true;

        return result;
    }

#if defined(POCO_HAVE_FD_EPOLL)
    ~SocketInterruptablePollWrapper()
    {
        ::close(epollfd);
    }
#endif
};

KeeperTCPHandler::KeeperTCPHandler(
    const Poco::Util::AbstractConfiguration & config_ref,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
    Poco::Timespan receive_timeout_,
    Poco::Timespan send_timeout_,
    const Poco::Net::StreamSocket & socket_)
    : Poco::Net::TCPServerConnection(socket_)
    , log(&Poco::Logger::get("KeeperTCPHandler"))
    , keeper_dispatcher(keeper_dispatcher_)
    , operation_timeout(
          0,
          config_ref.getUInt(
              "keeper_server.coordination_settings.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , min_session_timeout(
          0,
          config_ref.getUInt(
              "keeper_server.coordination_settings.min_session_timeout_ms", Coordination::DEFAULT_MIN_SESSION_TIMEOUT_MS) * 1000)
    , max_session_timeout(
          0,
          config_ref.getUInt(
              "keeper_server.coordination_settings.session_timeout_ms", Coordination::DEFAULT_MAX_SESSION_TIMEOUT_MS) * 1000)
    , poll_wrapper(std::make_unique<SocketInterruptablePollWrapper>(socket_))
    , send_timeout(send_timeout_)
    , receive_timeout(receive_timeout_)
    , responses(std::make_unique<ThreadSafeResponseQueue>(std::numeric_limits<size_t>::max()))
    , last_op(std::make_unique<LastOp>(EMPTY_LAST_OP))
{
    KeeperTCPHandler::registerConnection(this);
}

void KeeperTCPHandler::sendHandshake(bool has_leader)
{
    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, *out);
    if (has_leader)
    {
        Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION, *out);
    }
    else
    {
        /// Ignore connections if we are not leader, client will throw exception
        /// and reconnect to another replica faster. ClickHouse client provide
        /// clear message for such protocol version.
        Coordination::write(Coordination::KEEPER_PROTOCOL_VERSION_CONNECTION_REJECT, *out);
    }

    Coordination::write(static_cast<int32_t>(session_timeout.totalMilliseconds()), *out);
    Coordination::write(session_id, *out);
    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};
    Coordination::write(passwd, *out);
    out->next();
}

void KeeperTCPHandler::run()
{
    runImpl();
}

Poco::Timespan KeeperTCPHandler::receiveHandshake(int32_t handshake_length)
{
    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout_ms;
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    std::array<char, Coordination::PASSWORD_LENGTH> passwd {};

    if (!isHandShake(handshake_length))
        throw Exception("Unexpected handshake length received: " + toString(handshake_length), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    Coordination::read(protocol_version, *in);

    if (protocol_version != Coordination::ZOOKEEPER_PROTOCOL_VERSION)
        throw Exception("Unexpected protocol version: " + toString(protocol_version), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    Coordination::read(last_zxid_seen, *in);
    Coordination::read(timeout_ms, *in);

    /// TODO Stop ignoring this value
    Coordination::read(previous_session_id, *in);
    Coordination::read(passwd, *in);

    int8_t readonly;
    if (handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY)
        Coordination::read(readonly, *in);

    return Poco::Timespan(0, timeout_ms * 1000);
}


void KeeperTCPHandler::runImpl()
{
    setThreadName("KeeperHandler");
    ThreadStatus thread_status;

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    int32_t header;
    try
    {
        Coordination::read(header, *in);
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Error while read connection header {}", e.displayText());
        return;
    }

    /// All four letter word command code is larger than 2^24 or lower than 0.
    /// Hand shake package length must be lower than 2^24 and larger than 0.
    /// So collision never happens.
    int32_t four_letter_cmd = header;
    if (!isHandShake(four_letter_cmd))
    {
        tryExecuteFourLetterWordCmd(four_letter_cmd);
        return;
    }

    try
    {
        int32_t handshake_length = header;
        auto client_timeout = receiveHandshake(handshake_length);

        if (client_timeout == 0)
            client_timeout = Coordination::DEFAULT_SESSION_TIMEOUT_MS;
        session_timeout = std::max(client_timeout, min_session_timeout);
        session_timeout = std::min(session_timeout, max_session_timeout);
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        LOG_WARNING(log, "Cannot receive handshake {}", e.displayText());
        return;
    }

    // we store the checks because they can change during the execution
    // leading to weird results
    const auto is_initialized = keeper_dispatcher->checkInit();
    const auto has_leader = keeper_dispatcher->hasLeader();
    if (is_initialized && has_leader)
    {
        try
        {
            LOG_INFO(log, "Requesting session ID for the new client");
            session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());
            LOG_INFO(log, "Received session ID {}", session_id);
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, "Cannot receive session id {}", e.displayText());
            sendHandshake(false);
            return;

        }

        sendHandshake(true);
    }
    else
    {
        String reason;
        if (!is_initialized && !has_leader)
            reason = "server is not initialized yet and no alive leader exists";
        else if (!is_initialized)
            reason = "server is not initialized yet";
        else
            reason = "no alive leader exists";

        LOG_WARNING(log, "Ignoring user request, because {}", reason);
        sendHandshake(false);
        return;
    }

    auto response_fd = poll_wrapper->getResponseFD();
    auto response_callback = [this, response_fd] (const Coordination::ZooKeeperResponsePtr & response)
    {
        if (!responses->push(response))
            throw Exception(ErrorCodes::SYSTEM_ERROR,
                "Could not push response with xid {} and zxid {}",
                response->xid,
                response->zxid);

        UInt8 single_byte = 1;
        [[maybe_unused]] int result = write(response_fd, &single_byte, sizeof(single_byte));
    };
    keeper_dispatcher->registerSession(session_id, response_callback);

    Stopwatch logging_stopwatch;
    auto log_long_operation = [&](const String & operation)
    {
        constexpr UInt64 operation_max_ms = 500;
        auto elapsed_ms = logging_stopwatch.elapsedMilliseconds();
        if (operation_max_ms < elapsed_ms)
            LOG_TEST(log, "{} for session {} took {} ms", operation, session_id, elapsed_ms);
        logging_stopwatch.restart();
    };

    session_stopwatch.start();
    bool close_received = false;

    try
    {
        while (true)
        {
            using namespace std::chrono_literals;

            PollResult result = poll_wrapper->poll(session_timeout, in);
            log_long_operation("Polling socket");
            if (result.has_requests && !close_received)
            {
                if (in->eof())
                {
                    LOG_DEBUG(log, "Client closed connection, session id #{}", session_id);
                    keeper_dispatcher->finishSession(session_id);
                    break;
                }

                auto [received_op, received_xid] = receiveRequest();
                packageReceived();
                log_long_operation("Receiving request");

                if (received_op == Coordination::OpNum::Close)
                {
                    LOG_DEBUG(log, "Received close event with xid {} for session id #{}", received_xid, session_id);
                    close_xid = received_xid;
                    close_received = true;
                }
                else if (received_op == Coordination::OpNum::Heartbeat)
                {
                    LOG_TRACE(log, "Received heartbeat for session #{}", session_id);
                }
                else
                    operations[received_xid] = Poco::Timestamp();

                /// Each request restarts session stopwatch
                session_stopwatch.restart();
            }

            /// Process exact amount of responses from pipe
            /// otherwise state of responses queue and signaling pipe
            /// became inconsistent and race condition is possible.
            while (result.responses_count != 0)
            {
                Coordination::ZooKeeperResponsePtr response;

                if (!responses->tryPop(response))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "We must have ready response, but queue is empty. It's a bug.");
                log_long_operation("Waiting for response to be ready");

                if (response->xid == close_xid)
                {
                    LOG_DEBUG(log, "Session #{} successfully closed", session_id);
                    return;
                }

                updateStats(response);
                packageSent();

                response->write(*out);
                log_long_operation("Sending response");
                if (response->error == Coordination::Error::ZSESSIONEXPIRED)
                {
                    LOG_DEBUG(log, "Session #{} expired because server shutting down or quorum is not alive", session_id);
                    keeper_dispatcher->finishSession(session_id);
                    return;
                }

                result.responses_count--;
            }

            if (result.error)
                throw Exception("Exception happened while reading from socket", ErrorCodes::SYSTEM_ERROR);

            if (session_stopwatch.elapsedMicroseconds() > static_cast<UInt64>(session_timeout.totalMicroseconds()))
            {
                LOG_DEBUG(log, "Session #{} expired", session_id);
                keeper_dispatcher->finishSession(session_id);
                break;
            }
        }
    }
    catch (const Exception & ex)
    {
        log_long_operation("Unknown operation");
        LOG_TRACE(log, "Has {} responses in the queue", responses->size());
        LOG_INFO(log, "Got exception processing session #{}: {}", session_id, getExceptionMessage(ex, true));
        keeper_dispatcher->finishSession(session_id);
    }
}

bool KeeperTCPHandler::isHandShake(int32_t handshake_length)
{
    return handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH
    || handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY;
}

bool KeeperTCPHandler::tryExecuteFourLetterWordCmd(int32_t command)
{
    if (!FourLetterCommandFactory::instance().isKnown(command))
    {
        LOG_WARNING(log, "invalid four letter command {}", IFourLetterCommand::toName(command));
        return false;
    }
    else if (!FourLetterCommandFactory::instance().isEnabled(command))
    {
        LOG_WARNING(log, "Not enabled four letter command {}", IFourLetterCommand::toName(command));
        return false;
    }
    else
    {
        auto command_ptr = FourLetterCommandFactory::instance().get(command);
        LOG_DEBUG(log, "Receive four letter command {}", command_ptr->name());

        try
        {
            String res = command_ptr->run();
            out->write(res.data(), res.size());
            out->next();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error when executing four letter command " + command_ptr->name());
        }

        return true;
    }
}

std::pair<Coordination::OpNum, Coordination::XID> KeeperTCPHandler::receiveRequest()
{
    int32_t length;
    Coordination::read(length, *in);
    int32_t xid;
    Coordination::read(xid, *in);

    Coordination::OpNum opnum;
    Coordination::read(opnum, *in);

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;
    request->readImpl(*in);

    if (!keeper_dispatcher->putRequest(request, session_id))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", session_id);
    return std::make_pair(opnum, xid);
}

void KeeperTCPHandler::packageSent()
{
    conn_stats.incrementPacketsSent();
    keeper_dispatcher->incrementPacketsSent();
}

void KeeperTCPHandler::packageReceived()
{
    conn_stats.incrementPacketsReceived();
    keeper_dispatcher->incrementPacketsReceived();
}

void KeeperTCPHandler::updateStats(Coordination::ZooKeeperResponsePtr & response)
{
    /// update statistics ignoring watch response and heartbeat.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() != Coordination::OpNum::Heartbeat)
    {
        Int64 elapsed = (Poco::Timestamp() - operations[response->xid]) / 1000;
        conn_stats.updateLatency(elapsed);

        operations.erase(response->xid);
        keeper_dispatcher->updateKeeperStatLatency(elapsed);

        last_op.set(std::make_unique<LastOp>(LastOp{
            .name = Coordination::toString(response->getOpNum()),
            .last_cxid = response->xid,
            .last_zxid = response->zxid,
            .last_response_time = Poco::Timestamp().epochMicroseconds() / 1000,
        }));
    }

}

KeeperConnectionStats & KeeperTCPHandler::getConnectionStats()
{
    return conn_stats;
}

void KeeperTCPHandler::dumpStats(WriteBufferFromOwnString & buf, bool brief)
{
    auto & stats = getConnectionStats();

    writeText(' ', buf);
    writeText(socket().peerAddress().toString(), buf);
    writeText("(recved=", buf);
    writeIntText(stats.getPacketsReceived(), buf);
    writeText(",sent=", buf);
    writeIntText(stats.getPacketsSent(), buf);
    if (!brief)
    {
        if (session_id != 0)
        {
            writeText(",sid=0x", buf);
            writeText(getHexUIntLowercase(session_id), buf);

            writeText(",lop=", buf);
            LastOpPtr op = last_op.get();
            writeText(op->name, buf);
            writeText(",est=", buf);
            writeIntText(established.epochMicroseconds() / 1000, buf);
            writeText(",to=", buf);
            writeIntText(session_timeout.totalMilliseconds(), buf);
            int64_t last_cxid = op->last_cxid;
            if (last_cxid >= 0)
            {
                writeText(",lcxid=0x", buf);
                writeText(getHexUIntLowercase(last_cxid), buf);
            }
            writeText(",lzxid=0x", buf);
            writeText(getHexUIntLowercase(op->last_zxid), buf);
            writeText(",lresp=", buf);
            writeIntText(op->last_response_time, buf);

            writeText(",llat=", buf);
            writeIntText(stats.getLastLatency(), buf);
            writeText(",minlat=", buf);
            writeIntText(stats.getMinLatency(), buf);
            writeText(",avglat=", buf);
            writeIntText(stats.getAvgLatency(), buf);
            writeText(",maxlat=", buf);
            writeIntText(stats.getMaxLatency(), buf);
        }
    }
    writeText(')', buf);
    writeText('\n', buf);
}

void KeeperTCPHandler::resetStats()
{
    conn_stats.reset();
    last_op.set(std::make_unique<LastOp>(EMPTY_LAST_OP));
}

KeeperTCPHandler::~KeeperTCPHandler()
{
    KeeperTCPHandler::unregisterConnection(this);
}

std::mutex KeeperTCPHandler::conns_mutex;
std::unordered_set<KeeperTCPHandler *> KeeperTCPHandler::connections;

void KeeperTCPHandler::registerConnection(KeeperTCPHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.insert(conn);
}

void KeeperTCPHandler::unregisterConnection(KeeperTCPHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.erase(conn);
}

void KeeperTCPHandler::dumpConnections(WriteBufferFromOwnString & buf, bool brief)
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->dumpStats(buf, brief);
    }
}

void KeeperTCPHandler::resetConnsStats()
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->resetStats();
    }
}

}

#endif
