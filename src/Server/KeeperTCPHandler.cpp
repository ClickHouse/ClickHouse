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
#include <common/logger_useful.h>
#include <chrono>
#include <Common/PipeFDs.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <queue>
#include <mutex>

#ifdef POCO_HAVE_FD_EPOLL
    #include <sys/epoll.h>
#else
    #include <poll.h>
#endif


namespace DB
{


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
            if (rc >= 1 && poll_buf[1].revents & POLLIN)
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

KeeperTCPHandler::KeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , log(&Poco::Logger::get("KeeperTCPHandler"))
    , global_context(Context::createCopy(server.context()))
    , keeper_dispatcher(global_context->getKeeperStorageDispatcher())
    , operation_timeout(0, global_context->getConfigRef().getUInt("test_keeper_server.coordination_settings.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , session_timeout(0, global_context->getConfigRef().getUInt("test_keeper_server.coordination_settings.session_timeout_ms", Coordination::DEFAULT_SESSION_TIMEOUT_MS) * 1000)
    , poll_wrapper(std::make_unique<SocketInterruptablePollWrapper>(socket_))
    , responses(std::make_unique<ThreadSafeResponseQueue>())
{
}

void KeeperTCPHandler::sendHandshake(bool has_leader)
{
    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, *out);
    if (has_leader)
        Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION, *out);
    else /// Specially ignore connections if we are not leader, client will throw exception
        Coordination::write(42, *out);

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

Poco::Timespan KeeperTCPHandler::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout_ms;
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    std::array<char, Coordination::PASSWORD_LENGTH> passwd {};
    Coordination::read(handshake_length, *in);
    if (handshake_length != Coordination::CLIENT_HANDSHAKE_LENGTH && handshake_length != Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY)
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
    setThreadName("TstKprHandler");
    ThreadStatus thread_status;
    auto global_receive_timeout = global_context->getSettingsRef().receive_timeout;
    auto global_send_timeout = global_context->getSettingsRef().send_timeout;

    socket().setReceiveTimeout(global_receive_timeout);
    socket().setSendTimeout(global_send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    try
    {
        auto client_timeout = receiveHandshake();
        if (client_timeout != 0)
            session_timeout = std::min(client_timeout, session_timeout);
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        LOG_WARNING(log, "Cannot receive handshake {}", e.displayText());
        return;
    }

    if (keeper_dispatcher->hasLeader())
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
        LOG_WARNING(log, "Ignoring user request, because no alive leader exist");
        sendHandshake(false);
        return;
    }

    auto response_fd = poll_wrapper->getResponseFD();
    auto response_callback = [this, response_fd] (const Coordination::ZooKeeperResponsePtr & response)
    {
        responses->push(response);
        UInt8 single_byte = 1;
        [[maybe_unused]] int result = write(response_fd, &single_byte, sizeof(single_byte));
    };
    keeper_dispatcher->registerSession(session_id, response_callback);

    session_stopwatch.start();
    bool close_received = false;
    try
    {
        while (true)
        {
            using namespace std::chrono_literals;

            PollResult result = poll_wrapper->poll(session_timeout, in);
            if (result.has_requests && !close_received)
            {
                auto [received_op, received_xid] = receiveRequest();

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

                if (response->xid == close_xid)
                {
                    LOG_DEBUG(log, "Session #{} successfully closed", session_id);
                    return;
                }

                response->write(*out);
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
        LOG_INFO(log, "Got exception processing session #{}: {}", session_id, getExceptionMessage(ex, true));
        keeper_dispatcher->finishSession(session_id);
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

}

#endif
