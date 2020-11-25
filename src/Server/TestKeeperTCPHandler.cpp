#include <Server/TestKeeperTCPHandler.h>
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
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

struct SocketInterruptablePollWrapper
{
    int sockfd;
    PipeFDs pipe;

    enum class PollStatus
    {
        HAS_DATA,
        TIMEOUT,
        INTERRUPTED,
        ERROR,
    };

    using InterruptCallback = std::function<void()>;

    explicit SocketInterruptablePollWrapper(const Poco::Net::StreamSocket & poco_socket_)
        : sockfd(poco_socket_.impl()->sockfd())
    {
        pipe.setNonBlocking();
    }

    int getInterruptFD() const
    {
        return pipe.fds_rw[1];
    }

    PollStatus poll(Poco::Timespan remaining_time)
    {

#if defined(POCO_HAVE_FD_EPOLL)
        int epollfd = epoll_create(2);

        if (epollfd < 0)
            throwFromErrno("Cannot epoll_create", ErrorCodes::SYSTEM_ERROR);

        epoll_event socket_event{};
        socket_event.events = EPOLLIN | EPOLLERR;
        socket_event.data.fd = sockfd;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sockfd, &socket_event) < 0)
        {
            ::close(epollfd);
            throwFromErrno("Cannot insert socket into epoll queue", ErrorCodes::SYSTEM_ERROR);
        }
        epoll_event pipe_event{};
        pipe_event.events = EPOLLIN | EPOLLERR;
        pipe_event.data.fd = pipe.fds_rw[0];
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, pipe.fds_rw[0], &pipe_event) < 0)
        {
            ::close(epollfd);
            throwFromErrno("Cannot insert socket into epoll queue", ErrorCodes::SYSTEM_ERROR);
        }

        int rc;
        epoll_event evout{};
        do
        {
            Poco::Timestamp start;
            rc = epoll_wait(epollfd, &evout, 1, remaining_time.totalMilliseconds());
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

        ::close(epollfd);
        int out_fd = evout.data.fd;
#else
        pollfd poll_buf[2];
        poll_buf[0].fd = sockfd;
        poll_buf[0].events = POLLIN;
        poll_buf[1].fd = pipe.fds_rw[0];
        poll_buf[1].events = POLLIN;

        int rc;
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
        int out_fd = -1;
        if (poll_buf[0].revents & POLLIN)
            out_fd = sockfd;
        else if (poll_buf[1].revents & POLLIN)
            out_fd = pipe.fds_rw[0];
#endif
        if (rc < 0)
            return PollStatus::ERROR;
        else if (rc == 0)
            return PollStatus::TIMEOUT;
        else if (out_fd == pipe.fds_rw[0])
        {
            UInt64 bytes;
            if (read(pipe.fds_rw[0], &bytes, sizeof(bytes)) < 0)
                throwFromErrno("Cannot read from pipe", ErrorCodes::SYSTEM_ERROR);
            return PollStatus::INTERRUPTED;
        }
        return PollStatus::HAS_DATA;
    }
};

TestKeeperTCPHandler::TestKeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , log(&Poco::Logger::get("TestKeeperTCPHandler"))
    , global_context(server.context())
    , test_keeper_storage(global_context.getTestKeeperStorage())
    , operation_timeout(0, global_context.getConfigRef().getUInt("test_keeper_server.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , session_timeout(0, global_context.getConfigRef().getUInt("test_keeper_server.session_timeout_ms",  Coordination::DEFAULT_SESSION_TIMEOUT_MS) * 1000)
    , session_id(test_keeper_storage->getSessionID())
    , poll_wrapper(std::make_unique<SocketInterruptablePollWrapper>(socket_))
{
}

void TestKeeperTCPHandler::sendHandshake()
{
    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, *out);
    Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION, *out);
    Coordination::write(Coordination::DEFAULT_SESSION_TIMEOUT_MS, *out);
    Coordination::write(session_id, *out);
    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};
    Coordination::write(passwd, *out);
    out->next();
}

void TestKeeperTCPHandler::run()
{
    runImpl();
}

void TestKeeperTCPHandler::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout;
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    std::array<char, Coordination::PASSWORD_LENGTH> passwd {};

    Coordination::read(handshake_length, *in);
    if (handshake_length != Coordination::CLIENT_HANDSHAKE_LENGTH)
        throw Exception("Unexpected handshake length received: " + toString(handshake_length), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    Coordination::read(protocol_version, *in);

    if (protocol_version != Coordination::ZOOKEEPER_PROTOCOL_VERSION)
        throw Exception("Unexpected protocol version: " + toString(protocol_version), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    Coordination::read(last_zxid_seen, *in);

    if (last_zxid_seen != 0)
        throw Exception("Non zero last_zxid_seen is not supported", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    Coordination::read(timeout, *in);
    Coordination::read(previous_session_id, *in);

    if (previous_session_id != 0)
        throw Exception("Non zero previous session id is not supported", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    Coordination::read(passwd, *in);
}


void TestKeeperTCPHandler::runImpl()
{
    setThreadName("TstKprHandler");
    ThreadStatus thread_status;
    auto global_receive_timeout = global_context.getSettingsRef().receive_timeout;
    auto global_send_timeout = global_context.getSettingsRef().send_timeout;

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
        receiveHandshake();
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        LOG_WARNING(log, "Cannot receive handshake {}", e.displayText());
        return;
    }

    sendHandshake();
    session_stopwatch.start();

    while (true)
    {
        using namespace std::chrono_literals;

        Poco::Timespan poll_wait = responses.empty() ? session_timeout.totalMicroseconds() - session_stopwatch.elapsedMicroseconds() : session_timeout;

        auto state = poll_wrapper->poll(poll_wait);
        if (state == SocketInterruptablePollWrapper::PollStatus::HAS_DATA)
        {
            auto received_op = receiveRequest();
            if (received_op == Coordination::OpNum::Close)
            {
                LOG_DEBUG(log, "Received close request for session #{}", session_id);
                break;
            }
            else if (received_op == Coordination::OpNum::Heartbeat)
            {
                session_stopwatch.restart();
            }
        }
        else if (state == SocketInterruptablePollWrapper::PollStatus::INTERRUPTED)
        {
            while (!responses.empty())
            {
                if (responses.front().wait_for(0ms) != std::future_status::ready)
                    break;

                auto response = responses.front().get();
                response->write(*out);
                responses.pop();
            }
            for (auto it = watch_responses.begin(); it != watch_responses.end();)
            {
                if (it->wait_for(0s) == std::future_status::ready)
                {
                    auto response = it->get();
                    if (response->error == Coordination::Error::ZOK)
                        response->write(*out);
                    it = watch_responses.erase(it);
                }
                else
                {
                    ++it;
                }
            }
        }
        else if (state == SocketInterruptablePollWrapper::PollStatus::ERROR)
        {
            throw Exception("Exception happened while reading from socket", ErrorCodes::SYSTEM_ERROR);
        }

        if (session_stopwatch.elapsedMicroseconds() > static_cast<UInt64>(session_timeout.totalMicroseconds()))
        {
            LOG_DEBUG(log, "Session #{} expired", session_id);
            putCloseRequest();
            break;
        }
    }
}

void TestKeeperTCPHandler::putCloseRequest()
{
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
    test_keeper_storage->putCloseRequest(request, session_id);
}

Coordination::OpNum TestKeeperTCPHandler::receiveRequest()
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
    int interrupt_fd  = poll_wrapper->getInterruptFD();
    if (opnum != Coordination::OpNum::Close)
    {
        auto promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
        zkutil::ResponseCallback callback = [interrupt_fd, promise] (const Coordination::ZooKeeperResponsePtr & response)
        {
            promise->set_value(response);
            UInt64 bytes = 1;
            [[maybe_unused]] int result = write(interrupt_fd, &bytes, sizeof(bytes));
        };

        if (request->has_watch)
        {
            auto watch_promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
            zkutil::ResponseCallback watch_callback = [interrupt_fd, watch_promise] (const Coordination::ZooKeeperResponsePtr & response)
            {
                watch_promise->set_value(response);
                UInt64 bytes = 1;
                [[maybe_unused]] int result = write(interrupt_fd, &bytes, sizeof(bytes));
            };
            test_keeper_storage->putRequest(request, session_id, callback, watch_callback);
            responses.push(promise->get_future());
            watch_responses.emplace_back(watch_promise->get_future());
        }
        else
        {
            test_keeper_storage->putRequest(request, session_id, callback);
            responses.push(promise->get_future());
        }
    }
    else
    {
        test_keeper_storage->putCloseRequest(request, session_id);
    }

    return opnum;
}


}
