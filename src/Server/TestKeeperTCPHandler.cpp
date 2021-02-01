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
#include <IO/ReadBufferFromFileDescriptor.h>

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
    extern const int LOGICAL_ERROR;
}

struct PollResult
{
    std::vector<size_t> ready_responses;
    bool has_requests;
    bool error;
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

        socket_event.events = EPOLLIN | EPOLLERR;
        socket_event.data.fd = sockfd;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sockfd, &socket_event) < 0)
        {
            ::close(epollfd);
            throwFromErrno("Cannot insert socket into epoll queue", ErrorCodes::SYSTEM_ERROR);
        }
        pipe_event.events = EPOLLIN | EPOLLERR;
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

    PollResult poll(Poco::Timespan remaining_time)
    {
        std::array<int, 2> outputs = {-1, -1};
#if defined(POCO_HAVE_FD_EPOLL)
        int rc;
        epoll_event evout[2];
        memset(evout, 0, sizeof(evout));
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

        if (rc >= 1 && evout[0].events & EPOLLIN)
            outputs[0] = evout[0].data.fd;
        if (rc == 2 && evout[1].events & EPOLLIN)
            outputs[1] = evout[1].data.fd;
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
        if (rc >= 1 && poll_buf[0].revents & POLLIN)
            outputs[0] = sockfd;
        if (rc == 2 && poll_buf[1].revents & POLLIN)
            outputs[1] = pipe.fds_rw[0];
#endif

        PollResult result{};
        if (rc < 0)
        {
            result.error = true;
            return result;
        }
        else if (rc == 0)
        {
            return result;
        }
        else
        {
            for (auto fd : outputs)
            {
                if (fd != -1)
                {
                    if (fd == sockfd)
                        result.has_requests = true;
                    else
                    {
                        do
                        {
                            size_t response_position;
                            readIntBinary(response_position, response_in);
                            result.ready_responses.push_back(response_position);
                        } while (response_in.available());
                    }
                }
            }
        }
        return result;
    }

#if defined(POCO_HAVE_FD_EPOLL)
    ~SocketInterruptablePollWrapper()
    {
        ::close(epollfd);
    }
#endif
};

TestKeeperTCPHandler::TestKeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , log(&Poco::Logger::get("TestKeeperTCPHandler"))
    , global_context(server.context())
    , test_keeper_storage(global_context.getTestKeeperStorage())
    , operation_timeout(0, global_context.getConfigRef().getUInt("test_keeper_server.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , session_timeout(0, global_context.getConfigRef().getUInt("test_keeper_server.session_timeout_ms", Coordination::DEFAULT_SESSION_TIMEOUT_MS) * 1000)
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
    if (handshake_length != Coordination::CLIENT_HANDSHAKE_LENGTH && handshake_length != Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY)
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

    int8_t readonly;
    if (handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY)
        Coordination::read(readonly, *in);
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
    bool close_received = false;
    try
    {
        while (true)
        {
            using namespace std::chrono_literals;

            PollResult result = poll_wrapper->poll(session_timeout);
            if (result.has_requests)
            {
                do
                {
                    Coordination::OpNum received_op = receiveRequest();

                    if (received_op == Coordination::OpNum::Close)
                    {
                        auto last_response = responses.find(response_id_counter - 1);
                        if (last_response == responses.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Just inserted response #{} not found in responses", response_id_counter - 1);
                        LOG_DEBUG(log, "Received close request for session #{}", session_id);
                        if (last_response->second.wait_for(std::chrono::microseconds(operation_timeout.totalMicroseconds())) != std::future_status::ready)
                        {
                            LOG_DEBUG(log, "Cannot sent close for session #{}", session_id);
                        }
                        else
                        {
                            LOG_DEBUG(log, "Sent close for session #{}", session_id);
                            last_response->second.get()->write(*out);
                        }

                        close_received = true;

                        break;
                    }
                    else if (received_op == Coordination::OpNum::Heartbeat)
                    {
                        LOG_TRACE(log, "Received heartbeat for session #{}", session_id);
                        session_stopwatch.restart();
                    }
                }
                while (in->available());
            }

            if (close_received)
                break;

            for (size_t response_id : result.ready_responses)
            {
                auto response_future = responses.find(response_id);
                if (response_future == responses.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get unknown response #{}", response_id);

                if (response_future->second.wait_for(0s) != std::future_status::ready)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Response #{} was market as ready but corresponding future not ready yet", response_id);

                auto response = response_future->second.get();
                if (response->error == Coordination::Error::ZOK)
                {
                    response->write(*out);
                }
                else
                {
                    /// TODO Get rid of this
                    if (!dynamic_cast<Coordination::ZooKeeperWatchResponse *>(response.get()))
                        response->write(*out);
                }
                responses.erase(response_future);
            }

            if (result.error)
                throw Exception("Exception happened while reading from socket", ErrorCodes::SYSTEM_ERROR);

            if (session_stopwatch.elapsedMicroseconds() > static_cast<UInt64>(session_timeout.totalMicroseconds()))
            {
                LOG_DEBUG(log, "Session #{} expired", session_id);
                auto response = putCloseRequest();
                if (response.wait_for(std::chrono::microseconds(operation_timeout.totalMicroseconds())) != std::future_status::ready)
                    LOG_DEBUG(log, "Cannot sent close for expired session #{}", session_id);
                else
                    response.get()->write(*out);

                break;
            }
        }
    }
    catch (const Exception & ex)
    {
        LOG_INFO(log, "Got exception processing session #{}: {}", session_id, getExceptionMessage(ex, true));
        auto response = putCloseRequest();
        if (response.wait_for(std::chrono::microseconds(operation_timeout.totalMicroseconds())) != std::future_status::ready)
            LOG_DEBUG(log, "Cannot sent close for session #{}", session_id);
        else
            response.get()->write(*out);
    }

}

zkutil::TestKeeperStorage::AsyncResponse TestKeeperTCPHandler::putCloseRequest()
{
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
    request->xid = Coordination::CLOSE_XID;
    auto promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
    zkutil::ResponseCallback callback = [promise] (const Coordination::ZooKeeperResponsePtr & response)
    {
        promise->set_value(response);
    };
    test_keeper_storage->putRequest(request, session_id, callback);
    return promise->get_future();
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
    auto promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
    if (opnum != Coordination::OpNum::Close)
    {
        int response_fd = poll_wrapper->getResponseFD();
        size_t response_num = response_id_counter++;
        zkutil::ResponseCallback callback = [response_fd, promise, response_num] (const Coordination::ZooKeeperResponsePtr & response)
        {
            promise->set_value(response);
            [[maybe_unused]] int result = write(response_fd, &response_num, sizeof(response_num));
        };

        if (request->has_watch)
        {
            auto watch_promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
            size_t watch_response_num = response_id_counter++;
            zkutil::ResponseCallback watch_callback = [response_fd, watch_promise, watch_response_num] (const Coordination::ZooKeeperResponsePtr & response)
            {
                watch_promise->set_value(response);
                [[maybe_unused]] int result = write(response_fd, &watch_response_num, sizeof(watch_response_num));
            };
            test_keeper_storage->putRequest(request, session_id, callback, watch_callback);
            responses.try_emplace(response_num, promise->get_future());
            responses.try_emplace(watch_response_num, watch_promise->get_future());
        }
        else
        {
            test_keeper_storage->putRequest(request, session_id, callback);
            responses.try_emplace(response_num, promise->get_future());
        }
    }
    else
    {
        zkutil::ResponseCallback callback = [promise] (const Coordination::ZooKeeperResponsePtr & response)
        {
            promise->set_value(response);
        };
        test_keeper_storage->putRequest(request, session_id, callback);
        responses.try_emplace(response_id_counter++, promise->get_future());
    }

    return opnum;
}

}
