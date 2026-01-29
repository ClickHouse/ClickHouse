#include <Server/KeeperTCPHandler.h>

#if USE_NURAFT

#    include <mutex>
#    include <Coordination/CoordinationSettings.h>
#    include <Coordination/FourLetterCommand.h>
#    include <Core/Types.h>
#    include <IO/CompressionMethod.h>
#    include <IO/ReadBufferFromFileDescriptor.h>
#    include <IO/ReadBufferFromPocoSocket.h>
#    include <IO/WriteBufferFromPocoSocket.h>
#    include <IO/LimitReadBuffer.h>
#    include <base/defines.h>
#    include <base/hex.h>
#    include <Poco/Net/NetException.h>
#    include <Poco/Util/AbstractConfiguration.h>
#    include <Common/CurrentThread.h>
#    include <Common/NetException.h>
#    include <Common/PipeFDs.h>
#    include <Common/Stopwatch.h>
#    include <Common/ZooKeeper/ZooKeeperCommon.h>
#    include <Common/ZooKeeper/ZooKeeperConstants.h>
#    include <Common/ZooKeeper/ZooKeeperIO.h>
#    include <Common/logger_useful.h>
#    include <Common/setThreadName.h>
#    include <Common/HistogramMetrics.h>

#    include <Compression/CompressionFactory.h>

#    include <boost/algorithm/string/trim.hpp>


#    ifdef POCO_HAVE_FD_EPOLL
#        include <sys/epoll.h>
#    else
#        include <poll.h>
#    endif

namespace ProfileEvents
{
    extern const Event KeeperTotalElapsedMicroseconds;
}

namespace HistogramMetrics
{
    extern Metric & KeeperServerQueueDuration;
    extern Metric & KeeperServerSendDuration;
}


namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 log_slow_connection_operation_threshold_ms;
    extern const CoordinationSettingsUInt64 log_slow_total_threshold_ms;
    extern const CoordinationSettingsUInt64 max_request_size;
    extern const CoordinationSettingsBool use_xid_64;
}

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
    extern const int BAD_ARGUMENTS;
    extern const int LIMIT_EXCEEDED;
    extern const int AUTHENTICATION_FAILED;
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
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot epoll_create");

        socket_event.events = EPOLLIN | EPOLLERR | EPOLLPRI;
        socket_event.data.fd = sockfd;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sockfd, &socket_event) < 0)
        {
            [[maybe_unused]] int err = ::close(epollfd);
            chassert(!err || errno == EINTR);

            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot insert socket into epoll queue");
        }
        pipe_event.events = EPOLLIN | EPOLLERR | EPOLLPRI;
        pipe_event.data.fd = pipe.fds_rw[0];
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, pipe.fds_rw[0], &pipe_event) < 0)
        {
            [[maybe_unused]] int err = ::close(epollfd);
            chassert(!err || errno == EINTR);

            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot insert socket into epoll queue");
        }
#endif
    }

    int getResponseFD() const
    {
        return pipe.fds_rw[1];
    }

    PollResult poll(Poco::Timespan remaining_time, const ReadBufferFromPocoSocket & in)
    {

        bool socket_ready = false;
        bool fd_ready = false;

        if (in.available() != 0)
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
                /// TODO: use epoll_pwait() for more precise timers
                rc = epoll_wait(epollfd, evout, 2, static_cast<int>(remaining_time.totalMilliseconds()));
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
                rc = ::poll(poll_buf, 2, static_cast<int>(remaining_time.totalMilliseconds()));
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

            if (rc >= 1)
            {
                if (poll_buf[0].revents & POLLIN)
                    socket_ready = true;
                if (poll_buf[1].revents & POLLIN)
                    fd_ready = true;
            }
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
        [[maybe_unused]] int err = ::close(epollfd);
        chassert(!err || errno == EINTR);
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
    , log(getLogger("KeeperTCPHandler"))
    , keeper_dispatcher(keeper_dispatcher_)
    , keeper_context(keeper_dispatcher->getKeeperContext())
    , min_session_timeout(config_ref.getInt64("keeper_server.coordination_settings.min_session_timeout_ms", Coordination::DEFAULT_MIN_SESSION_TIMEOUT_MS) * 1000)
    , max_session_timeout(config_ref.getInt64("keeper_server.coordination_settings.session_timeout_ms", Coordination::DEFAULT_MAX_SESSION_TIMEOUT_MS) * 1000)
    , poll_wrapper(std::make_shared<SocketInterruptablePollWrapper>(socket_))
    , send_timeout(send_timeout_)
    , receive_timeout(receive_timeout_)
    , responses(std::make_unique<ThreadSafeResponseQueue>(std::numeric_limits<size_t>::max()))
    , last_op(std::make_unique<LastOp>(EMPTY_LAST_OP))
{
    KeeperTCPHandler::registerConnection(this);
}

void KeeperTCPHandler::sendHandshake(bool has_leader, bool & use_compression)
{
    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, *out);
    if (has_leader)
    {
        if (use_xid_64)
            Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION_WITH_XID_64, *out);
        else if (use_compression)
            Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION_WITH_COMPRESSION, *out);
        else
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

Poco::Timespan KeeperTCPHandler::receiveHandshake(int32_t handshake_length, bool & use_compression)
{
    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout_ms;
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    std::array<char, Coordination::PASSWORD_LENGTH> passwd {};

    if (!isHandShake(handshake_length))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected handshake length received: {}", toString(handshake_length));

    Coordination::read(protocol_version, *in);

    if (protocol_version != Coordination::ZOOKEEPER_PROTOCOL_VERSION
        && protocol_version < Coordination::ZOOKEEPER_PROTOCOL_VERSION_WITH_COMPRESSION
        && protocol_version > Coordination::ZOOKEEPER_PROTOCOL_VERSION_WITH_XID_64)
    {
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected protocol version: {}", toString(protocol_version));
    }

    if (protocol_version == Coordination::ZOOKEEPER_PROTOCOL_VERSION_WITH_COMPRESSION)
    {
        use_compression = true;
    }
    else if (protocol_version >= Coordination::ZOOKEEPER_PROTOCOL_VERSION_WITH_XID_64)
    {
        if (!keeper_context->getCoordinationSettings()[CoordinationSetting::use_xid_64])
            throw Exception(
                ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "keeper_server.coordination_settings.use_xid_64 is set to 'false' while client has it enabled");
        close_xid = Coordination::CLOSE_XID_64;
        use_xid_64 = true;
        Coordination::read(use_compression, *in);
    }

    Coordination::read(last_zxid_seen, *in);
    Coordination::read(timeout_ms, *in);

    Coordination::read(previous_session_id, *in);
    Coordination::read(passwd, *in);


    auto auth_data = keeper_dispatcher->getAuthenticationData();
    if (auth_data)
    {
        String password{std::begin(passwd), std::end(passwd)};
        /// It was padded to Coordination::PASSWORD_LENGTH with '\0'
        boost::trim_right_if(password, [](char c) { return c == '\0'; });
        AuthenticationData client_auth_data(auth_data->getType());
        client_auth_data.setPassword(password, true);
        if (client_auth_data != *auth_data)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Wrong password specified, authentication failed");
    }

    int8_t readonly;
    if (handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY)
        Coordination::read(readonly, *in);

    return Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(timeout_ms) * 1000);
}


void KeeperTCPHandler::runImpl()
{
    DB::setThreadName(ThreadName::KEEPER_HANDLER);

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in.emplace(socket());
    out.emplace(socket());
    compressed_in.reset();
    compressed_out.reset();

    bool use_compression = false;

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
        connected.store(true, std::memory_order_relaxed);
        tryExecuteFourLetterWordCmd(four_letter_cmd);
        return;
    }

    try
    {
        int32_t handshake_length = header;
        auto client_timeout = receiveHandshake(handshake_length, use_compression);

        if (client_timeout.totalMilliseconds() == 0)
            client_timeout = Poco::Timespan(Coordination::DEFAULT_SESSION_TIMEOUT_MS * Poco::Timespan::MILLISECONDS);
        session_timeout = std::max(client_timeout, min_session_timeout);
        session_timeout = std::min(session_timeout, max_session_timeout);
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        LOG_WARNING(log, "Cannot receive handshake {}", e.displayText());
        return;
    }

    if (keeper_dispatcher->isServerActive())
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
            sendHandshake(/* has_leader */ false, use_compression);
            return;

        }

        sendHandshake(/* has_leader */ true, use_compression);
    }
    else
    {
        LOG_WARNING(log, "Ignoring user request, because the server is not active yet");
        sendHandshake(/* has_leader */ false, use_compression);
        return;
    }

    if (use_compression)
    {
        compressed_in.emplace(*in);
        compressed_out.emplace(*out, CompressionCodecFactory::instance().get("LZ4",{}));
    }

    max_request_size = static_cast<UInt64>(keeper_context->getCoordinationSettings()[CoordinationSetting::max_request_size]);

    auto response_callback = [my_responses = this->responses, my_poll_wrapper = this->poll_wrapper](
                                 const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)
    {
        if (!my_responses->push(RequestWithResponse{response, std::move(request)}))
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push response with xid {} and zxid {}", response->xid, response->zxid);

        UInt8 single_byte = 1;
        [[maybe_unused]] ssize_t result = write(my_poll_wrapper->getResponseFD(), &single_byte, sizeof(single_byte));
    };
    keeper_dispatcher->registerSession(session_id, response_callback);

    Stopwatch logging_stopwatch;
    auto operation_max_ms = keeper_context->getCoordinationSettings()[CoordinationSetting::log_slow_connection_operation_threshold_ms];
    auto log_long_operation = [&](const String & operation)
    {
        auto elapsed_ms = logging_stopwatch.elapsedMilliseconds();
        if (operation_max_ms < elapsed_ms)
            LOG_INFO(log, "{} for session {} took {} ms", operation, session_id, elapsed_ms);
        logging_stopwatch.restart();
    };

    session_stopwatch.start();
    connected.store(true, std::memory_order_release);
    bool close_received = false;

    try
    {
        while (true)
        {
            using namespace std::chrono_literals;

            PollResult result = poll_wrapper->poll(session_timeout, *in);
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
                RequestWithResponse request_with_response;

                if (!responses->tryPop(request_with_response))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "We must have ready response, but queue is empty. It's a bug.");

                auto dequeue_ts = std::chrono::steady_clock::now();

                auto & response = request_with_response.response;

                if (response->xid != Coordination::WATCH_XID && response->error != Coordination::Error::ZSESSIONEXPIRED)
                {
                    /// The same ZooKeeperWatchResponse pointer may be put into the `responses` queue multiple times (once for each session).
                    /// So, to avoid a data race between the producer and consumer threads accessing response->enqueue_ts,
                    /// we should just ignore watches in this thread.
                    chassert(response->enqueue_ts != std::chrono::steady_clock::time_point{});
                    HistogramMetrics::observe(
                        HistogramMetrics::KeeperServerQueueDuration,
                        std::chrono::duration_cast<std::chrono::milliseconds>(dequeue_ts - response->enqueue_ts).count());
                }

                log_long_operation("Waiting for response to be ready");

                if (response->xid == close_xid)
                {
                    LOG_DEBUG(log, "Session #{} successfully closed", session_id);
                    return;
                }

                updateStats(response, request_with_response.request);
                packageSent();

                {
                    Stopwatch watch;

                    response->write(getWriteBuffer(), use_xid_64);
                    flushWriteBuffer();

                    auto elapsed_ms = watch.elapsedMilliseconds();

                    HistogramMetrics::observe(HistogramMetrics::KeeperServerSendDuration, elapsed_ms);
                }

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
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Exception happened while reading from socket");

            if (session_stopwatch.elapsedMicroseconds() > static_cast<UInt64>(session_timeout.totalMicroseconds()))
            {
                LOG_DEBUG(log, "Session #{} expired", session_id);
                keeper_dispatcher->finishSession(session_id);
                break;
            }
        }
        finalizeWriteBuffer();
    }
    catch (const Exception & ex)
    {
        log_long_operation("Unknown operation");
        LOG_TRACE(log, "Has {} responses in the queue", responses->size());
        LOG_INFO(log, "Got exception processing session #{}: {}", session_id, getExceptionMessage(ex, true));
        cancelWriteBuffer();
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
    if (!FourLetterCommandFactory::instance().isEnabled(command))
    {
        LOG_WARNING(log, "Not enabled four letter command {}", IFourLetterCommand::toName(command));
        return false;
    }

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

WriteBuffer & KeeperTCPHandler::getWriteBuffer()
{
    if (compressed_out)
        return *compressed_out;
    return *out;
}

void KeeperTCPHandler::flushWriteBuffer()
{
    if (compressed_out)
        compressed_out->next();
    out->next();
}

void KeeperTCPHandler::finalizeWriteBuffer()
{
    if (compressed_out)
        compressed_out->finalize();
    out->finalize();
}

void KeeperTCPHandler::cancelWriteBuffer() noexcept
{
    if (compressed_out)
        compressed_out->cancel();
    if (out)
        out->cancel();
}

ReadBuffer & KeeperTCPHandler::getReadBuffer()
{
    if (compressed_in)
        return *compressed_in;
    return *in;
}

std::pair<Coordination::OpNum, Coordination::XID> KeeperTCPHandler::receiveRequest()
{
    std::optional<LimitReadBuffer> limited_buffer_holder;
    /// Wrap regular read buffer with LimitReadBuffer to apply max_request_size
    /// (this should be done on per-request basis)
    auto get_read_buffer_with_limit = [&]() -> ReadBuffer &
    {
        if (max_request_size == 0)
            return getReadBuffer();
        limited_buffer_holder.emplace(getReadBuffer(), LimitReadBuffer::Settings{.read_no_more = max_request_size, .expect_eof = false, .excetion_hint = "too long packet."});
        return *limited_buffer_holder;
    };
    auto & read_buffer = get_read_buffer_with_limit();
    int32_t length;
    Coordination::read(length, read_buffer);
    if (length < 0 || (max_request_size > 0 && static_cast<uint32_t>(length) > max_request_size))
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Request size {} is too big request (limit: {})", length, max_request_size);

    int64_t xid;
    if (use_xid_64)
        Coordination::read(xid, read_buffer);
    else
    {
        int32_t read_xid;
        Coordination::read(read_xid, read_buffer);
        xid = read_xid;
    }

    Coordination::OpNum opnum;
    Coordination::read(opnum, read_buffer);

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;

    auto request_validator = [&](const Coordination::ZooKeeperRequest & current_request)
    {
        if (!keeper_context->isOperationSupported(current_request.getOpNum()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported operation: {}, Path: '{}'", current_request.getOpNum(), current_request.getPath());
    };

    if (auto * multi_request = dynamic_cast<Coordination::ZooKeeperMultiRequest *>(request.get()))
    {
        multi_request->readImpl(read_buffer, request_validator);
    }
    else
    {
        request->readImpl(read_buffer);
        request_validator(*request);
    }


    if (!keeper_dispatcher->putRequest(request, session_id, use_xid_64))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", session_id);

    if (keeper_context->shouldLogRequests())
        LOG_TRACE(log, "Received request:\n{}", request->toString(/*short_format=*/true));

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

void KeeperTCPHandler::updateStats(Coordination::ZooKeeperResponsePtr & response, const Coordination::ZooKeeperRequestPtr & request)
{
    /// update statistics ignoring watch response and heartbeat.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() != Coordination::OpNum::Heartbeat)
    {
        Int64 elapsed = (Poco::Timestamp() - operations[response->xid]);
        ProfileEvents::increment(ProfileEvents::KeeperTotalElapsedMicroseconds, elapsed);
        Int64 elapsed_ms = elapsed / 1000;

        if (request && elapsed_ms > static_cast<Int64>(keeper_context->getCoordinationSettings()[CoordinationSetting::log_slow_total_threshold_ms]))
        {
            LOG_INFO(
                log,
                "Total time to process a request in session {} took too long ({}ms).\nRequest info:\n{}",
                session_id,
                elapsed_ms,
                request->toString(/*short_format=*/true));
        }

        conn_stats.updateLatency(elapsed_ms);

        operations.erase(response->xid);
        keeper_dispatcher->updateKeeperStatLatency(elapsed_ms);

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
    if (!connected.load(std::memory_order_acquire))
        return;

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
    cancelWriteBuffer();
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
