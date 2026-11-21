#include <Storages/Pulsar/Client/Connection.h>

#if USE_PULSAR

#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <base/scope_guard.h>

#include <Poco/Net/IPAddress.h>
#include <Poco/Net/NetException.h>

#if USE_SSL
#include <Poco/Net/SecureStreamSocket.h>
#include <openssl/x509v3.h>
#endif

#include <fmt/format.h>

#include <poll.h>
#include <sys/socket.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CANNOT_CONNECT_PULSAR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace Pulsar
{

namespace
{

/// The protocol version 20 adds `TOPIC_MIGRATED`; with 19 the broker closes the consumers and producers instead.
constexpr Int32 PROTOCOL_VERSION = 19;
constexpr size_t READ_CHUNK_SIZE = 64 * 1024;

String stripBrackets(const String & host)
{
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']')
        return host.substr(1, host.size() - 2);
    return host;
}

}

void throwUnexpected(const char * what)
{
    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Unexpected frame from the Pulsar broker: {}", what);
}


void Interrupter::interrupt() noexcept
{
    std::lock_guard lock(mutex);
    interrupted = true;
    for (int fd : fds)
        ::shutdown(fd, SHUT_RDWR);
}

bool Interrupter::isInterrupted() const
{
    std::lock_guard lock(mutex);
    return interrupted;
}

void Interrupter::add(int fd)
{
    std::lock_guard lock(mutex);
    fds.push_back(fd);
    if (interrupted)
        ::shutdown(fd, SHUT_RDWR);
}

void Interrupter::remove(int fd)
{
    std::lock_guard lock(mutex);
    std::erase(fds, fd);
}


String BrokerAddress::toString() const
{
    String res;
    for (const auto & [host, port] : hosts)
        res += fmt::format("{}{}:{}", res.empty() ? "" : ",", host, port);
    if (!proxy_to_broker_url.empty())
        res += fmt::format(" (proxy to {})", proxy_to_broker_url);
    return res;
}


Connection::Connection(ClientState & client_, BrokerAddress address_, Interrupter * owner_interrupter_)
    : client(client_)
    , broker_address(std::move(address_))
    , owner_interrupter(owner_interrupter_)
    , serial(client.next_serial++)
    , limits(client.limits)
    , log(client.log)
{
    last_write_progress = last_received = step_started = Clock::now();
    startNextAddress();
}

Connection::~Connection()
{
    unregisterSocket();
}

void Connection::unregisterSocket()
{
    if (registered_fd < 0)
        return;
    client.interrupter.remove(registered_fd);
    if (owner_interrupter)
        owner_interrupter->remove(registered_fd);
    registered_fd = -1;
}

void Connection::startNextAddress()
{
    while (true)
    {
        unregisterSocket();
        socket.reset();

        if (address_index < addresses.size())
        {
            const auto & address = addresses[address_index++];
            try
            {
                if (client.service.tls)
                {
#if USE_SSL
                    /// The default client context, configured in the `openSSL.client` section.
                    auto secure_socket = std::make_unique<Poco::Net::SecureStreamSocket>();
                    secure_socket->setPeerHostName(host);
                    socket = std::move(secure_socket);
#else
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "TLS connections to Pulsar are disabled, because ClickHouse was built without SSL support");
#endif
                }
                else
                {
                    socket = std::make_unique<Poco::Net::StreamSocket>();
                }
                socket->connectNB(address);
                registered_fd = socket->impl()->sockfd();
                client.interrupter.add(registered_fd);
                if (owner_interrupter)
                    owner_interrupter->add(registered_fd);
                current_state = State::TCPConnecting;
                step_started = Clock::now();
                LOG_TRACE(log, "Connecting to {} ({})", broker_address.toString(), address.toString());
                return;
            }
            catch (const Exception &)
            {
                throw;
            }
            catch (const Poco::Exception & e)
            {
                last_error = fmt::format("{}: {}", address.toString(), e.displayText());
                continue;
            }
        }

        if (host_index >= broker_address.hosts.size())
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot connect to the Pulsar broker {}: {}", broker_address.toString(), last_error);

        const auto & [name, port] = broker_address.hosts[host_index++];
        /// Rejects by throwing.
        if (client.configuration.connection_validator)
            client.configuration.connection_validator(name, port);
        host = stripBrackets(name);
        addresses.clear();
        address_index = 0;
        try
        {
            auto resolved = DNSResolver::instance().resolveAddressList(host, port);
            addresses.assign(resolved.begin(), resolved.end());
            if (addresses.empty())
                last_error = fmt::format("{} has no addresses", name);
        }
        catch (const Exception & e)
        {
            last_error = e.message();
        }
        catch (const Poco::Exception & e)
        {
            last_error = e.displayText();
        }
    }
}

void Connection::onTCPConnected()
{
    pollfd poll_fd{.fd = registered_fd, .events = POLLOUT, .revents = 0};
    int res = ::poll(&poll_fd, 1, 0);
    if (res <= 0)
    {
        if (Clock::now() - step_started > client.configuration.connection_timeout)
        {
            last_error = fmt::format("timeout of {} ms", client.configuration.connection_timeout.count());
            startNextAddress();
        }
        return;
    }

    int error = socket->impl()->socketError();
    if (error)
    {
        last_error = errnoToString(error);
        startNextAddress();
        return;
    }

    socket->setNoDelay(true);
    step_started = Clock::now();
    if (client.service.tls)
        current_state = State::TlsHandshake;
    else
        sendConnect();
}

void Connection::verifyPeerHostName()
{
#if USE_SSL
    /// Poco checks the host name only with extended verification and not for loopback hosts, so check it here.
    auto & secure_socket = static_cast<Poco::Net::SecureStreamSocket &>(*socket);
    X509 * certificate = secure_socket.peerCertificate();
    if (!certificate)
        throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The Pulsar broker {} did not present a certificate", host);
    SCOPE_EXIT(X509_free(certificate));

    Poco::Net::IPAddress ip;
    bool matches = Poco::Net::IPAddress::tryParse(host, ip)
        ? X509_check_ip_asc(certificate, host.c_str(), 0) == 1
        : X509_check_host(certificate, host.data(), host.size(), 0, nullptr) == 1;
    if (!matches)
        throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The certificate of the Pulsar broker does not match its host name {}", host);
#endif
}

void Connection::sendConnect()
{
    PulsarProto::BaseCommand command;
    command.set_type(PulsarProto::BaseCommand::CONNECT);
    auto & connect = *command.mutable_connect();
    connect.set_client_version(fmt::format("ClickHouse {}", VERSION_STRING));
    connect.set_protocol_version(PROTOCOL_VERSION);
    connect.mutable_feature_flags();
    if (!broker_address.proxy_to_broker_url.empty())
        connect.set_proxy_to_broker_url(broker_address.proxy_to_broker_url);
    output.emplace_back(encodeCommand(command));
    current_state = State::ConnectSent;
    step_started = Clock::now();
}

void Connection::send(const PulsarProto::BaseCommand & command)
{
    sendFrame(encodeCommand(command));
}

void Connection::sendFrame(StringWithMemoryTracking bytes)
{
    if (current_state == State::Closed)
        return;
    if (output.empty())
        last_write_progress = Clock::now();
    output.emplace_back(std::move(bytes));
}

void Connection::markClosed(const String & reason)
{
    if (current_state == State::Closed)
        return;
    LOG_INFO(log, "The connection to the Pulsar broker {} is closed: {}", broker_address.toString(), reason);
    current_state = State::Closed;
    close();
}

void Connection::close() noexcept
{
    if (!socket)
        return;
    try
    {
        if (current_state == State::Ready || current_state == State::ConnectSent)
            processOutput();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: best effort, the broker redelivers the unacknowledged messages anyway.
    }
    current_state = State::Closed;
    output.clear();
    input.clear();
    input_begin = 0;
    unregisterSocket();
    try
    {
        socket->close();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: the descriptor is released by the destructor anyway.
    }
    socket.reset();
}

void Connection::processOutput()
{
    while (!output.empty())
    {
        auto & front = output.front();
        int to_send = static_cast<int>(std::min<size_t>(front.size() - output_offset, std::numeric_limits<int>::max()));
        int sent = socket->sendBytes(front.data() + output_offset, to_send);
        if (sent <= 0)
        {
            /// Would block. For TLS, the same bytes are sent again later, as OpenSSL requires.
#if USE_SSL
            tls_wants_write = sent == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE;
#endif
            return;
        }
        last_write_progress = Clock::now();
        output_offset += sent;
        if (output_offset == front.size())
        {
            output.pop_front();
            output_offset = 0;
        }
    }
    tls_wants_write = false;
}

void Connection::processInput(FrameHandler & handler)
{
    if (read_buffer.empty())
        read_buffer.resize(READ_CHUNK_SIZE);
    while (current_state != State::Closed)
    {
        /// The size of every frame is checked before the buffer grows beyond one chunk.
        while (input.size() - input_begin >= FRAME_SIZE_LENGTH)
        {
            size_t size = decodeFrameSize(input.data() + input_begin, limits);
            if (input.size() - input_begin - FRAME_SIZE_LENGTH < size)
                break;
            std::string_view body(input.data() + input_begin + FRAME_SIZE_LENGTH, size);
            input_begin += FRAME_SIZE_LENGTH + size;
            handleFrame(handler, body);
            if (current_state == State::Closed)
                return;
        }
        if (input_begin > 0)
        {
            input.erase(0, input_begin);
            input_begin = 0;
        }

        int received = socket->receiveBytes(read_buffer.data(), static_cast<int>(read_buffer.size()));
        if (received == 0)
        {
            markClosed("closed by the broker");
            return;
        }
        if (received < 0)
        {
#if USE_SSL
            if (received == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE)
                tls_wants_write = true;
#endif
            return;
        }
        input.append(read_buffer.data(), received);
        last_received = Clock::now();
        ping_sent.reset();
    }
}

void Connection::handleFrame(FrameHandler & handler, std::string_view body)
{
    decodeFrame(body, limits, frame);
    if (!frame.command.has_type())
    {
        LOG_TRACE(log, "Ignoring a command of an unknown type from {}", broker_address.toString());
        return;
    }

    using Type = PulsarProto::BaseCommand;
    switch (frame.command.type())
    {
        case Type::CONNECTED:
        {
            if (current_state != State::ConnectSent || !frame.command.has_connected())
                throwUnexpected("CONNECTED");
            const auto & connected = frame.command.connected();
            if (connected.has_max_message_size())
            {
                if (connected.max_message_size() <= 0)
                    throwUnexpected("CONNECTED with a non-positive max_message_size");
                limits.max_message_size = std::min<size_t>(connected.max_message_size(), MAX_ALLOWED_MESSAGE_SIZE);
            }
            current_state = State::Ready;
            LOG_TRACE(log, "Connected to {}", broker_address.toString());
            return;
        }
        case Type::PING:
        {
            PulsarProto::BaseCommand pong;
            pong.set_type(Type::PONG);
            pong.mutable_pong();
            send(pong);
            return;
        }
        case Type::PONG:
            return;
        case Type::AUTH_CHALLENGE:
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The Pulsar broker {} requires authentication, which is not supported", broker_address.toString());
        case Type::ERROR:
            if (current_state == State::ConnectSent)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The Pulsar broker {} rejected the connection: {}",
                    broker_address.toString(), frame.command.has_error() ? frame.command.error().message() : "");
            break;
        default:
            break;
    }

    if (current_state != State::Ready)
        throwUnexpected("a command before CONNECTED");

    if (frame.command.type() == Type::MESSAGE)
    {
        /// The payload of the message is kept without copying again.
        auto buffer = std::make_shared<StringWithMemoryTracking>(body);
        frame.tail = std::string_view(*buffer).substr(body.size() - frame.tail.size());
        handler.onFrame(*this, frame, buffer);
        return;
    }
    handler.onFrame(*this, frame, nullptr);
}

void Connection::checkTimeouts()
{
    const auto now = Clock::now();
    const auto operation_timeout = client.configuration.operation_timeout;
    const auto keep_alive = client.configuration.keep_alive_interval;
    if ((current_state == State::TlsHandshake || current_state == State::ConnectSent) && now - step_started > operation_timeout)
        throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Connecting to the Pulsar broker {} did not complete within {} ms",
            broker_address.toString(), operation_timeout.count());

    if (current_state != State::Ready)
        return;

    if (!output.empty() && now - last_write_progress > operation_timeout)
    {
        markClosed(fmt::format("the broker did not read for {} ms", operation_timeout.count()));
        return;
    }
    if (ping_sent)
    {
        if (now - *ping_sent > keep_alive)
            markClosed(fmt::format("no response to PING within {} ms", keep_alive.count()));
    }
    else if (now - last_received > keep_alive)
    {
        PulsarProto::BaseCommand ping;
        ping.set_type(PulsarProto::BaseCommand::PING);
        ping.mutable_ping();
        send(ping);
        ping_sent = now;
    }
}

void Connection::process(FrameHandler & handler)
{
    if (current_state == State::Closed)
        return;

    checkTimeouts();

    if (current_state == State::TCPConnecting)
        onTCPConnected();

    if (current_state == State::TlsHandshake)
    {
#if USE_SSL
        int res = 0;
        try
        {
            res = static_cast<Poco::Net::SecureStreamSocket &>(*socket).completeHandshake();
        }
        catch (const Exception &)
        {
            throw;
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "TLS handshake with the Pulsar broker {} failed: {}",
                broker_address.toString(), e.displayText());
        }
        if (res == 1)
        {
            verifyPeerHostName();
            sendConnect();
        }
        else if (res == 0)
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The Pulsar broker {} closed the connection during the TLS handshake", broker_address.toString());
        else
        {
            tls_wants_write = res == Poco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE;
            return;
        }
#endif
    }

    if (current_state != State::ConnectSent && current_state != State::Ready)
        return;

    try
    {
        processOutput();
        processInput(handler);
        if (current_state != State::Closed)
            processOutput();
    }
    catch (const Exception &)
    {
        /// Protocol violations.
        throw;
    }
    catch (const Poco::Exception & e)
    {
        /// Network errors.
        if (current_state != State::Ready)
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot connect to the Pulsar broker {}: {}", broker_address.toString(), e.displayText());
        markClosed(e.displayText());
    }
}

int Connection::fd() const
{
    return registered_fd;
}

Int16 Connection::pollEvents() const
{
    switch (current_state)
    {
        case State::TCPConnecting:
            return POLLOUT;
        case State::TlsHandshake:
            return tls_wants_write ? POLLOUT : POLLIN;
        case State::ConnectSent:
        case State::Ready:
            return static_cast<Int16>(POLLIN | ((output.empty() && !tls_wants_write) ? 0 : POLLOUT));
        case State::Closed:
            return 0;
    }
}

bool Connection::hasBufferedInput() const
{
    if (!socket || !client.service.tls || (current_state != State::ConnectSent && current_state != State::Ready))
        return false;
    try
    {
        return socket->impl()->available() > 0;
    }
    catch (...)
    {
        /// Ok: the next read reports the error.
        return false;
    }
}

TimePoint Connection::wakeUpTime() const
{
    const auto & configuration = client.configuration;
    switch (current_state)
    {
        case State::TCPConnecting:
            return step_started + configuration.connection_timeout;
        case State::TlsHandshake:
        case State::ConnectSent:
            return step_started + configuration.operation_timeout;
        case State::Ready:
        {
            TimePoint res = ping_sent ? *ping_sent + configuration.keep_alive_interval : last_received + configuration.keep_alive_interval;
            if (!output.empty())
                res = std::min(res, last_write_progress + configuration.operation_timeout);
            return res;
        }
        case State::Closed:
            return TimePoint::max();
    }
}


Session::Session(ClientStatePtr client_, Owner & owner_, Interrupter * owner_interrupter_)
    : client_state(std::move(client_))
    , owner(owner_)
    , owner_interrupter(owner_interrupter_)
    , log(client_state->log)
{
}

Session::~Session()
{
    close();
}

Session::RequestPtr Session::lookup(const String & topic)
{
    auto request = std::make_shared<Request>();
    request->topic = topic;
    request->is_lookup = true;
    request->started = Clock::now();
    requests_to_send.push_back(request);
    return request;
}

Session::RequestPtr Session::partitionedMetadata(const String & topic)
{
    auto request = std::make_shared<Request>();
    request->topic = topic;
    request->started = Clock::now();
    requests_to_send.push_back(request);
    return request;
}

UInt64 Session::nextRequestId()
{
    return next_request_id++;
}

Connection & Session::connection(const BrokerAddress & address)
{
    for (auto & connection : connections)
        if (!connection->isClosed() && connection->address() == address)
            return *connection;
    connections.push_back(std::make_unique<Connection>(*client_state, address, owner_interrupter));
    return *connections.back();
}

Connection * Session::findConnection(UInt64 id)
{
    for (auto & connection : connections)
        if (connection->id() == id)
            return connection.get();
    return nullptr;
}

Connection & Session::serviceConnection()
{
    for (auto & connection : connections)
        if (connection->id() == service_connection_id && !connection->isClosed())
            return *connection;

    /// Every new service connection starts from the next host.
    const auto & hosts = client_state->service.hosts;
    BrokerAddress address;
    size_t start = client_state->next_service_host++;
    for (size_t i = 0; i < hosts.size(); ++i)
        address.hosts.push_back(hosts[(start + i) % hosts.size()]);
    Connection & res = connection(address);
    service_connection_id = res.id();
    return res;
}

BrokerAddress Session::parseBrokerUrl(const String & url) const
{
    ServiceUrl parsed;
    try
    {
        parsed = parseServiceUrl(url);
    }
    catch (const Exception & e)
    {
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Invalid broker URL in a Pulsar lookup response: {}", e.message());
    }
    if (parsed.hosts.size() != 1 || parsed.tls != client_state->service.tls)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Unexpected broker URL '{}' in a Pulsar lookup response", url);
    BrokerAddress res;
    res.hosts = std::move(parsed.hosts);
    return res;
}

void Session::sendRequest(Request & request)
{
    Connection & connection = request.target.hosts.empty() ? serviceConnection() : this->connection(request.target);
    if (!connection.isReady())
        return;

    request.request_id = nextRequestId();
    request.connection_id = connection.id();
    PulsarProto::BaseCommand command;
    if (request.is_lookup)
    {
        command.set_type(PulsarProto::BaseCommand::LOOKUP);
        auto & lookup = *command.mutable_lookuptopic();
        lookup.set_topic(request.topic);
        lookup.set_request_id(request.request_id);
        lookup.set_authoritative(request.authoritative);
    }
    else
    {
        command.set_type(PulsarProto::BaseCommand::PARTITIONED_METADATA);
        auto & metadata = *command.mutable_partitionmetadata();
        metadata.set_topic(request.topic);
        metadata.set_request_id(request.request_id);
    }
    connection.send(command);
}

void Session::completeLookup(const RequestPtr & request_ptr, const PulsarProto::CommandLookupTopicResponse & response)
{
    Request & request = *request_ptr;
    using Response = PulsarProto::CommandLookupTopicResponse;
    const bool tls = client_state->service.tls;
    if (!response.has_response() || response.response() == Response::Failed)
    {
        request.done = true;
        request.error = fmt::format("lookup of topic {} failed: {} {}", request.topic,
            response.has_error() ? PulsarProto::ServerError_Name(response.error()) : "", response.message());
        return;
    }

    if ((tls && !response.has_brokerserviceurltls()) || (!tls && !response.has_brokerserviceurl()))
    {
        request.done = true;
        request.error = fmt::format("lookup of topic {} returned no {} URL", request.topic, tls ? "TLS" : "broker");
        return;
    }
    const String & url = tls ? response.brokerserviceurltls() : response.brokerserviceurl();

    BrokerAddress broker;
    if (response.proxy_through_service_url())
    {
        broker.hosts = client_state->service.hosts;
        broker.proxy_to_broker_url = url;
    }
    else
        broker = parseBrokerUrl(url);

    if (response.response() == Response::Connect)
    {
        request.done = true;
        request.broker = std::move(broker);
        return;
    }

    /// Redirect.
    if (++request.redirects > MAX_LOOKUP_REDIRECTS)
        throwUnexpected("too many lookup redirects");
    request.target = std::move(broker);
    request.authoritative = response.authoritative();
    request.started = Clock::now();
    request.connection_id = 0;
    requests_to_send.push_back(request_ptr);
}

void Session::onFrame(Connection & connection, Frame & frame, const SharedBuffer & buffer)
{
    using Type = PulsarProto::BaseCommand;
    const auto & command = frame.command;

    auto take_request = [&](UInt64 request_id) -> RequestPtr
    {
        auto it = requests_in_flight.find(request_id);
        if (it == requests_in_flight.end() || it->second->connection_id != connection.id())
            return nullptr;
        RequestPtr request = it->second;
        requests_in_flight.erase(it);
        return request;
    };

    switch (command.type())
    {
        case Type::LOOKUP_RESPONSE:
        {
            if (!command.has_lookuptopicresponse() || !command.lookuptopicresponse().has_request_id())
                throwUnexpected("LOOKUP_RESPONSE");
            const auto & response = command.lookuptopicresponse();
            RequestPtr request = take_request(response.request_id());
            if (!request || !request->is_lookup)
                throwUnexpected("LOOKUP_RESPONSE for an unknown request");
            if (!request->done)
                completeLookup(request, response);
            return;
        }
        case Type::PARTITIONED_METADATA_RESPONSE:
        {
            if (!command.has_partitionmetadataresponse() || !command.partitionmetadataresponse().has_request_id())
                throwUnexpected("PARTITIONED_METADATA_RESPONSE");
            const auto & response = command.partitionmetadataresponse();
            RequestPtr request = take_request(response.request_id());
            if (!request || request->is_lookup)
                throwUnexpected("PARTITIONED_METADATA_RESPONSE for an unknown request");
            if (request->done)
                return;
            request->done = true;
            if (response.has_response() && response.response() == PulsarProto::CommandPartitionedTopicMetadataResponse::Failed)
                request->error = fmt::format("getting the partitions of topic {} failed: {} {}", request->topic,
                    response.has_error() ? PulsarProto::ServerError_Name(response.error()) : "", response.message());
            else
                request->partitions = response.partitions();
            return;
        }
        case Type::ERROR:
        {
            if (!command.has_error() || !command.error().has_request_id())
                throwUnexpected("ERROR");
            if (RequestPtr request = take_request(command.error().request_id()))
            {
                if (!request->done)
                {
                    request->done = true;
                    request->error = fmt::format("request for topic {} failed: {} {}", request->topic,
                        PulsarProto::ServerError_Name(command.error().error()), command.error().message());
                }
                return;
            }
            break;
        }
        default:
            break;
    }
    owner.onFrame(connection, frame, buffer);
}

void Session::sendRequests()
{
    auto to_send = std::move(requests_to_send);
    requests_to_send.clear();
    for (auto & request : to_send)
    {
        if (request->done)
            continue;
        if (request->connection_id == 0)
            sendRequest(*request);
        if (request->connection_id != 0)
            requests_in_flight.emplace(request->request_id, request);
        else
            requests_to_send.push_back(request);
    }
}

void Session::process()
{
    checkInterrupted();

    /// New connections are created here, before processing them all.
    sendRequests();
    for (auto & connection : connections)
        connection->process(*this);
    /// On the connections that became ready. The output is sent by the next call, `wait` does not wait for it.
    sendRequests();

    for (size_t i = 0; i < connections.size();)
    {
        Connection & connection = *connections[i];
        if (!connection.isClosed())
        {
            ++i;
            continue;
        }
        owner.onConnectionClosed(connection);
        /// The requests on a lost connection start again on the service.
        for (auto it = requests_in_flight.begin(); it != requests_in_flight.end();)
        {
            if (it->second->connection_id == connection.id())
            {
                Request & request = *it->second;
                request.connection_id = 0;
                request.target = {};
                request.redirects = 0;
                request.authoritative = false;
                if (!request.done)
                    requests_to_send.push_back(it->second);
                it = requests_in_flight.erase(it);
            }
            else
                ++it;
        }
        connections.erase(connections.begin() + i);
    }

    /// The requests that did not complete in time fail. A late response to them is ignored.
    const auto now = Clock::now();
    const auto timeout = client_state->configuration.operation_timeout;
    auto expire = [&](Request & request)
    {
        if (!request.done && now - request.started > timeout)
        {
            request.done = true;
            request.error = fmt::format("{} of topic {} did not complete within {} ms",
                request.is_lookup ? "lookup" : "getting the partitions", request.topic, timeout.count());
        }
    };
    for (auto & [_, request] : requests_in_flight)
        expire(*request);
    for (auto & request : requests_to_send)
        expire(*request);
    std::erase_if(requests_to_send, [](const RequestPtr & request) { return request->done; });

    checkInterrupted();
}

void Session::wait(TimePoint deadline)
{
    checkInterrupted();

    const auto now = Clock::now();
    /// Interruption shuts the sockets down, which wakes up `poll`; waking up regularly is only a safety net.
    TimePoint until = std::min(deadline, now + std::chrono::seconds(1));
    VectorWithMemoryTracking<pollfd> fds;
    for (const auto & connection : connections)
    {
        until = std::min(until, connection->wakeUpTime());
        if (connection->hasBufferedInput())
            until = now;
        if (connection->fd() >= 0 && connection->pollEvents())
            fds.push_back({.fd = connection->fd(), .events = connection->pollEvents(), .revents = 0});
    }
    for (const auto & [_, request] : requests_in_flight)
        if (!request->done)
            until = std::min(until, request->started + client_state->configuration.operation_timeout);
    for (const auto & request : requests_to_send)
        until = std::min(until, request->started + client_state->configuration.operation_timeout);

    if (until <= now)
        return;
    auto timeout_ms = std::chrono::ceil<std::chrono::milliseconds>(until - now).count();
    [[maybe_unused]] int res = ::poll(fds.data(), fds.size(), static_cast<int>(timeout_ms));
}

bool Session::hasPendingOutput() const
{
    for (const auto & connection : connections)
        if (connection->hasPendingOutput())
            return true;
    return false;
}

void Session::checkInterrupted() const
{
    if (client_state->interrupter.isInterrupted())
        throw Exception(ErrorCodes::ABORTED, "The Pulsar client is closed");
    if (owner_interrupter && owner_interrupter->isInterrupted())
        throw Exception(ErrorCodes::ABORTED, "The Pulsar producer is interrupted");
}

void Session::close() noexcept
{
    for (auto & connection : connections)
        connection->close();
    connections.clear();
    requests_in_flight.clear();
    requests_to_send.clear();
}

}

}

#endif
