#include <Storages/Kafka/Clients/BrokerConnection.h>

#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Sasl.h>
#include <Storages/Kafka/Clients/SaslAuthentication.h>

#include <Common/config_version.h>
#include <Common/logger_useful.h>

#include <algorithm>

namespace Kafka
{

namespace
{

/// Guard against garbage in the size prefix (e.g. talking to a TLS listener).
constexpr Int32 max_frame_size = 512 << 20;

}

BrokerConnection::BrokerConnection(std::unique_ptr<Net::Transport> transport_, ConnectionOptions options_)
    : transport(std::move(transport_))
    , options(std::move(options_))
{
}

void BrokerConnection::open(const String & host_, UInt16 port_, const Net::Deadline & deadline)
{
    host = host_;
    port = port_;
    poisoned = false;
    next_correlation_id = 1;
    broker_versions.clear();
    transport->connect(host_, port_, deadline);
    handshakeApiVersions(deadline);
    if (options.sasl)
        authenticateSasl(*options.sasl, deadline);
    LOG_DEBUG(options.log, "Connection {}:{}: opened ({} APIs advertised)", host, port, broker_versions.size());
}

void BrokerConnection::close()
{
    transport->close();
}

Int16 BrokerConnection::negotiateVersion(Protocol::ApiKey api, Int16 min_version, Int16 max_version) const
{
    const auto it = broker_versions.find(Protocol::toWire(api));
    if (it == broker_versions.end())
        throw Exception(ErrorKind::NotSupported, "Broker does not advertise {} at all", Protocol::apiKeyName(api));
    const auto [broker_min, broker_max] = it->second;
    const Int16 version = std::min(max_version, broker_max);
    if (version < min_version || version < broker_min)
        throw Exception(
            ErrorKind::NotSupported,
            "No common version for {}: we support [v{}, v{}], broker supports [v{}, v{}]",
            Protocol::apiKeyName(api),
            min_version,
            max_version,
            broker_min,
            broker_max);
    return version;
}

std::string_view BrokerConnection::exchangeFrame(std::string_view frame, Int32 correlation_id, const Net::Deadline & deadline)
{
    if (!transport->isOpen())
        throw Exception(ErrorKind::Connection, "Connection is not open");
    if (poisoned)
        throw Exception(ErrorKind::Connection, "Connection is poisoned by an earlier failure");
    poisoned = true;
    transport->writeAll(frame, deadline);
    const std::string_view payload = recvFrame(deadline);
    /// Peek the correlation id without consuming the payload.
    Protocol::Reader peek(payload);
    const Int32 got = peek.readInt32();
    if (got != correlation_id)
        throw Exception(ErrorKind::Protocol, "Correlation id mismatch: expected {}, got {}", correlation_id, got);
    poisoned = false;
    return payload;
}

std::string_view BrokerConnection::recvFrame(const Net::Deadline & deadline)
{
    char size_buf[4];
    readExact(size_buf, 4, deadline);
    Int32 size = 0;
    for (char c : size_buf)
        size = (size << 8) | static_cast<UInt8>(c);
    if (size < 0 || size > max_frame_size)
        throw Exception(ErrorKind::Protocol, "Invalid frame size {} — not a Kafka PLAINTEXT listener?", size);
    /// Grow-only reusable buffer: the zero-fill happens once per growth, not
    /// once per frame.
    if (recv_buffer.size() < static_cast<size_t>(size))
        recv_buffer.resize(static_cast<size_t>(size));
    readExact(recv_buffer.data(), static_cast<size_t>(size), deadline);
    return {recv_buffer.data(), static_cast<size_t>(size)};
}

std::string_view BrokerConnection::recvFrameResumable(const Net::Deadline & deadline)
{
    char size_buf[4];
    size_t got = 0;
    while (got < 4)
    {
        try
        {
            got += transport->readSome(size_buf + got, 4 - got, deadline);
        }
        catch (const Exception & e)
        {
            /// Nothing consumed yet: the frame boundary is intact, the caller
            /// may retry the receive later.
            if (got == 0 && (e.getKind() == ErrorKind::Timeout || e.getKind() == ErrorKind::Wakeup))
                poisoned = false;
            throw;
        }
    }
    Int32 size = 0;
    for (char c : size_buf)
        size = (size << 8) | static_cast<UInt8>(c);
    if (size < 0 || size > max_frame_size)
        throw Exception(ErrorKind::Protocol, "Invalid frame size {} — not a Kafka PLAINTEXT listener?", size);
    if (recv_buffer.size() < static_cast<size_t>(size))
        recv_buffer.resize(static_cast<size_t>(size));
    readExact(recv_buffer.data(), static_cast<size_t>(size), deadline);
    return {recv_buffer.data(), static_cast<size_t>(size)};
}

void BrokerConnection::readExact(char * data, size_t size, const Net::Deadline & deadline)
{
    size_t got = 0;
    while (got < size)
        got += transport->readSome(data + got, size - got, deadline);
}

void BrokerConnection::handshakeApiVersions(const Net::Deadline & deadline)
{
    /// Bootstrapping problem: the negotiation map does not exist yet, so the
    /// request is pinned (v3, supported by every 2.4+ broker).
    constexpr Int16 version = 3;
    Protocol::ApiVersionsRequest request;
    request.client_software_name = VERSION_NAME;
    request.client_software_version = VERSION_DESCRIBE;

    const std::string_view payload = roundTrip(
        Protocol::toWire(Protocol::ApiKey::ApiVersions),
        version,
        Protocol::ApiVersionsRequest::flexible(version),
        [&](Protocol::Writer & w) { request.encode(w, version); },
        deadline);
    Protocol::Reader r(payload);
    Protocol::ResponseHeader::decode(r, Protocol::ApiVersionsRequest::responseHeaderFlexible(version));
    const auto response = Protocol::ApiVersionsResponse::decode(r, version);

    if (response.error_code == BrokerError::UNSUPPORTED_VERSION)
        throw Exception(
            ErrorKind::NotSupported,
            "Broker rejected ApiVersions v3 — broker older than the Kafka 4.0 target of this client");
    if (response.error_code != BrokerError::NONE)
        throw Exception(BrokerCode{response.error_code}, "ApiVersions failed");

    for (const auto & range : response.api_keys)
        broker_versions[range.api_key] = {range.min_version, range.max_version};
}

void BrokerConnection::authenticateSasl(const SaslConfig & sasl, const Net::Deadline & deadline)
{
    const auto mechanism = createSaslMechanism(sasl, host);

    {
        Protocol::SaslHandshakeRequest handshake;
        handshake.mechanism = mechanism->name();
        const auto response = call(handshake, deadline);
        if (response.error_code != BrokerError::NONE)
        {
            String mechanisms;
            for (const auto & m : response.mechanisms)
                mechanisms += (mechanisms.empty() ? "" : ", ") + m;
            throw Exception(
                BrokerCode{response.error_code},
                "SASL handshake rejected {}; broker offers: [{}]",
                mechanism->name(),
                mechanisms);
        }
    }

    std::optional<Bytes> to_send = mechanism->firstMessage();
    Int64 session_lifetime_ms = 0;
    while (to_send)
    {
        Protocol::SaslAuthenticateRequest authenticate;
        authenticate.auth_bytes = std::move(*to_send);
        const auto response = call(authenticate, deadline);
        if (response.error_code != BrokerError::NONE)
            throw Exception(
                BrokerCode{response.error_code},
                "SASL/{} authentication failed{}",
                mechanism->name(),
                response.error_message ? ": " + *response.error_message : String{});
        session_lifetime_ms = response.session_lifetime_ms;
        to_send = mechanism->nextMessage(response.auth_bytes);
    }

    /// KIP-368: the broker kills the connection when the advertised session
    /// lifetime elapses without re-authentication. Mark the connection as
    /// expiring a little earlier; the pool then recycles it (a fresh
    /// connection re-authenticates) instead of re-authenticating in place.
    if (session_lifetime_ms > 0)
    {
        const auto margin = std::min<Int64>(session_lifetime_ms / 10, 10'000);
        auth_session_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(session_lifetime_ms - margin);
    }

    LOG_DEBUG(options.log, "Connection {}:{}: authenticated via SASL/{}", host, port, mechanism->name());
}

}
