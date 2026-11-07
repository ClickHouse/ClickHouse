#pragma once

/// One TCP connection to one broker: i32 size-prefix framing, correlation-id
/// matching, ApiVersions-based version clamping, optional SASL/PLAIN.
///
/// One request in flight at a time (v1): every call() is a synchronous
/// request/response round trip on the caller's thread. Depth-1 pipelining for
/// prefetch is the designed v1.x extension.

#include <Storages/Kafka/Clients/Config.h>
#include <Storages/Kafka/Clients/Net/Transport.h>
#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <base/types.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace Kafka
{

struct ConnectionOptions
{
    String client_id = "ClickHouse";
    std::optional<SaslConfig> sasl;
    /// Never null: the connection logs through it unconditionally.
    LoggerPtr log = getLogger("KafkaClient");
};

class BrokerConnection
{
public:
    BrokerConnection(std::unique_ptr<Net::Transport> transport_, ConnectionOptions options_);

    /// Connect + ApiVersions handshake (+ SASL when configured).
    void open(const String & host_, UInt16 port_, const Net::Deadline & deadline);
    void close();
    bool isOpen() const { return transport && transport->isOpen() && !poisoned; }

    /// After any throw during a call the connection state on the wire is
    /// unknown (half-written frame, unread response). It must not be reused.
    bool isPoisoned() const { return poisoned; }

    /// Number of pipelined requests whose responses were not received yet.
    /// A connection with outstanding responses must not serve other callers.
    size_t getOutstanding() const { return outstanding; }

    /// Highest version of [min,max] the broker also supports; throws
    /// NotSupported when the ranges are disjoint.
    Int16 negotiateVersion(Protocol::ApiKey api, Int16 min_version, Int16 max_version) const;

    /// Fire-and-forget request (Produce with acks=0: the broker never
    /// responds). The correlation id is consumed; the connection stays usable.
    template <typename Req>
    void callNoResponse(const Req & request, const Net::Deadline & deadline)
    {
        if (!transport->isOpen())
            throw Exception(ErrorKind::Connection, "Connection is not open");
        if (poisoned)
            throw Exception(ErrorKind::Connection, "Connection is poisoned by an earlier failure");
        const Int16 version = negotiateVersion(Req::api_key, Req::min_version, Req::max_version);
        const Int32 correlation_id = next_correlation_id++;
        const String frame = Protocol::buildFrame([&](Protocol::Writer & w)
        {
            Protocol::RequestHeader header;
            header.api_key = Protocol::toWire(Req::api_key);
            header.api_version = version;
            header.correlation_id = correlation_id;
            header.client_id = options.client_id;
            header.flexible = Req::flexible(version);
            header.encode(w);
            request.encode(w, version);
        });
        poisoned = true;
        transport->writeAll(frame, deadline);
        poisoned = false;
    }

    /// One pipelined in-flight request. Responses arrive in request order on a
    /// Kafka connection, so receiveResponse must be called with the OLDEST
    /// pending entry first.
    struct PendingResponse
    {
        Int32 correlation_id = 0;
        Int16 api_version = 0;
        bool flexible_response_header = true;
    };

    /// Pipelined send: write the request frame, do not wait for the response.
    template <typename Req>
    PendingResponse sendRequest(const Req & request, const Net::Deadline & deadline)
    {
        if (!transport->isOpen())
            throw Exception(ErrorKind::Connection, "Connection is not open");
        if (poisoned)
            throw Exception(ErrorKind::Connection, "Connection is poisoned by an earlier failure");
        const Int16 version = negotiateVersion(Req::api_key, Req::min_version, Req::max_version);
        const Int32 correlation_id = next_correlation_id++;
        const String frame = Protocol::buildFrame([&](Protocol::Writer & w)
        {
            Protocol::RequestHeader header;
            header.api_key = Protocol::toWire(Req::api_key);
            header.api_version = version;
            header.correlation_id = correlation_id;
            header.client_id = options.client_id;
            header.flexible = Req::flexible(version);
            header.encode(w);
            request.encode(w, version);
        });
        poisoned = true;
        transport->writeAll(frame, deadline);
        poisoned = false;
        ++outstanding;
        return {correlation_id, version, Req::responseHeaderFlexible(version)};
    }

    /// True when a response frame (or EOF/error) is buffered and a receive
    /// would not block.
    bool responseAvailable() { return transport->isOpen() && !poisoned && transport->readable(); }

    /// Receive the response for the oldest pending request. A deadline that
    /// expires before the first byte of the frame arrives leaves the
    /// connection clean (resumable); any later failure poisons it.
    template <typename Req>
    typename Req::Response receiveResponse(const PendingResponse & pending, const Net::Deadline & deadline)
    {
        if (poisoned)
            throw Exception(ErrorKind::Connection, "Connection is poisoned by an earlier failure");
        poisoned = true;
        const std::string_view payload = recvFrameResumable(deadline);
        Protocol::Reader r(payload);
        const auto header = Protocol::ResponseHeader::decode(r, pending.flexible_response_header);
        if (header.correlation_id != pending.correlation_id)
            Protocol::throwProtocol(
                fmt::format("Correlation id mismatch: expected {}, got {}", pending.correlation_id, header.correlation_id));
        auto response = Req::Response::decode(r, pending.api_version);
        if (!r.empty())
            Protocol::throwProtocol("Trailing bytes after response body");
        poisoned = false;
        --outstanding;
        return response;
    }

    /// Synchronous request/response with version clamping.
    ///
    /// Lifetime note: the response is decoded from the connection's reusable
    /// receive buffer. Most responses copy what they keep, but zero-copy
    /// fields (Fetch/Produce record blobs, typed string_view) stay views into
    /// that buffer — valid until the next request on this connection.
    template <typename Req>
    typename Req::Response call(const Req & request, const Net::Deadline & deadline)
    {
        if (outstanding > 0)
            throw Exception(
                ErrorKind::Connection, "Connection has pipelined responses outstanding; synchronous call is not allowed");
        const Int16 version = negotiateVersion(Req::api_key, Req::min_version, Req::max_version);
        const std::string_view payload = roundTrip(
            Protocol::toWire(Req::api_key),
            version,
            Req::flexible(version),
            [&](Protocol::Writer & w) { request.encode(w, version); },
            deadline);
        Protocol::Reader r(payload);
        Protocol::ResponseHeader::decode(r, Req::responseHeaderFlexible(version));
        auto response = Req::Response::decode(r, version);
        if (!r.empty())
            Protocol::throwProtocol("Trailing bytes after response body");
        return response;
    }

    const String & getHost() const { return host; }
    UInt16 getPort() const { return port; }

    /// KIP-368: true when the broker-advertised SASL session lifetime is about
    /// to elapse. The broker kills the connection at the deadline, so pools
    /// must recycle it beforehand (a fresh connection re-authenticates).
    bool isAuthSessionExpiring() const
    {
        return auth_session_deadline && std::chrono::steady_clock::now() >= *auth_session_deadline;
    }

private:
    template <typename FillBody>
    std::string_view roundTrip(Int16 api_key, Int16 api_version, bool flexible, FillBody && fill, const Net::Deadline & deadline)
    {
        const Int32 correlation_id = next_correlation_id++;
        const String frame = Protocol::buildFrame([&](Protocol::Writer & w)
        {
            Protocol::RequestHeader header;
            header.api_key = api_key;
            header.api_version = api_version;
            header.correlation_id = correlation_id;
            header.client_id = options.client_id;
            header.flexible = flexible;
            header.encode(w);
            fill(w);
        });
        return exchangeFrame(frame, correlation_id, deadline);
    }

    /// Send one frame, receive one frame into the reusable buffer, verify the
    /// correlation id (peeked, not consumed). Marks the connection poisoned on
    /// any failure. The returned view is valid until the next exchange.
    std::string_view exchangeFrame(std::string_view frame, Int32 correlation_id, const Net::Deadline & deadline);
    std::string_view recvFrame(const Net::Deadline & deadline);
    /// Like recvFrame, but a timeout before the first byte unpoisons the
    /// connection and rethrows (safe to retry the receive later).
    std::string_view recvFrameResumable(const Net::Deadline & deadline);
    void readExact(char * data, size_t size, const Net::Deadline & deadline);

    void handshakeApiVersions(const Net::Deadline & deadline);
    void authenticateSasl(const SaslConfig & sasl, const Net::Deadline & deadline);

    std::unique_ptr<Net::Transport> transport;
    ConnectionOptions options;
    String host;
    UInt16 port = 0;
    bool poisoned = false;
    size_t outstanding = 0;
    Int32 next_correlation_id = 1;
    /// Reused across responses: grows to the largest frame seen, never pays
    /// the zero-fill again afterwards (an 8 MB memset per fetch otherwise).
    String recv_buffer;
    /// api_key -> [min, max] advertised by the broker.
    std::unordered_map<Int16, std::pair<Int16, Int16>> broker_versions;
    /// KIP-368 re-authentication deadline (with a safety margin), when the
    /// broker advertised a bounded SASL session.
    std::optional<std::chrono::steady_clock::time_point> auth_session_deadline;
};

}
