#pragma once

#include <map>
#include <time.h>
#include <base/types.h>
#include <Common/HTTPFieldLess.h>
#include <Common/OpenTelemetryTracingContext.h>

namespace Poco::Net
{
    class HTTPRequest;
    class SocketAddress;
}

namespace DB
{

class WriteBuffer;
class ReadBuffer;


/** Information about client for query.
  * Some fields are passed explicitly from client and some are calculated automatically.
  *
  * Contains info about initial query source, for tracing distributed queries
  *  (where one query initiates many other queries).
  */
class ClientInfo
{
public:
    enum class Interface : uint8_t
    {
        TCP = 1,
        HTTP = 2,
        GRPC = 3,
        MYSQL = 4,
        POSTGRESQL = 5,
        LOCAL = 6,
        TCP_INTERSERVER = 7,
        PROMETHEUS = 8,
        BACKGROUND = 9, // e.g. queries from refreshable materialized views
        ARROW_FLIGHT = 10,
    };

    enum class HTTPMethod : uint8_t
    {
        UNKNOWN = 0,
        GET     = 1,
        POST    = 2,
        OPTIONS = 3
    };

    enum class QueryKind : uint8_t
    {
        NO_QUERY = 0,            /// Uninitialized object.
        INITIAL_QUERY = 1,
        SECONDARY_QUERY = 2,    /// Query that was initiated by another query for distributed query execution.
    };

    ClientInfo();

    QueryKind query_kind = QueryKind::NO_QUERY;

    std::shared_ptr<Poco::Net::SocketAddress> connection_address;

    /// Current values are not serialized, because it is passed separately.
    String current_user;
    String current_query_id;
    std::shared_ptr<Poco::Net::SocketAddress> current_address;

    /// For IMPERSONATEd session, stores the original authenticated user
    String authenticated_user;

    /// When query_kind == INITIAL_QUERY, these values are equal to current.
    String initial_user;
    String initial_query_id;
    std::shared_ptr<Poco::Net::SocketAddress> initial_address;
    time_t initial_query_start_time{};
    Decimal64 initial_query_start_time_microseconds{};

    /// OpenTelemetry trace context we received from client, or which we are going to send to server.
    OpenTelemetry::TracingContext client_trace_context;

    /// All below are parameters related to initial query.

    Interface interface = Interface::TCP;
    bool is_secure = false;
    String certificate;

    /// For tcp
    String os_user;
    String client_hostname;
    String client_name;
    /// Canonical id of the AI coding agent that invoked the client (e.g. `claude-code`, `cursor`),
    /// detected from environment variables. Empty when no agent is detected.
    String client_agent;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    UInt32 client_tcp_protocol_version = 0;

    /// Numbers are starting from 1. Zero means unset.
    UInt32 script_query_number = 0;
    UInt32 script_line_number = 0;

    /// In case of distributed query, client info for query is actually a client info of client.
    /// In order to get a version of server-initiator, use connection_ values.
    /// Also for tcp only.
    UInt64 connection_client_version_major = 0;
    UInt64 connection_client_version_minor = 0;
    UInt64 connection_client_version_patch = 0;
    UInt32 connection_tcp_protocol_version = 0;
    /// Parallel-replicas protocol version negotiated with the immediate upstream connection on
    /// hello. Populated locally by `TCPHandler` from its own `client_parallel_replicas_protocol_version`
    /// member — NOT serialized to the wire. A follower uses this to recognise whether its
    /// initiator can speak features bumped in newer parallel-replicas protocol versions
    /// (e.g. announcement-response in `DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_ANNOUNCEMENT_RESPONSE`)
    /// and degrade gracefully when it can't. 0 means "unknown / pre-versioning".
    UInt32 connection_parallel_replicas_protocol_version = 0;

    /// For http
    HTTPMethod http_method = HTTPMethod::UNKNOWN;
    String http_user_agent;
    String http_referer;
    std::map<String, String, HTTPFieldLess> http_headers;

    /// For mysql and postgresql
    UInt64 connection_id = 0;

    /// For interserver in case initial query transport was authenticated via JWT.
    String jwt;

    /// Comma separated list of forwarded IP addresses (from X-Forwarded-For for HTTP interface).
    /// It's expected that proxy appends the forwarded address to the end of the list.
    /// The element can be trusted only if you trust the corresponding proxy.
    /// NOTE This field can also be reused in future for TCP interface with PROXY v1/v2 protocols.
    String forwarded_for;
    std::optional<Poco::Net::SocketAddress> getLastForwardedFor() const;
    String getLastForwardedForHost() const;

    /// Common
    String quota_key;

    UInt64 distributed_depth = 0;

    bool is_replicated_database_internal = false;
    bool is_shared_catalog_internal = false;
    /// Server-internal query (not user-issued), propagated to remote queries.
    /// Independent of `query_kind == SECONDARY_QUERY`: either can hold without the other.
    bool is_internal = false;

    /// For parallel processing on replicas
    bool collaborate_with_initiator{false};
    UInt64 obsolete_count_participating_replicas{0};
    UInt64 number_of_current_replica{0};

    bool empty() const { return query_kind == QueryKind::NO_QUERY; }

    /** Serialization and deserialization.
      * Only values that are not calculated automatically or passed separately are serialized.
      * Revisions are passed to use format that server will understand or client was used.
      */
    /// `with_trailing_fields` controls whether the `client_agent` and `is_internal` fields are (de)serialized as
    /// trailing members of `ClientInfo`. It must be `false` for the embedded `ClientInfo` of the persisted async
    /// `Distributed` insert header, where `client_agent` and `is_internal` are stored as trailing header fields
    /// instead, so that older binaries draining newer queue files can read the header without misinterpreting it.
    void write(WriteBuffer & out, UInt64 server_protocol_revision, bool with_trailing_fields = true) const;
    void read(ReadBuffer & in, UInt64 client_protocol_revision, bool with_trailing_fields = true);

    /// Initialize parameters on client initiating query.
    void setInitialQuery();

    /// Initialize parameters related to HTTP request.
    void setFromHTTPRequest(const Poco::Net::HTTPRequest & request);

    bool clientVersionEquals(const ClientInfo & other, bool compare_patch) const;

    String getVersionStr() const;

private:
    void fillOSUserHostNameAndVersionInfo();
};

String toString(ClientInfo::Interface interface);
String toString(ClientInfo::HTTPMethod method);
}
