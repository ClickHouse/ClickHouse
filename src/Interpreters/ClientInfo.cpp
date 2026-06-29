#include <Core/ProtocolDefines.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <base/getFQDNOrHostName.h>
#include <Common/StringUtils.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>

#include <Common/config_version.h>

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Parse a string read off the native protocol wire into an "ip:port" SocketAddress WITHOUT resolving.
/// ClientInfo::write always serializes the address as an IP literal plus a numeric port ("ip:port" or
/// "[ipv6]:port", via SocketAddress::toString), but the value is attacker-controlled and may be
/// corrupted or desynced. Poco's SocketAddress(String) constructor would resolve a non-numeric port
/// via getservbyname() and a non-IP host via DNS::hostByName() - both reach a non-reentrant libc
/// resolver family that base/harmful traps to an uncatchable SIGILL in debug/sanitizer builds. We split
/// host and port exactly as Poco::Net::SocketAddress::init() does, require a numeric port <= 65535 and a
/// host that parses as an IP literal, and build the address directly from the parsed IPAddress. Returns
/// nullopt for anything else (empty, a leading-'/' UNIX-local form, a non-numeric/out-of-range port, or
/// a non-IP host), so the caller decides whether to reject it or fall back to a default - never resolving.
std::optional<Poco::Net::SocketAddress> tryParseIpEndpointFromWire(const String & host_and_port)
{
    /// A leading '/' makes Poco build a UNIX_LOCAL address, whose host()/port() throw later.
    if (host_and_port.empty() || host_and_port.front() == '/')
        return {};

    std::string_view host;
    size_t port_pos = String::npos;
    if (host_and_port.front() == '[')
    {
        /// "[ipv6]:port" - Poco requires ':' immediately after the closing ']'. The host token
        /// for IPAddress::tryParse is the address between the brackets (unbracketed).
        const auto closing_bracket = host_and_port.find(']');
        if (closing_bracket == String::npos)
            return {};
        host = std::string_view(host_and_port).substr(1, closing_bracket - 1);
        if (closing_bracket + 1 < host_and_port.size() && host_and_port[closing_bracket + 1] == ':')
            port_pos = closing_bracket + 2;
    }
    else
    {
        /// "host:port" - Poco splits on the first ':'.
        const auto colon = host_and_port.find(':');
        if (colon != String::npos)
        {
            host = std::string_view(host_and_port).substr(0, colon);
            port_pos = colon + 1;
        }
    }

    const std::string_view port
        = port_pos == String::npos ? std::string_view{} : std::string_view(host_and_port).substr(port_pos);
    if (port.empty())
        return {};

    UInt32 port_number = 0;
    for (const char c : port)
    {
        if (!isNumericASCII(c))
            return {};
        port_number = port_number * 10 + static_cast<UInt32>(c - '0');
        if (port_number > 0xFFFF)
            return {};
    }

    Poco::Net::IPAddress ip;
    if (!Poco::Net::IPAddress::tryParse(std::string(host), ip))
        return {};

    return Poco::Net::SocketAddress(ip, static_cast<UInt16>(port_number));
}

/// Detect whether the client (clickhouse-client or clickhouse-local) is being invoked under a known
/// AI coding agent, by inspecting environment variables that these agents set for the processes they
/// spawn. Returns the canonical agent id, or an empty string when no agent is detected.
/// Only environment variables are inspected; no filesystem probing is performed.
String detectClientAgent()
{
    /// The presence of a specific marker variable maps to a canonical agent id.
    static constexpr std::pair<const char *, std::string_view> agent_env_markers[] =
    {
        {"CLAUDECODE", "claude-code"},
        {"CLAUDE_CODE", "claude-code"},
        {"CURSOR_TRACE_ID", "cursor"},
        {"CURSOR_AGENT", "cursor-cli"},
        {"GEMINI_CLI", "gemini-cli"},
        {"CODEX_SANDBOX", "codex"},
        {"CODEX_CI", "codex"},
        {"CODEX_THREAD_ID", "codex"},
        {"ANTIGRAVITY_AGENT", "antigravity"},
        {"AUGMENT_AGENT", "augment"},
        {"CLINE_ACTIVE", "cline"},
        {"OPENCODE_CLIENT", "opencode"},
        {"TRAE_AI_SHELL_ID", "trae"},
        {"GOOSE_TERMINAL", "goose"},
        {"REPL_ID", "replit"},
        {"COPILOT_MODEL", "github-copilot"},
        {"COPILOT_ALLOW_ALL", "github-copilot"},
        {"COPILOT_GITHUB_TOKEN", "github-copilot"},
    };

    for (const auto & [env_name, agent_id] : agent_env_markers)
        if (nullptr != std::getenv(env_name)) // NOLINT(concurrency-mt-unsafe)
            return String(agent_id);

    /// Cursor CLI also identifies itself via a role marker that must have a specific value.
    if (const char * cursor_role = std::getenv("CURSOR_EXTENSION_HOST_ROLE"); // NOLINT(concurrency-mt-unsafe)
        cursor_role != nullptr && 0 == std::strcmp(cursor_role, "agent-exec"))
        return "cursor-cli";

    /// Generic convention: any tool may advertise itself via the standard AGENT environment variable.
    if (const char * generic_agent = std::getenv("AGENT"); // NOLINT(concurrency-mt-unsafe)
        generic_agent != nullptr && generic_agent[0] != '\0')
        return String(generic_agent);

    return {};
}

}

ClientInfo::ClientInfo()
{
    connection_address = std::make_shared<Poco::Net::SocketAddress>();
    current_address = std::make_shared<Poco::Net::SocketAddress>();
    initial_address = std::make_shared<Poco::Net::SocketAddress>();
}

std::optional<Poco::Net::SocketAddress> ClientInfo::getLastForwardedFor() const
{
    if (forwarded_for.empty())
        return {};
    String last = forwarded_for.substr(forwarded_for.find_last_of(',') + 1);
    boost::trim(last);

    /// IPv6 address with port
    if (last[0] == '[')
        return Poco::Net::SocketAddress{Poco::Net::AddressFamily::IPv6, last};

    const auto colons = std::count(last.begin(), last.end(), ':');

    /// IPv6 address without port
    if (colons > 1)
        return Poco::Net::SocketAddress{Poco::Net::AddressFamily::IPv6, last, 0};

    /// IPv4 address with port
    if (colons == 1)
        return Poco::Net::SocketAddress{Poco::Net::AddressFamily::IPv4, last};

    /// IPv4 address without port
    return Poco::Net::SocketAddress{Poco::Net::AddressFamily::IPv4, last, 0};
}

String ClientInfo::getLastForwardedForHost() const
{
    auto addr = getLastForwardedFor();
    return addr ? addr->host().toString() : "";
}


void ClientInfo::write(WriteBuffer & out, UInt64 server_protocol_revision, bool with_client_agent) const
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method ClientInfo::write is called for unsupported server revision");

    writeBinary(static_cast<UInt8>(query_kind), out);
    if (empty())
        return;

    writeBinary(initial_user, out);
    writeBinary(initial_query_id, out);
    writeBinary(initial_address->toString(), out);

    if (server_protocol_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME)
        writeBinary(initial_query_start_time_microseconds, out);

    writeBinary(static_cast<UInt8>(interface), out);

    if (interface == Interface::TCP)
    {
        writeBinary(os_user, out);
        writeBinary(client_hostname, out);
        writeBinary(client_name, out);
        writeVarUInt(client_version_major, out);
        writeVarUInt(client_version_minor, out);
        writeVarUInt(client_tcp_protocol_version, out);
    }
    else if (interface == Interface::HTTP)
    {
        writeBinary(static_cast<UInt8>(http_method), out);
        writeBinary(http_user_agent, out);

        if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_X_FORWARDED_FOR_IN_CLIENT_INFO)
            writeBinary(forwarded_for, out);

        if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_REFERER_IN_CLIENT_INFO)
            writeBinary(http_referer, out);
    }

    if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
        writeBinary(quota_key, out);

    if (server_protocol_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH)
        writeVarUInt(distributed_depth, out);

    if (interface == Interface::TCP)
    {
        if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
            writeVarUInt(client_version_patch, out);
    }

    if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY)
    {
        if (client_trace_context.trace_id != UUID())
        {
            // Have OpenTelemetry header.
            writeBinary(uint8_t(1), out);
            // No point writing these numbers with variable length, because they
            // are random and will probably require the full length anyway.
            writeBinary(client_trace_context.trace_id, out);
            writeBinary(client_trace_context.span_id, out);
            writeBinary(client_trace_context.tracestate, out);
            writeBinary(client_trace_context.trace_flags, out);
        }
        else
        {
            // Don't have OpenTelemetry header.
            writeBinary(static_cast<UInt8>(0), out);
        }
    }

    if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS)
    {
        writeVarUInt(static_cast<UInt64>(collaborate_with_initiator), out);
        writeVarUInt(obsolete_count_participating_replicas, out);
        writeVarUInt(number_of_current_replica, out);
    }

    if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS)
    {
        writeVarUInt(script_query_number, out);
        writeVarUInt(script_line_number, out);
    }

    if (server_protocol_revision >= DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER)
    {
        if (!jwt.empty())
        {
            writeBinary(static_cast<UInt8>(1), out);
            writeBinary(jwt, out);
        }
        else
            writeBinary(static_cast<UInt8>(0), out);
    }

    /// Sent for all interfaces (not only TCP): the detected client agent must also be preserved
    /// when a clickhouse-local query (LOCAL interface) is forwarded to remote shards.
    /// Skipped for the embedded `ClientInfo` of the persisted async `Distributed` insert header
    /// (see `with_client_agent` in the declaration), where it is stored as a trailing header field.
    if (with_client_agent && server_protocol_revision >= DBMS_MIN_REVISION_WITH_CLIENT_AGENT_IN_CLIENT_INFO)
        writeBinary(client_agent, out);
}


void ClientInfo::read(ReadBuffer & in, UInt64 client_protocol_revision, bool with_client_agent)
{
    if (client_protocol_revision < DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method ClientInfo::read is called for unsupported client revision");

    UInt8 read_query_kind = 0;
    readBinary(read_query_kind, in);
    query_kind = QueryKind(read_query_kind);
    if (empty())
        return;

    readBinary(initial_user, in);
    readBinary(initial_query_id, in);

    String initial_address_string;
    readBinary(initial_address_string, in);
    /// The wire address must never reach Poco's resolver (getservbyname/DNS, trapped to SIGILL). For a
    /// SECONDARY_QUERY the value is consumed verbatim (system.query_log, interserver authenticate), so a
    /// non-"ip:port" form is corrupted input and is rejected as INCORRECT_DATA. For an INITIAL_QUERY the
    /// server overwrites initial_address with the real peer address in Session::makeQueryContextImpl, so
    /// the wire value is discarded; to stay compatible with the pre-validation native protocol (which
    /// documented a generic host:port) we accept it leniently and fall back to a default endpoint when it
    /// is not a plain IP literal, instead of rejecting otherwise-valid initiating clients.
    auto parsed_address = tryParseIpEndpointFromWire(initial_address_string);
    if (!parsed_address && query_kind == QueryKind::SECONDARY_QUERY)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Malformed initial_address received over the network: expected an IP literal with a numeric port");
    initial_address = std::make_shared<Poco::Net::SocketAddress>(parsed_address.value_or(Poco::Net::SocketAddress{}));

    if (client_protocol_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME)
    {
        readBinary(initial_query_start_time_microseconds, in);
        initial_query_start_time = initial_query_start_time_microseconds / 1000000;
    }

    UInt8 read_interface = 0;
    readBinary(read_interface, in);
    interface = Interface(read_interface);

    if (interface == Interface::TCP)
    {
        readBinary(os_user, in);
        readBinary(client_hostname, in);
        readBinary(client_name, in);
        readVarUInt(client_version_major, in);
        readVarUInt(client_version_minor, in);
        readVarUInt(client_tcp_protocol_version, in);
    }
    else if (interface == Interface::HTTP)
    {
        UInt8 read_http_method = 0;
        readBinary(read_http_method, in);
        http_method = HTTPMethod(read_http_method);

        readBinary(http_user_agent, in);

        if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_X_FORWARDED_FOR_IN_CLIENT_INFO)
            readBinary(forwarded_for, in);

        if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_REFERER_IN_CLIENT_INFO)
            readBinary(http_referer, in);
    }

    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
        readBinary(quota_key, in);

    if (client_protocol_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH)
        readVarUInt(distributed_depth, in);

    if (interface == Interface::TCP)
    {
        if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
            readVarUInt(client_version_patch, in);
        else
            client_version_patch = client_tcp_protocol_version;
    }

    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY)
    {
        uint8_t have_trace_id = 0;
        readBinary(have_trace_id, in);
        if (have_trace_id)
        {
            readBinary(client_trace_context.trace_id, in);
            readBinary(client_trace_context.span_id, in);
            readBinary(client_trace_context.tracestate, in);
            readBinary(client_trace_context.trace_flags, in);
        }
    }

    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS)
    {
        UInt64 value = 0;
        readVarUInt(value, in);
        collaborate_with_initiator = static_cast<bool>(value);
        readVarUInt(obsolete_count_participating_replicas, in);
        readVarUInt(number_of_current_replica, in);
    }

    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS)
    {
        readVarUInt(script_query_number, in);
        readVarUInt(script_line_number, in);
    }

    if (client_protocol_revision >= DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER)
    {
        UInt8 have_jwt = 0;
        readBinary(have_jwt, in);
        if (have_jwt)
            readBinary(jwt, in);
    }

    if (with_client_agent && client_protocol_revision >= DBMS_MIN_REVISION_WITH_CLIENT_AGENT_IN_CLIENT_INFO)
        readBinary(client_agent, in);
}


void ClientInfo::setInitialQuery()
{
    query_kind = QueryKind::INITIAL_QUERY;
    fillOSUserHostNameAndVersionInfo();
    if (client_name.empty())
        client_name = VERSION_NAME;
    else
        client_name = std::string(VERSION_NAME) + " " + client_name;
}

bool ClientInfo::clientVersionEquals(const ClientInfo & other, bool compare_patch) const
{
    bool patch_equals = compare_patch ? client_version_patch == other.client_version_patch : true;
    return client_version_major == other.client_version_major &&
           client_version_minor == other.client_version_minor &&
           patch_equals &&
           client_tcp_protocol_version == other.client_tcp_protocol_version;
}

String ClientInfo::getVersionStr() const
{
    return fmt::format("{}.{}.{} ({})", client_version_major, client_version_minor, client_version_patch, client_tcp_protocol_version);
}

void ClientInfo::fillOSUserHostNameAndVersionInfo()
{
    os_user.resize(256, '\0');
    if (0 == getlogin_r(os_user.data(), static_cast<int>(os_user.size() - 1)))
        os_user.resize(strlen(os_user.c_str()));
    else
        os_user.clear();    /// Don't mind if we cannot determine user login.

    client_hostname = getFQDNOrHostName();

    client_agent = detectClientAgent();

    client_version_major = VERSION_MAJOR;
    client_version_minor = VERSION_MINOR;
    client_version_patch = VERSION_PATCH;
    client_tcp_protocol_version = DBMS_TCP_PROTOCOL_VERSION;
}

String toString(ClientInfo::Interface interface)
{
    switch (interface)
    {
        case ClientInfo::Interface::TCP:
            return "TCP";
        case ClientInfo::Interface::HTTP:
            return "HTTP";
        case ClientInfo::Interface::GRPC:
            return "GRPC";
        case ClientInfo::Interface::MYSQL:
            return "MYSQL";
        case ClientInfo::Interface::POSTGRESQL:
            return "POSTGRESQL";
        case ClientInfo::Interface::LOCAL:
            return "LOCAL";
        case ClientInfo::Interface::TCP_INTERSERVER:
            return "TCP_INTERSERVER";
        case ClientInfo::Interface::PROMETHEUS:
            return "PROMETHEUS";
        case ClientInfo::Interface::BACKGROUND:
            return "BACKGROUND";
        case ClientInfo::Interface::ARROW_FLIGHT:
            return "ARROWFLIGHT";
    }

    return fmt::format("Unknown server interface ({}).", static_cast<int>(interface));
}

void ClientInfo::setFromHTTPRequest(const Poco::Net::HTTPRequest & request)
{
    http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;

    http_user_agent = request.get("User-Agent", "");
    http_referer = request.get("Referer", "");
    forwarded_for = request.get("X-Forwarded-For", "");

    for (const auto & header : request)
    {
        /// These headers can contain authentication info and shouldn't be accessible by the user.
        String key_lowercase = Poco::toLower(header.first);
        if (key_lowercase.starts_with("x-clickhouse") || key_lowercase == "authentication")
            continue;
        http_headers[header.first] = header.second;
    }
}

String toString(ClientInfo::HTTPMethod method)
{
    switch (method)
    {
        case ClientInfo::HTTPMethod::UNKNOWN:
            return "UNKNOWN";
        case ClientInfo::HTTPMethod::GET:
            return "GET";
        case ClientInfo::HTTPMethod::POST:
            return "POST";
        case ClientInfo::HTTPMethod::OPTIONS:
            return "OPTIONS";
    }
}

}
