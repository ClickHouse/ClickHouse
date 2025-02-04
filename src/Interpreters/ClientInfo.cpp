#include <Interpreters/ClientInfo.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Net/HTTPRequest.h>
#include <unistd.h>

#include <Common/config_version.h>

#include <format>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ClientInfo::write(WriteBuffer & out, UInt64 server_protocol_revision) const
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method ClientInfo::write is called for unsupported server revision");

    writeBinary(static_cast<UInt8>(query_kind), out);
    if (empty())
        return;

    writeBinary(initial_user, out);
    writeBinary(initial_query_id, out);
    writeBinary(initial_address.toString(), out);

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
}


void ClientInfo::read(ReadBuffer & in, UInt64 client_protocol_revision)
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
    initial_address = Poco::Net::SocketAddress(initial_address_string);

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
        UInt64 value;
        readVarUInt(value, in);
        collaborate_with_initiator = static_cast<bool>(value);
        readVarUInt(obsolete_count_participating_replicas, in);
        readVarUInt(number_of_current_replica, in);
    }
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
    return std::format("{}.{}.{} ({})", client_version_major, client_version_minor, client_version_patch, client_tcp_protocol_version);
}

VersionNumber ClientInfo::getVersionNumber() const
{
    return VersionNumber(client_version_major, client_version_minor, client_version_patch);
}

void ClientInfo::fillOSUserHostNameAndVersionInfo()
{
    os_user.resize(256, '\0');
    if (0 == getlogin_r(os_user.data(), static_cast<int>(os_user.size() - 1)))
        os_user.resize(strlen(os_user.c_str()));
    else
        os_user.clear();    /// Don't mind if we cannot determine user login.

    client_hostname = getFQDNOrHostName();

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
    }

    return std::format("Unknown server interface ({}).", static_cast<int>(interface));
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

}
