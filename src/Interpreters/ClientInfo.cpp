#include <Interpreters/ClientInfo.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>
#include <base/getFQDNOrHostName.h>
#include <unistd.h>

#include <Common/config_version.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void ClientInfo::write(WriteBuffer & out, UInt64 server_protocol_revision) const
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        throw Exception("Logical error: method ClientInfo::write is called for unsupported server revision", ErrorCodes::LOGICAL_ERROR);

    writeBinary(UInt8(query_kind), out);
    if (empty())
        return;

    writeBinary(initial_user, out);
    writeBinary(initial_query_id, out);
    writeBinary(initial_address.toString(), out);

    if (server_protocol_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME)
        writeBinary(initial_query_start_time_microseconds, out);

    writeBinary(UInt8(interface), out);

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
        writeBinary(UInt8(http_method), out);
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
            writeBinary(uint8_t(0), out);
        }
    }

    if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS)
    {
        writeVarUInt(static_cast<UInt64>(collaborate_with_initiator), out);
        writeVarUInt(count_participating_replicas, out);
        writeVarUInt(number_of_current_replica, out);
    }
}


void ClientInfo::read(ReadBuffer & in, UInt64 client_protocol_revision)
{
    if (client_protocol_revision < DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        throw Exception("Logical error: method ClientInfo::read is called for unsupported client revision", ErrorCodes::LOGICAL_ERROR);

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
        readVarUInt(count_participating_replicas, in);
        readVarUInt(number_of_current_replica, in);
    }
}


void ClientInfo::setInitialQuery()
{
    query_kind = QueryKind::INITIAL_QUERY;
    fillOSUserHostNameAndVersionInfo();
    if (client_name.empty())
        client_name = DBMS_NAME;
    else
        client_name = (DBMS_NAME " ") + client_name;
}


void ClientInfo::fillOSUserHostNameAndVersionInfo()
{
    os_user.resize(256, '\0');
    if (0 == getlogin_r(os_user.data(), os_user.size() - 1))
        os_user.resize(strlen(os_user.c_str()));
    else
        os_user.clear();    /// Don't mind if we cannot determine user login.

    client_hostname = getFQDNOrHostName();

    client_version_major = DBMS_VERSION_MAJOR;
    client_version_minor = DBMS_VERSION_MINOR;
    client_version_patch = DBMS_VERSION_PATCH;
    client_tcp_protocol_version = DBMS_TCP_PROTOCOL_VERSION;
}


}
