#include <Interpreters/ClientInfo.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <common/getFQDNOrHostName.h>
#include <unistd.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void ClientInfo::write(WriteBuffer & out, const UInt64 server_protocol_revision) const
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        throw Exception("Logical error: method ClientInfo::write is called for unsupported server revision", ErrorCodes::LOGICAL_ERROR);

    writeBinary(UInt8(query_kind), out);
    if (empty())
        return;

    writeBinary(initial_user, out);
    writeBinary(initial_query_id, out);
    writeBinary(initial_address.toString(), out);

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
    }

    if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
        writeBinary(quota_key, out);

    if (interface == Interface::TCP)
    {
        if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
            writeVarUInt(client_version_patch, out);
    }

    if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY)
    {
        if (opentelemetry_trace_id)
        {
            // Have OpenTelemetry header.
            writeBinary(uint8_t(1), out);
            // No point writing these numbers with variable length, because they
            // are random and will probably require the full length anyway.
            writeBinary(opentelemetry_trace_id, out);
            writeBinary(opentelemetry_span_id, out);
            writeBinary(opentelemetry_tracestate, out);
            writeBinary(opentelemetry_trace_flags, out);
        }
        else
        {
            // Don't have OpenTelemetry header.
            writeBinary(uint8_t(0), out);
        }
    }
}


void ClientInfo::read(ReadBuffer & in, const UInt64 client_protocol_revision)
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
    }

    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
        readBinary(quota_key, in);

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
            readBinary(opentelemetry_trace_id, in);
            readBinary(opentelemetry_span_id, in);
            readBinary(opentelemetry_tracestate, in);
            readBinary(opentelemetry_trace_flags, in);
        }
    }
}


void ClientInfo::setInitialQuery()
{
    query_kind = QueryKind::INITIAL_QUERY;
    fillOSUserHostNameAndVersionInfo();
    client_name = (DBMS_NAME " ") + client_name;
}

bool ClientInfo::parseTraceparentHeader(const std::string & traceparent,
    std::string & error)
{
    uint8_t version = -1;
    uint64_t trace_id_high = 0;
    uint64_t trace_id_low = 0;
    uint64_t trace_parent = 0;
    uint8_t trace_flags = 0;

    // Version 00, which is the only one we can parse, is fixed width. Use this
    // fact for an additional sanity check.
    const int expected_length = 2 + 1 + 32 + 1 + 16 + 1 + 2;
    if (traceparent.length() != expected_length)
    {
        error = fmt::format("unexpected length {}, expected {}",
            traceparent.length(), expected_length);
        return false;
    }

    // clang-tidy doesn't like sscanf:
    //   error: 'sscanf' used to convert a string to an unsigned integer value,
    //   but function will not report conversion errors; consider using 'strtoul'
    //   instead [cert-err34-c,-warnings-as-errors]
    // There is no other ready solution, and hand-rolling a more complicated
    // parser for an HTTP header in C++ sounds like RCE.
    // NOLINTNEXTLINE(cert-err34-c)
    int result = sscanf(&traceparent[0],
        "%2" SCNx8 "-%16" SCNx64 "%16" SCNx64 "-%16" SCNx64 "-%2" SCNx8,
        &version, &trace_id_high, &trace_id_low, &trace_parent, &trace_flags);

    if (result == EOF)
    {
        error = "EOF";
        return false;
    }

    // We read uint128 as two uint64, so 5 parts and not 4.
    if (result != 5)
    {
        error = fmt::format("could only read {} parts instead of the expected 5",
            result);
        return false;
    }

    if (version != 0)
    {
        error = fmt::format("unexpected version {}, expected 00", version);
        return false;
    }

    opentelemetry_trace_id = static_cast<__uint128_t>(trace_id_high) << 64
        | trace_id_low;
    opentelemetry_span_id = trace_parent;
    opentelemetry_trace_flags = trace_flags;
    return true;
}


std::string ClientInfo::composeTraceparentHeader() const
{
    // This span is a parent for its children, so we specify this span_id as a
    // parent id.
    return fmt::format("00-{:032x}-{:016x}-{:02x}", opentelemetry_trace_id,
        opentelemetry_span_id,
        // This cast is needed because fmt is being weird and complaining that
        // "mixing character types is not allowed".
        static_cast<uint8_t>(opentelemetry_trace_flags));
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
