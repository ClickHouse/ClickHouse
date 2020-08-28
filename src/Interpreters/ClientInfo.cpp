#include <Interpreters/ClientInfo.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <common/getFQDNOrHostName.h>
#include <Common/ClickHouseRevision.h>
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
        writeVarUInt(client_revision, out);
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
        // No point writing these numbers with variable length, because they
        // are random and will probably require the full length anyway.
        writeBinary(opentelemetry_trace_id, out);
        writeBinary(opentelemetry_span_id, out);
        writeBinary(opentelemetry_parent_span_id, out);
        writeBinary(opentelemetry_tracestate, out);
        writeBinary(opentelemetry_trace_flags, out);
        std::cerr << fmt::format("wrote {:x}, {}, {}\n", opentelemetry_trace_id, opentelemetry_span_id, opentelemetry_parent_span_id) << StackTrace().toString() << std::endl;
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
        readVarUInt(client_revision, in);
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
            client_version_patch = client_revision;
    }

    if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY)
    {
        readBinary(opentelemetry_trace_id, in);
        readBinary(opentelemetry_span_id, in);
        readBinary(opentelemetry_parent_span_id, in);
        readBinary(opentelemetry_tracestate, in);
        readBinary(opentelemetry_trace_flags, in);

        std::cerr << fmt::format("read {:x}, {}, {}\n", opentelemetry_trace_id, opentelemetry_span_id, opentelemetry_parent_span_id) << StackTrace().toString() << std::endl;
    }
}


void ClientInfo::setInitialQuery()
{
    query_kind = QueryKind::INITIAL_QUERY;
    fillOSUserHostNameAndVersionInfo();
    client_name = (DBMS_NAME " ") + client_name;
}

template <typename T>
bool readLowercaseHexDigits(const char *& begin, const char * end, T & dest_value, std::string & error)
{
    char * dest_begin = reinterpret_cast<char *>(&dest_value);
    char * dest_end = dest_begin + sizeof(dest_value);
    bool odd_character = true;
    for (;;)
    {
        if (begin == end)
        {
            if (dest_begin == dest_end)
            {
                return true;
            }
            error = fmt::format("Not enough charaters in the input, got {}, need {} more", end - begin, dest_end - dest_begin);
            return false;
        }

        if (dest_begin == dest_end)
        {
            return true;
        }

        int cur = 0;
        if (*begin >= '0' && *begin <= '9')
        {
            cur = *begin - '0';
        }
        else if (*begin >= 'a' && *begin <= 'f')
        {
            cur = 10 + *begin - 'a';
        }
        else
        {
            error = fmt::format("Encountered '{}' which is not a lowercase hexadecimal digit", *begin);
            return false;
        }

        // Two characters per byte, little-endian.
        if (odd_character)
        {
            *(dest_end - 1) = cur;
        }
        else
        {
            *(dest_end - 1) = *(dest_end - 1) << 8 | cur;
            --dest_end;
        }

        begin++;
        odd_character = !odd_character;
    }
}

bool ClientInfo::setOpenTelemetryTraceparent(const std::string & traceparent,
    std::string & error)
{
    uint8_t version = -1;
    __uint128_t trace_id = 0;
    uint64_t trace_parent = 0;
    uint8_t trace_flags = 0;

    const char * begin = &traceparent[0];
    const char * end = begin + traceparent.length();

#define CHECK_CONDITION(condition, ...) \
    ((condition) || (error = fmt::format(__VA_ARGS__), false))

#define CHECK_DELIMITER \
    (begin >= end \
        ? (error = fmt::format( \
                "Expected '-' delimiter, got EOL at position {}", \
                begin - &traceparent[0]), \
            false) \
        : *begin != '-' \
            ? (error = fmt::format( \
                    "Expected '-' delimiter, got '{}' at position {}", \
                    *begin, begin - &traceparent[0]), \
                false) \
            : (++begin, true))

    bool result = readLowercaseHexDigits(begin, end, version, error)
        && CHECK_CONDITION(version == 0, "Expected version 00, got {}", version)
        && CHECK_DELIMITER
        && readLowercaseHexDigits(begin, end, trace_id, error)
        && CHECK_DELIMITER
        && readLowercaseHexDigits(begin, end, trace_parent, error)
        && CHECK_DELIMITER
        && readLowercaseHexDigits(begin, end, trace_flags, error)
        && CHECK_CONDITION(begin == end,
            "Expected end of string, got {} at position {}", *begin, end - begin);

#undef CHECK
#undef CHECK_DELIMITER

    if (!result)
    {
        return false;
    }

    opentelemetry_trace_id = trace_id;
    opentelemetry_parent_span_id = trace_parent;
    opentelemetry_trace_flags = trace_flags;
    return true;
}


std::string ClientInfo::getOpenTelemetryTraceparentForChild() const
{
    // This span is a parent for its children (so deep...), so we specify
    // this span_id as a parent id.
    return fmt::format("00-{:032x}-{:016x}-{:02x}", opentelemetry_trace_id,
        opentelemetry_span_id,
        // This cast is because fmt is being weird and complaining that
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
    client_revision = ClickHouseRevision::get();
}


}
