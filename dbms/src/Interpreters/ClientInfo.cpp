#include <Interpreters/ClientInfo.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/ClickHouseRevision.h>
#include <Common/config_version.h>
#include <port/unistd.h>


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
