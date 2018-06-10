#pragma once

#include <Poco/Net/SocketAddress.h>
#include <Core/Types.h>


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
    enum class Interface : UInt8
    {
        TCP = 1,
        HTTP = 2,
    };

    enum class HTTPMethod : UInt8
    {
        UNKNOWN = 0,
        GET = 1,
        POST = 2,
    };

    enum class QueryKind : UInt8
    {
        NO_QUERY = 0,            /// Uninitialized object.
        INITIAL_QUERY = 1,
        SECONDARY_QUERY = 2,    /// Query that was initiated by another query for distributed query execution.
    };


    QueryKind query_kind = QueryKind::NO_QUERY;

    /// Current values are not serialized, because it is passed separately.
    String current_user;
    String current_query_id;
    Poco::Net::SocketAddress current_address;
    /// Use current user and password when sending query to replica leader
    String current_password;

    /// When query_kind == INITIAL_QUERY, these values are equal to current.
    String initial_user;
    String initial_query_id;
    Poco::Net::SocketAddress initial_address;

    /// All below are parameters related to initial query.

    Interface interface = Interface::TCP;

    /// For tcp
    String os_user;
    String client_hostname;
    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    unsigned client_revision = 0;

    /// For http
    HTTPMethod http_method = HTTPMethod::UNKNOWN;
    String http_user_agent;

    /// Common
    String quota_key;

    bool empty() const { return query_kind == QueryKind::NO_QUERY; }

    /** Serialization and deserialization.
      * Only values that are not calculated automatically or passed separately are serialized.
      * Revisions are passed to use format that server will understand or client was used.
      */
    void write(WriteBuffer & out, const UInt64 server_protocol_revision) const;
    void read(ReadBuffer & in, const UInt64 client_protocol_revision);

    void fillOSUserHostNameAndVersionInfo();
};

}
