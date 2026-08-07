#include <Core/PostgreSQL/Utils.h>

#if USE_LIBPQXX

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

namespace postgres
{

ConnectionInfo formatConnectionString(String dbname, String host, UInt16 port, String user, String password, UInt64 timeout)
{
    DB::WriteBufferFromOwnString out;
    out << "dbname=" << DB::quote << dbname
        << " host=" << DB::quote << host
        << " port=" << port
        << " user=" << DB::quote << user
        << " password=" << DB::quote << password
        << " connect_timeout=" << timeout;
    return {out.str(), host + ':' + DB::toString(port)};
}

String getConnectionForLog(const String & host, UInt16 port)
{
    return host + ":" + DB::toString(port);
}

String formatNameForLogs(const String & postgres_database_name, const String & postgres_table_name)
{
    /// Logger for StorageMaterializedPostgreSQL - both db and table names.
    /// Logger for PostgreSQLReplicationHandler and Consumer - either both db and table names or only db name.
    chassert(!postgres_database_name.empty());
    if (postgres_table_name.empty())
        return postgres_database_name;
    return postgres_database_name + '.' + postgres_table_name;
}

bool isTransientConnectionError(std::string_view message)
{
    /// libpq frames connect failures as `connection to server at "host", port N failed: <reason>`,
    /// where `<reason>` for a transport failure is the OS `strerror`.
    static constexpr std::string_view transient_markers[] = {
        "Connection refused",
        "Connection timed out",
        "timeout expired",
        "No route to host",
        "Network is unreachable",
        "Connection reset by peer",
        "could not connect to server",
        "could not translate host name",
        "Name or service not known",
        "Temporary failure in name resolution",
    };

    for (const auto marker : transient_markers)
        if (message.contains(marker))
            return true;
    return false;
}

}

#endif
