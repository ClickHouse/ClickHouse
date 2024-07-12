#include "Utils.h"

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
    assert(!postgres_database_name.empty());
    if (postgres_table_name.empty())
        return postgres_database_name;
    return postgres_database_name + '.' + postgres_table_name;
}

}

#endif
