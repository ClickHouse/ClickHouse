#include "Utils.h"

#if USE_LIBPQXX

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

namespace postgres
{

ConnectionInfo formatConnectionString(
    String dbname, String host, UInt16 port, String user, String password, size_t connect_timeout,
    SSLMode ssl_mode, String ssl_root_cert)
{
    DB::WriteBufferFromOwnString out;
    out << "dbname=" << DB::quote << dbname
        << " host=" << DB::quote << host
        << " port=" << port
        << " user=" << DB::quote << user
        << " password=" << DB::quote << password
        << " application_name=clickhouse"
        << " connect_timeout=" << connect_timeout
        << " sslmode=" << DB::SettingFieldSSLMode(ssl_mode).toString();
    if (!ssl_root_cert.empty())
        out << " sslrootcert=" << DB::quote << ssl_root_cert;
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
