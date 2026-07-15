#include <Core/PostgreSQL/Utils.h>

#if USE_LIBPQXX

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

namespace postgres
{

ConnectionInfo formatConnectionString(
    String dbname, String host, UInt16 port, String user, String password, UInt64 timeout,
    const ConnectionSSLParams & ssl_params)
{
    DB::WriteBufferFromOwnString out;
    out << "dbname=" << DB::quote << dbname
        << " host=" << DB::quote << host
        << " port=" << port
        << " user=" << DB::quote << user
        << " password=" << DB::quote << password
        << " connect_timeout=" << timeout;

    /// Append only the SSL options that were explicitly set, so that unset options
    /// keep libpq's own defaults. libpq validates the values (e.g. an unknown
    /// `sslmode`) and reports a clear error on connect.
    if (!ssl_params.ssl_mode.empty())
        out << " sslmode=" << DB::quote << ssl_params.ssl_mode;
    if (!ssl_params.ssl_root_cert.empty())
        out << " sslrootcert=" << DB::quote << ssl_params.ssl_root_cert;
    if (!ssl_params.ssl_cert.empty())
        out << " sslcert=" << DB::quote << ssl_params.ssl_cert;
    if (!ssl_params.ssl_key.empty())
        out << " sslkey=" << DB::quote << ssl_params.ssl_key;

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

}

#endif
