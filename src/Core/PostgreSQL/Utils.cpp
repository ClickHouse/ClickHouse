#include <Core/PostgreSQL/Utils.h>

#if USE_LIBPQXX

#include <Common/Exception.h>
#include <Common/TemporarySecretFile.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

    ConnectionInfo connection_info;

    /// Append only the SSL options that were explicitly set, so that unset options
    /// keep libpq's own defaults. libpq validates the values (e.g. an unknown
    /// `sslmode`) and reports a clear error on connect.
    if (!ssl_params.ssl_mode.empty())
        out << " sslmode=" << DB::quote << ssl_params.ssl_mode;

    /// A credential given as literal contents is materialized into a private temporary file:
    /// libpq can only load certificates and keys from the filesystem. The file must live as long
    /// as the connection info, because libpq re-reads it on every (re)connect. The conflict check
    /// is a last line of defence for the code paths whose input is not validated upstream
    /// (a dictionary defined in a server configuration file).
    auto add_credential = [&](const char * option, const String & path, const String & pem,
                              std::string_view path_key, std::string_view contents_key)
    {
        if (!path.empty() && !pem.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "`{}` and `{}` cannot be specified at the same time", path_key, contents_key);

        if (!pem.empty())
        {
            auto file = std::make_shared<DB::TemporarySecretFile>(pem);
            out << " " << option << "=" << DB::quote << file->getPath();
            connection_info.tls_secret_files.emplace_back(std::move(file));
        }
        else if (!path.empty())
        {
            out << " " << option << "=" << DB::quote << path;
        }
    };

    add_credential("sslrootcert", ssl_params.ssl_root_cert, ssl_params.ssl_root_cert_pem, "sslrootcert", "sslrootcert_pem");
    add_credential("sslcert", ssl_params.ssl_cert, ssl_params.ssl_cert_pem, "sslcert", "sslcert_pem");
    add_credential("sslkey", ssl_params.ssl_key, ssl_params.ssl_key_pem, "sslkey", "sslkey_pem");

    connection_info.connection_string = out.str();
    connection_info.host_port = host + ':' + DB::toString(port);
    return connection_info;
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
        /// `EAI_AGAIN` only: the resolver was unreachable. `EAI_NONAME` (`Name or service not known`)
        /// means the host does not exist, which is a permanent misconfiguration.
        "Temporary failure in name resolution",
    };

    for (const auto marker : transient_markers)
        if (message.contains(marker))
            return true;
    return false;
}

}

#endif
