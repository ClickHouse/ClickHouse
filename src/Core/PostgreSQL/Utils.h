#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/Types.h>
#include <Core/PostgreSQL/Connection.h>

namespace pqxx
{
    using ReadTransaction = pqxx::read_transaction;
    using ReplicationTransaction = pqxx::transaction<isolation_level::repeatable_read, write_policy::read_only>;
}

namespace postgres
{

/// Optional TLS/SSL parameters passed to libpq via the connection string.
/// Empty fields are omitted from the connection string, so libpq keeps its own
/// defaults (in particular, an empty `ssl_mode` leaves libpq at `sslmode=prefer`).
/// Certificate and key paths coming from SQL are restricted to `user_files_path`
/// before they reach here, see `StoragePostgreSQL::validateSSLCertificatePaths`.
struct ConnectionSSLParams
{
    String ssl_mode;       /// libpq `sslmode`: disable, allow, prefer, require, verify-ca or verify-full.
    String ssl_root_cert;  /// libpq `sslrootcert`: path to the CA certificate (or the special value `system`).
    String ssl_cert;       /// libpq `sslcert`: path to the client certificate.
    String ssl_key;        /// libpq `sslkey`: path to the client private key.
};

ConnectionInfo formatConnectionString(
    String dbname, String host, UInt16 port, String user, String password, UInt64 timeout,
    const ConnectionSSLParams & ssl_params = {});

String getConnectionForLog(const String & host, UInt16 port);

String formatNameForLogs(const String & postgres_database_name, const String & postgres_table_name);

}

#endif
