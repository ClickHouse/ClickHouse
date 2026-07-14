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

ConnectionInfo formatConnectionString(String dbname, String host, UInt16 port, String user, String password, UInt64 timeout);

String getConnectionForLog(const String & host, UInt16 port);

String formatNameForLogs(const String & postgres_database_name, const String & postgres_table_name);

/// Classify a `pqxx::broken_connection` message as a transient transport failure (server not
/// reachable / not responding) rather than a permanent one (server responded and rejected us, or a
/// misconfiguration). libpq's connection errors carry no error code — only a localized free-text
/// message (`PQerrorMessage`), and the connection is already finished when the exception is thrown —
/// so this matches the known transport `strerror` substrings that libpq appends after
/// `... failed: `. Anything unrecognized (including a server `FATAL:` auth / missing-database reply)
/// is treated as non-transient, so an unclassifiable failure fails loud (logged at Error) instead of
/// silently downgraded.
bool isTransientConnectionError(std::string_view message);

}

#endif
