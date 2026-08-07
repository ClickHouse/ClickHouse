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

/// Whether the server was unreachable / not responding, as opposed to having responded and rejected
/// us. Matched on the message text because libpq connection errors carry no error code, only the
/// free-text `PQerrorMessage`. Unrecognized messages are not transient, so they stay logged at Error.
bool isTransientConnectionError(std::string_view message);

}

#endif
