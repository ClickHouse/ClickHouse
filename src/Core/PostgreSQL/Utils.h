#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/Types.h>
#include "Connection.h"
#include <Common/Exception.h>

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

}

#endif
