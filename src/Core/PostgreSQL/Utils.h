#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/SettingsEnums.h>
#include <Core/Types.h>
#include "Connection.h"
#include "PoolWithFailover.h"
#include <Common/Exception.h>

namespace pqxx
{
    using ReadTransaction = pqxx::read_transaction;
    using ReplicationTransaction = pqxx::transaction<isolation_level::repeatable_read, write_policy::read_only>;
}

namespace postgres
{
    using SSLMode = DB::SSLMode;

ConnectionInfo formatConnectionString(
    String dbname, String host, UInt16 port, String user, String password,
    size_t connect_timeout = POSTGRESQL_POOL_DEFAULT_CONNECT_TIMEOUT_SEC,
    SSLMode ssl_mode = SSLMode::PREFER,
    String ssl_root_cert = ""
);

String getConnectionForLog(const String & host, UInt16 port);

String formatNameForLogs(const String & postgres_database_name, const String & postgres_table_name);

}

#endif
