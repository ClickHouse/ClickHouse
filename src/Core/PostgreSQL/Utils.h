#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

#include <pqxx/pqxx> // Y_IGNORE
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
ConnectionInfo formatConnectionString(String dbname, String host, UInt16 port, String user, String password);
}

#endif
