#pragma once

#include <Common/config.h>
#include <cassandra.h>

#if USE_CASSANDRA
#include <base/types.h>
#include <Storages/ColumnsDescription.h>
#include <Processors/Transforms/CassandraSource.h>
#include <map>

namespace DB
{
std::map<std::string, ColumnsDescription> fetchTablesCassandra(
    String database,
    String table,
    UInt16 port,
    String host,
    String username,
    String password,
    String consist);

    std::mutex connect_mutex;
    CassClusterPtr cluster;
    CassSessionWeak maybe_session;
}

#endif
