#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

#include "config.h"

namespace DB
{

class AsynchronousMetrics;
class IDatabase;

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);
void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics);

void validateSystemUserQueryLog(ContextPtr context, const IDatabase & system_database);
void attachSystemTableOne(ContextPtr context, IDatabase & system_database);
void attachSystemTablesServerExceptOne(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);

/// System tables which are attached only when ZooKeeper or ClickHouse Keeper is configured, when this node runs an
/// in-process Keeper, or when experimental transactions are enabled. Their documentation is owned by the source and
/// is independent of that configuration, so `system.documentation` attaches them to a scratch database to render
/// their pages wherever the tables themselves are unavailable, such as in `clickhouse-local`.
void attachSystemTablesGatedOnZooKeeper(ContextPtr context, IDatabase & system_database);
#if USE_NURAFT
void attachSystemTablesGatedOnKeeperServer(ContextPtr context, IDatabase & system_database);
#endif
void attachSystemTablesGatedOnTransactions(ContextPtr context, IDatabase & system_database);

}
