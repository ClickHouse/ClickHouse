#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class AsynchronousMetrics;
class IDatabase;

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);
void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics);

/// `system.one` alone. Split out so that `clickhouse local` can attach it eagerly and leave the remaining ~130
/// tables to be attached on demand: every `FROM`-less query (`SELECT 1`) resolves `system.one`, so deferring it
/// together with the rest would make the very first query build the whole `system` database anyway.
void attachSystemTableOne(ContextPtr context, IDatabase & system_database);
/// Everything `attachSystemTablesServer` attaches except `system.one`.
void attachSystemTablesServerExceptOne(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);

}
