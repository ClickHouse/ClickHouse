#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class AsynchronousMetrics;
class IDatabase;

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);
void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics);

/// Reject a `query_log` configuration or an existing table that collides with `system.user_query_log`. Part of
/// what `attachSystemTablesServer` does, but split out so that `clickhouse local`, which attaches the system
/// tables on demand, can still run it at startup: a collision has to be reported right away instead of on the
/// first query that happens to read a system table. Must be called before the deferred population is armed, so
/// that the existence check does not trigger it.
void validateSystemUserQueryLog(ContextPtr context, const IDatabase & system_database);

/// `system.one` alone. Split out so that `clickhouse local` can attach it eagerly and leave the remaining ~130
/// tables to be attached on demand: every `FROM`-less query (`SELECT 1`) resolves `system.one`, so deferring it
/// together with the rest would make the very first query build the whole `system` database anyway.
void attachSystemTableOne(ContextPtr context, IDatabase & system_database);
/// Everything `attachSystemTablesServer` attaches except `system.one`.
void attachSystemTablesServerExceptOne(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);

}
