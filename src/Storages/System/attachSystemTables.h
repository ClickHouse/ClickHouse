#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class AsynchronousMetrics;
class IDatabase;

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);
void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics);

/// Reject a `query_log` configuration that collides with `system.user_query_log`. Part of what
/// `attachSystemTablesServer` does, but split out, because it is a check of the configuration rather than of the
/// database: `clickhouse local` attaches the system tables on demand and has to run it at startup regardless, so
/// that a bad configuration is rejected right away instead of on the first query that reads a system table.
void validateUserQueryLogConfig(ContextPtr context);

/// `system.one` alone. Split out so that `clickhouse local` can attach it eagerly and leave the remaining ~130
/// tables to be attached on demand: every `FROM`-less query (`SELECT 1`) resolves `system.one`, so deferring it
/// together with the rest would make the very first query build the whole `system` database anyway.
void attachSystemTableOne(ContextPtr context, IDatabase & system_database);
/// Everything `attachSystemTablesServer` attaches except `system.one`.
void attachSystemTablesServerExceptOne(ContextPtr context, IDatabase & system_database, bool has_zookeeper, [[maybe_unused]] bool has_keeper_server);

}
