#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/AsyncLoader.h>

namespace DB
{

/// Load tables from system database. Only real tables like query_log, part_log.
/// You should first load system database, then attach system tables that you need into it, then load other databases.
/// It returns tasks that are still in progress if `async_load_system_database = true` otherwise it wait for all jobs to be done.
/// Background operations in system tables may slowdown loading of the rest tables,
/// so we startup system tables after all databases are loaded.
[[nodiscard]] LoadTaskPtrs loadMetadataSystem(ContextMutablePtr context, bool async_load_system_database = false);

/// Load tables from databases and add them to context. Databases 'system' and 'information_schema' are ignored.
/// Use separate function to load system tables.
/// If `async_load_databases = true` returns tasks for asynchronous load and startup of all tables
/// Note that returned tasks are already scheduled.
[[nodiscard]] LoadTaskPtrs loadMetadata(ContextMutablePtr context, const String & default_database_name = {}, bool async_load_databases = false);

/// Converts `system` database from Ordinary to Atomic (if needed)
void maybeConvertSystemDatabase(ContextMutablePtr context, LoadTaskPtrs & load_system_metadata_tasks);

/// Converts all databases (except system) from Ordinary to Atomic if convert_ordinary_to_atomic flag exists
/// Waits for `load_metadata` task before conversions
void convertDatabasesEnginesIfNeed(const LoadTaskPtrs & load_metadata, ContextMutablePtr context);

}
