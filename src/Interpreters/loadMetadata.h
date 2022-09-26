#pragma once

#include <Interpreters/Context_fwd.h>
#include <Databases/TablesLoader.h>


namespace DB
{

/// Load tables from system database. Only real tables like query_log, part_log.
/// You should first load system database, then attach system tables that you need into it, then load other databases.
void loadMetadataSystem(ContextMutablePtr context);

/// Load tables from databases and add them to context. Database 'system' and 'information_schema' is ignored.
/// Use separate function to load system tables.
void loadMetadata(ContextMutablePtr context, const String & default_database_name = {});

/// Background operations in system tables may slowdown loading of the rest tables,
/// so we startup system tables after all databases are loaded.
void startupSystemTables();

/// Converts database with Ordinary engine to Atomic. Does nothing if database is not Ordinary.
/// Can be called only during server startup when there are no queries from users.
void maybeConvertOrdinaryDatabaseToAtomic(ContextMutablePtr context, const DatabasePtr & database);

}
