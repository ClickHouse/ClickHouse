#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Load tables from system database. Only real tables like query_log, part_log.
/// You should first load system database, then attach system tables that you need into it, then load other databases.
void loadMetadataSystem(ContextMutablePtr context);

/// Load tables from databases and add them to context. Database 'system' is ignored. Use separate function to load system tables.
void loadMetadata(ContextMutablePtr context, const String & default_database_name = {});

}
