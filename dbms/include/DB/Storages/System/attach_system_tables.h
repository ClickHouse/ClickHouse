#pragma once

#include <DB/Databases/IDatabase.h>

namespace DB
{
class Context;
class AsynchronousMetrics;

void attach_system_tables_server(DatabasePtr system_database, Context * global_context, bool has_zookeeper);
void attach_system_tables_local(DatabasePtr system_database);
void attach_system_tables_async(DatabasePtr system_database, AsynchronousMetrics & async_metrics);
}
