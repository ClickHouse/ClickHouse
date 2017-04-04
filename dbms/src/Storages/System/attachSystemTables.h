#pragma once

#include <Databases/IDatabase.h>

namespace DB
{
class Context;
class AsynchronousMetrics;

void attachSystemTablesServer(DatabasePtr system_database, Context * global_context, bool has_zookeeper);
void attachSystemTablesLocal(DatabasePtr system_database);
void attachSystemTablesAsync(DatabasePtr system_database, AsynchronousMetrics & async_metrics);
}
