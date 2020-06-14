#pragma once

#include <memory>


namespace DB
{

class Context;
class AsynchronousMetrics;
class IDatabase;

void attachSystemTablesServer(IDatabase & system_database, bool has_zookeeper);
void attachSystemTablesLocal(IDatabase & system_database);
void attachSystemTablesAsync(IDatabase & system_database, AsynchronousMetrics & async_metrics);

}
