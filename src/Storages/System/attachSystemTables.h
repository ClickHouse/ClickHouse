#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class AsynchronousMetrics;
class IDatabase;

template <bool lazy>
void attachSystemTablesLocal(ContextPtr context, IDatabase & system_database);
void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper);
void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics);

extern template void attachSystemTablesLocal<false>(ContextPtr context, IDatabase & system_database);
extern template void attachSystemTablesLocal<true>(ContextPtr context, IDatabase & system_database);

}
