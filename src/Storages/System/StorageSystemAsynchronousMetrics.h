#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class AsynchronousMetrics;
class Context;

/** The documentation of `system.asynchronous_metrics`.
  *
  * The table itself is attached only by the server, because it needs a live `AsynchronousMetrics` instance,
  * but its documentation has to be available everywhere, in particular in `clickhouse-local`, which is how
  * the documentation generator reads `system.documentation`.
  */
extern const char * const ASYNCHRONOUS_METRICS_DOCUMENTATION;
extern const char * const ASYNCHRONOUS_METRICS_DOCUMENTATION_SOURCE;


/** Implements system table asynchronous_metrics, which allows to get values of periodically (asynchronously) updated metrics.
  */
class StorageSystemAsynchronousMetrics final : public IStorageSystemOneBlock
{
public:
    StorageSystemAsynchronousMetrics(const StorageID & table_id_, const AsynchronousMetrics & async_metrics_);

    std::string getName() const override { return "SystemAsynchronousMetrics"; }

    static ColumnsDescription getColumnsDescription();

private:
    const AsynchronousMetrics & async_metrics;

protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
