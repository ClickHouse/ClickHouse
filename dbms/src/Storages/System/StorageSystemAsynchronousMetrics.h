#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class AsynchronousMetrics;
class Context;


/** Implements system table asynchronous_metrics, which allows to get values of periodically (asynchronously) updated metrics.
  */
class StorageSystemAsynchronousMetrics : public ext::shared_ptr_helper<StorageSystemAsynchronousMetrics>, public IStorageSystemOneBlock<StorageSystemAsynchronousMetrics>
{
public:
    std::string getName() const override { return "SystemAsynchronousMetrics"; }

    static NamesAndTypesList getNamesAndTypes();

private:
    const AsynchronousMetrics & async_metrics;

protected:
    StorageSystemAsynchronousMetrics(const std::string & name_, const AsynchronousMetrics & async_metrics_);

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
