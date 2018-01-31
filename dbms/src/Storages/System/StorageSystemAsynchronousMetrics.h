#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class AsynchronousMetrics;
class Context;


/** Implements system table asynchronous_metrics, which allows to get values of periodically (asynchronously) updated metrics.
  */
class StorageSystemAsynchronousMetrics : public ext::shared_ptr_helper<StorageSystemAsynchronousMetrics>, public IStorage
{
public:
    std::string getName() const override { return "SystemAsynchronousMetrics"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;
    const AsynchronousMetrics & async_metrics;

protected:
    StorageSystemAsynchronousMetrics(const std::string & name_, const AsynchronousMetrics & async_metrics_);
};

}
