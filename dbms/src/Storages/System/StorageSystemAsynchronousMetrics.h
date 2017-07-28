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
friend class ext::shared_ptr_helper<StorageSystemAsynchronousMetrics>;

public:
    std::string getName() const override { return "SystemAsynchronousMetrics"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;
    NamesAndTypesList columns;
    const AsynchronousMetrics & async_metrics;

    StorageSystemAsynchronousMetrics(const std::string & name_, const AsynchronousMetrics & async_metrics_);
};

}
