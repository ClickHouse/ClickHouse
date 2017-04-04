#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class AsynchronousMetrics;
class Context;


/** Implements system table asynchronous_metrics, which allows to get values of periodically (asynchronously) updated metrics.
  */
class StorageSystemAsynchronousMetrics : private ext::shared_ptr_helper<StorageSystemAsynchronousMetrics>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemAsynchronousMetrics>;

public:
    static StoragePtr create(const std::string & name_, const AsynchronousMetrics & async_metrics_);

    std::string getName() const override { return "SystemAsynchronousMetrics"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

private:
    const std::string name;
    NamesAndTypesList columns;
    const AsynchronousMetrics & async_metrics;

    StorageSystemAsynchronousMetrics(const std::string & name_, const AsynchronousMetrics & async_metrics_);
};

}
