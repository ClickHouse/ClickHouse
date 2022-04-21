#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class AsynchronousMetrics;
class Context;


/** Implements system table asynchronous_metrics, which allows to get values of periodically (asynchronously) updated metrics.
  */
class StorageSystemAsynchronousMetrics final : public IStorageSystemOneBlock<StorageSystemAsynchronousMetrics>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemAsynchronousMetrics> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemAsynchronousMetrics>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemAsynchronousMetrics(CreatePasskey, TArgs &&... args) : StorageSystemAsynchronousMetrics{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemAsynchronousMetrics"; }

    static NamesAndTypesList getNamesAndTypes();

private:
    const AsynchronousMetrics & async_metrics;

protected:
    StorageSystemAsynchronousMetrics(const StorageID & table_id_, const AsynchronousMetrics & async_metrics_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
