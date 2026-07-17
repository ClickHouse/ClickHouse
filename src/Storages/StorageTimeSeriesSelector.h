#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/IStorage.h>


namespace DB
{

/// Represents a storage for table function timeSeriesSelector().
class StorageTimeSeriesSelector : public IStorage
{
public:
    StorageTimeSeriesSelector(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const StorageID & time_series_storage_id_,
        const PrometheusQueryTree & instant_selector_,
        const Field & min_time_,
        const Field & max_time_);

    std::string getName() const override { return "TimeSeriesSelector"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    StorageID time_series_storage_id;
    PrometheusQueryTree instant_selector;
    Field min_time;
    Field max_time;
};

}
