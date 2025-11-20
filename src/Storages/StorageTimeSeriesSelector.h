#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/IStorage.h>


namespace DB
{

/// Represents a storage for table function timeSeriesSelector() and timeSeriesSelectorToGrid().
class StorageTimeSeriesSelector : public IStorage
{
public:
    struct Configuration
    {
        PrometheusQueryTree selector;

        StorageID time_series_storage_id = StorageID::createEmpty();
        DataTypePtr id_type;
        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;

        DecimalField<DateTime64> start_time;
        DecimalField<DateTime64> end_time;
        DecimalField<Decimal64> step;
        DecimalField<Decimal64> window;

        /// If true the `window` is left-closed, otherwise it's left-opened.
        /// As for the right boundary the `window` is always right-closed.
        bool left_closed = false;
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context, bool to_grid);

    StorageTimeSeriesSelector(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_);

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
    Configuration config;
};

}
