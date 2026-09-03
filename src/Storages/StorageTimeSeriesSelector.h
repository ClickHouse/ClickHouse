#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/IStorage.h>


namespace DB
{

/// Represents a storage for table function timeSeriesSelector().
class StorageTimeSeriesSelector : public IStorage
{
public:
    struct Configuration
    {
        StorageID time_series_storage_id = StorageID::createEmpty();

        /// Data types of the corresponding columns in the TimeSeries table.
        /// We use these data types for the columns we read from table function timeSeriesSelector().
        DataTypePtr id_data_type;
        DataTypePtr timestamp_data_type;
        DataTypePtr scalar_data_type;

        PrometheusQueryTree selector;

        /// The scale of these fields is the same as the scale used in `timestamp_data_type`.
        DateTime64 min_time;
        DateTime64 max_time;
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context);

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
