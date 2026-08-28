#pragma once

#include <Common/Logger.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

/// Represents a storage for table function timeSeriesSelector().
class StorageTimeSeriesSelector : public StorageWithCommonVirtualColumns
{
public:
    struct Configuration
    {
        StorageID time_series_storage_id = StorageID::createEmpty();

        /// Data types of the columns read from table function timeSeriesSelector().
        DataTypePtr id_data_type;
        DataTypePtr timestamp_data_type;
        DataTypePtr scalar_data_type;

        /// The type of the `id` column of the TimeSeries table itself.
        /// It differs from `id_data_type` only when the id is narrowed (see below).
        DataTypePtr table_id_data_type;

        /// Whether the selector reads and returns only the second component of a two-component
        /// id `Tuple(F, LowCardinality(S))` (`id_data_type` is that component's type then).
        /// With the canonical id generator the second component (a hash of all the tags) alone
        /// identifies a time series, and returning only it keeps the identifiers
        /// dictionary-encoded end-to-end: the `IN <ids>` filter over the samples table and
        /// `timeSeriesIdToGroup` are then executed per dictionary key instead of per row.
        bool narrow_id_to_second_component = false;

        PrometheusQueryTree selector;

        /// The scale of these fields is the same as the scale used in `timestamp_data_type`.
        DateTime64 min_time{};
        DateTime64 max_time{};
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context);

    StorageTimeSeriesSelector(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_);

    std::string getName() const override { return "TimeSeriesSelector"; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
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
    LoggerPtr log;
};

}
