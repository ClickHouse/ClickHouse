#pragma once

#include <Common/Logger.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

struct TimeSeriesSettings;

/// Represents a storage for table function timeSeriesSelector().
class StorageTimeSeriesSelector : public StorageWithCommonVirtualColumns
{
public:
    enum class SamplesReadOrder
    {
        Unordered,
        IdBucket,
    };

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
        DateTime64 min_time{};
        DateTime64 max_time{};
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context);

    StorageTimeSeriesSelector(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_);

    std::string getName() const override { return "TimeSeriesSelector"; }

    static VirtualColumnsDescription createVirtuals();

    /// Makes a SELECT query for the ids (`series_id`) of the series matching the matchers and optional time bounds (need stored min_time/max_time), registering their tags for timeSeriesIdToTags().
    static ASTPtr makeSelectIDsQuery(
        const StorageID & tags_table_id,
        const PrometheusQueryTree::MatcherList & matchers,
        const TimeSeriesSettings & time_series_settings,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        const DataTypePtr & timestamp_data_type);

    /// Return whether the samples metadata describes an ascending `(id, bucket, ...)` sorting key.
    static bool hasSamplesIdBucketOrder(const StorageMetadataPtr & samples_table_metadata);

    /// Return whether the selected samples table has the physical order required by an ordered native read.
    static bool canReadSamplesInOrder(const StoragePtr & samples_table, const StorageMetadataPtr & samples_table_metadata);

    /// Build the selector plan, optionally preserving the samples table's `(id, bucket)` order.
    /// Returns false when ordered mode is requested but the selected samples table cannot provide that order.
    bool buildQueryPlan(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams,
        SamplesReadOrder samples_read_order,
        bool enable_whole_metric_id_range_optimization = true);

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
