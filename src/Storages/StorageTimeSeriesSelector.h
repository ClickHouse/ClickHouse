#pragma once

#include <Common/Logger.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class StorageTimeSeries;
struct TimeSeriesSettings;

/// Represents a storage for table function timeSeriesSelector().
class StorageTimeSeriesSelector : public StorageWithCommonVirtualColumns
{
public:
    struct Configuration
    {
        StorageID time_series_storage_id = StorageID::createEmpty();

        /// Data types of the columns `id`, `timestamp` and `value` in the TimeSeries table.
        /// The columns returned by table function timeSeriesSelector() have these data types.
        DataTypePtr table_id_type;
        DataTypePtr table_timestamp_type;
        DataTypePtr table_value_type;

        PrometheusQueryTree selector;

        /// The scale of `min_time` and `max_time`: the scale of `table_timestamp_type`, but not less than the scale
        /// of the arguments `min_time` and `max_time` of the table function and not less than 3 (milliseconds).
        /// The time range is converted to the scale of the table when the samples are read.
        UInt32 time_scale = 0;
        DateTime64 min_time{};
        DateTime64 max_time{};
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context);

    StorageTimeSeriesSelector(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_);

    std::string getName() const override { return "TimeSeriesSelector"; }

    static VirtualColumnsDescription createVirtuals();

    /// Describes how to filter time series by their stored time ranges (`min_time`, `max_time`) when selecting their identifiers.
    struct TimeRangeFilter
    {
        /// The bounds of the requested time range. Unset if the table doesn't store time ranges: then the range is ignored,
        /// and the identifiers are filtered by the timestamps of the samples only.
        std::optional<DateTime64> min_time;
        std::optional<DateTime64> max_time;

        /// The "time ranges" table storing `min_time` and `max_time` of every time series (tables of version 7 and later).
        /// Empty if the "tags" table stores these columns itself (tables of the earlier versions).
        StorageID time_ranges_table_id = StorageID::createEmpty();
    };

    /// Makes the filter for a TimeSeries table: the bounds are kept only if the table stores time ranges.
    /// The bounds must have the scale of the timestamps in the table.
    static TimeRangeFilter makeTimeRangeFilter(
        const StorageTimeSeries & time_series_storage,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        const ContextPtr & context);

    /// Makes a SELECT query for the ids (`series_id`) of the series matching the matchers and optional time bounds,
    /// registering their tags for timeSeriesIdToTags(). The time bounds have the scale `time_scale`; they are converted
    /// to the type of the timestamps in the table and applied to the stored time ranges of the series if the table
    /// stores them (see TimeRangeFilter). The query reads from a `null` table (returns no ids) if no timestamp of the
    /// table is in the time range.
    static ASTPtr makeSelectIDsQuery(
        const StorageTimeSeries & time_series_storage,
        const StorageID & tags_table_id,
        const TimeSeriesSettings & time_series_settings,
        const DataTypePtr & table_timestamp_type,
        const DataTypePtr & table_id_type,
        const PrometheusQueryTree::MatcherList & matchers,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        UInt32 time_scale,
        const ContextPtr & context);

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
