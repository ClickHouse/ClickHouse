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

    /// Makes a SELECT query over the tags table of a TimeSeries table returning the ids (aliased as
    /// `series_id`) of the series matching a list of PromQL label matchers. As a side effect of
    /// executing that query the tags of each matched series are registered in the query context
    /// under its id (see functions timeSeriesStoreTags and timeSeriesIdToTags). If `min_time` or
    /// `max_time` is set, only series whose time range overlaps [min_time, max_time] are returned;
    /// the caller must ensure that the tags table stores the `min_time` and `max_time` columns
    /// (see the TimeSeries setting `store_min_time_and_max_time`).
    static ASTPtr makeSelectIDsQuery(
        const StorageID & tags_table_id,
        const PrometheusQueryTree::MatcherList & matchers,
        const TimeSeriesSettings & time_series_settings,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        const DataTypePtr & timestamp_data_type);

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
