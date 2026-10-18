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
    /// What the table function returns: the optional last argument `mode`.
    enum class Mode
    {
        /// The samples of the matching series: columns `id`, `timestamp`, `value`.
        SAMPLES,

        /// The samples of the matching series whose set of tags is stored under several matching ids.
        /// Nothing is read from the samples table when every matching set of tags is stored under one id.
        SAMPLES_OF_IDS_SHARING_TAGS,

        /// The ids of the matching series whose set of tags is stored under several matching ids: column `id`.
        IDS_SHARING_TAGS,
    };

    static constexpr std::string_view MODE_SAMPLES_OF_IDS_SHARING_TAGS = "samples_of_ids_sharing_tags";
    static constexpr std::string_view MODE_IDS_SHARING_TAGS = "ids_sharing_tags";

    struct Configuration
    {
        StorageID time_series_storage_id = StorageID::createEmpty();
        Mode mode = Mode::SAMPLES;

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

    /// Makes a SELECT query for the ids (`series_id`) of the series matching the matchers and optional time bounds,
    /// registering their tags for timeSeriesIdToTags(). The time bounds have the scale `time_scale`; they are converted
    /// to the type of the timestamps in the table and applied to the stored time ranges of the series if the table
    /// stores them. The query reads from a `null` table (returns no ids) if no timestamp of the table is in the time range.
    /// The query must be executed with the settings set by applySettingsForSelectIDsQuery.
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

    /// Sets the settings the query made by makeSelectIDsQuery relies on: the time ranges table is joined by a merge join
    /// over tables read in the order of `id`.
    static void applySettingsForSelectIDsQuery(Context & context);

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
