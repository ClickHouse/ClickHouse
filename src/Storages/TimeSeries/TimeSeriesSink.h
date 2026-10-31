#pragma once

#include <Columns/IColumn.h>
#include <Common/Logger_fwd.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>
#include <base/Decimal.h>

#include <optional>


namespace DB
{
class StorageTimeSeries;
class ExpressionActions;
class IColumn;
class PushingPipelineExecutor;
struct TimeSeriesSettings;
using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

/// Sink for inserting data into the TimeSeries table engine.
/// Transforms outer columns (samples, metric_name, tags, metric_family, type, unit, help)
/// into blocks for the target tables (Tags, TimeRanges, Samples, RecentSamples, MetricFamilies).
class TimeSeriesSink : public SinkToStorage, WithContext
{
public:
    /// `insert_columns` contains column names from the INSERT query,
    /// empty `insert_columns` means all columns from `header_`.
    TimeSeriesSink(
        StorageTimeSeries & time_series_storage_,
        const Block & header_,
        const Names & insert_columns_,
        ContextPtr context_,
        bool async_insert_);

    String getName() const override { return "TimeSeriesSink"; }

    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    /// A persistent pipeline for inserting blocks into one target table.
    struct TargetPipeline
    {
        ContextMutablePtr context;
        BlockIO io;
        std::unique_ptr<PushingPipelineExecutor> executor;
        std::shared_ptr<ExpressionActions> converting_actions;

        void push(Block block) const;
        ~TargetPipeline();
    };

    void initTagsAndSamplesPipelines();
    void initMetricFamiliesPipeline();
    std::unique_ptr<TargetPipeline> createTargetPipeline(ViewTarget::Kind kind, const Block & header);

    void consumeTagsAndSamples(const Block & block);
    void consumeMetricFamilies(const Block & block);

    /// Makes the tags block from the time series marked in `filter` (`num_time_series` of them): the `id` column calculated
    /// by the id generator, the columns `metric_name` and the columns from the `tags_to_columns` setting, `tags`,
    /// and `min_time`, `max_time` if the tags table stores them (tables of earlier versions).
    Block makeTagsBlock(
        const IColumn::Filter & filter,
        size_t num_time_series,
        const IColumn & metric_name_column,
        const IColumn::Offsets & tags_offsets,
        const IColumn & tags_keys,
        const IColumn & tags_values,
        const IColumn::Offsets & ts_offsets,
        const IColumn & ts_timestamps) const;

    StorageTimeSeries & time_series_storage;
    TimeSeriesSettingsPtr time_series_settings;
    LoggerPtr log;

    bool insert_tags_and_samples = false;
    bool insert_metric_families = false;
    bool async_insert = false;

    /// Source header for the tags pipeline WITHOUT the `id` column.
    Block tags_header_before_id;

    /// Type of the `id` column in the tags target table.
    DataTypePtr id_type;

    /// Types of the timestamps and the values of the samples, taken from the input header (see initTagsAndSamplesPipelines).
    DataTypePtr timestamp_type;
    DataTypePtr value_type;

    /// True when the resolved id-generator references the `all_tags` identifier.
    bool id_generator_uses_all_tags = false;

    /// Precomputed ExpressionActions for calculating the "id" column from a tags block.
    std::shared_ptr<ExpressionActions> calculate_id_actions;
    std::shared_ptr<ExpressionActions> convert_id_actions;

    /// The steps of the buckets of the samples table and of the recent samples table
    /// with the scale of `timestamp_type`, e.g. in milliseconds for `DateTime64(3)`.
    /// Here zero means a table of an earlier version storing one sample per row;
    /// and std::nullopt means there is no `recent_samples` table.
    Decimal64 samples_bucket_step{0};
    std::optional<Decimal64> recent_samples_bucket_step;

    std::unique_ptr<TargetPipeline> tags_pipeline;
    std::unique_ptr<TargetPipeline> time_ranges_pipeline;
    std::unique_ptr<TargetPipeline> samples_pipeline;
    std::unique_ptr<TargetPipeline> recent_samples_pipeline;
    std::unique_ptr<TargetPipeline> metric_families_pipeline;

    /// Skip the rows already written to the "tags" and "metric families" tables, null if the corresponding cache is disabled.
    TimeSeriesDeduplicationCachePtr tags_deduplication_cache;
    TimeSeriesDeduplicationCachePtr metric_families_deduplication_cache;

    /// Rows of the "tags" and "metric families" tables which this insert is going to write, they are marked as written when the insert is finished.
    TimeSeriesDeduplicationCache::PendingRows pending_tags;
    TimeSeriesDeduplicationCache::PendingRows pending_metric_families;
};

}
