#pragma once

#include <Common/Logger_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/BlockIO.h>

#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{
class StorageTimeSeries;
class ExpressionActions;
class IColumn;
class PushingPipelineExecutor;
struct TimeSeriesSettings;
using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

/// Sink for inserting data into the TimeSeries table engine.
/// Transforms outer columns (time_series, metric_name, tags, metric_family, type, unit, help)
/// into blocks for the three inner target tables (Tags, Samples, Metrics).
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

    /// Sorts tags by name, removes exact duplicates and tags with empty values,
    /// and throws if the `__name__` tag is missing or appears with conflicting values.
    static void sortTagsAndRemoveDuplicates(std::vector<std::pair<std::string_view, std::string_view>> & tags);

    /// Dispatches one row of already-sorted tags into the appropriate output columns.
    /// Tags matching a key in `columns_by_tag_name` go to that column; the rest go to `out_tags_names`/`out_tags_values`.
    /// The optional `all_tags_*` columns (pass `nullptr` to skip) receive every non-`__name__` tag.
    static void insertSortedTagsToColumns(
        const std::vector<std::pair<std::string_view, std::string_view>> & sorted_tags,
        IColumn & out_metric_name_column,
        IColumn & out_tags_names,
        IColumn & out_tags_values,
        IColumn & out_tags_offsets,
        std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name,
        IColumn * all_tags_names,
        IColumn * all_tags_values,
        IColumn * all_tags_offsets);

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
    void initMetricsPipeline();
    std::unique_ptr<TargetPipeline> createTargetPipeline(ViewTarget::Kind kind, const Block & header);

    void consumeTagsAndSamples(const Block & block);
    void consumeMetrics(const Block & block);

    /// Calculates the "id" column by applying id_generator defaults and type conversion to the tags block.
    ColumnPtr calculateId(const Block & tags_block) const;

    StorageTimeSeries & time_series_storage;
    TimeSeriesSettingsPtr time_series_settings;
    LoggerPtr log;

    bool insert_tags_and_samples = false;
    bool insert_metrics = false;
    bool async_insert = false;

    /// Precomputed ExpressionActions for calculating the "id" column from a tags block.
    std::shared_ptr<ExpressionActions> calculate_id_actions;
    std::shared_ptr<ExpressionActions> convert_id_actions;

    std::unique_ptr<TargetPipeline> tags_pipeline;
    std::unique_ptr<TargetPipeline> samples_pipeline;
    std::unique_ptr<TargetPipeline> metrics_pipeline;
};

}
