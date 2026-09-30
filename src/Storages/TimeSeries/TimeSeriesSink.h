#pragma once

#include <Common/Logger_fwd.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Chain.h>
#include <Storages/TimeSeries/TimeSeriesDeduplicationCache.h>

#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{
class StorageTimeSeries;
class ExpressionActions;
class IColumn;
struct TimeSeriesSettings;
using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

Chain buildTimeSeriesWriteChain(
    StorageTimeSeries & storage,
    const SharedHeader & input_header,
    const ASTPtr & query,
    ContextPtr context,
    bool async_insert);

SinkToStoragePtr wrapTimeSeriesWriteChain(Chain chain);

/// Builds blocks for the TimeSeries target tables and the insert chains that write them.
class TimeSeriesSink : public WithContext
{
public:
    struct Target
    {
        Chain chain;
        std::shared_ptr<ExpressionActions> converting_actions;
        SharedHeader output_header;
        bool is_tags = false;
        bool is_samples = false;
        bool is_recent_samples = false;
        bool is_metric_families = false;
    };

    struct PreparedChunk
    {
        Chunk passthrough;
        std::vector<Chunk> branches;
    };

    TimeSeriesSink(
        StorageTimeSeries & time_series_storage_,
        const Block & header_,
        const Names & insert_columns_,
        ContextPtr context_,
        bool async_insert_);

    const Block & getHeader() const { return header; }
    const std::vector<Target> & getTargets() const { return targets; }
    std::vector<Target> & getTargets() { return targets; }

    void buildChains();
    PreparedChunk prepareChunk(Chunk chunk);

    void markTagsWritten();
    void markMetricFamiliesWritten();

    /// Sorts tags by name, removes exact duplicates and tags with empty values,
    /// and throws if the `__name__` tag is missing or appears with conflicting values.
    static void sortTagsAndRemoveDuplicates(std::vector<std::pair<std::string_view, std::string_view>> & tags);

    /// Dispatches one row of already-sorted tags into the appropriate output columns.
    /// Every tag goes to `out_tags_names`/`out_tags_values`; tags matching a key in `columns_by_tag_name`
    /// are also copied to the corresponding column.
    static void insertSortedTagsToColumns(
        const std::vector<std::pair<std::string_view, std::string_view>> & sorted_tags,
        IColumn & out_tags_names,
        IColumn & out_tags_values,
        IColumn & out_tags_offsets,
        std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name);

private:
    Target createTarget(
        ViewTarget::Kind kind,
        const Block & source_header,
        bool is_tags,
        bool is_samples,
        bool is_recent_samples,
        bool is_metric_families);

    void initTagsAndSamples();
    void initMetricFamilies();

    void consumeTagsAndSamples(const Block & block, Block & tags_block_out, Block & samples_block_out);
    void consumeMetricFamilies(const Block & block, Block & metric_families_block_out);

    Chunk convertToChunk(const Block & block, const Target & target) const;

    /// Calculates the "id" column by applying id_generator defaults and type conversion to the tags block.
    ColumnPtr calculateId(const Block & tags_block) const;

    StorageTimeSeries & time_series_storage;
    TimeSeriesSettingsPtr time_series_settings;
    LoggerPtr log;
    Block header;

    bool insert_tags_and_samples = false;
    bool insert_metric_families = false;
    bool async_insert = false;
    bool has_recent_samples = false;

    /// Source header for the tags chain WITHOUT the `id` column.
    Block tags_header_before_id;
    Block tags_source_header;
    Block samples_source_header;
    Block metric_families_source_header;

    /// Type of the `id` column in the tags target table.
    DataTypePtr id_type;

    /// True when the resolved id-generator references the `all_tags` identifier.
    bool id_generator_uses_all_tags = false;

    /// Precomputed ExpressionActions for calculating the "id" column from a tags block.
    std::shared_ptr<ExpressionActions> calculate_id_actions;
    std::shared_ptr<ExpressionActions> convert_id_actions;

    std::vector<Target> targets;

    /// Skip the rows already written to the "tags" and "metric families" tables, null if the corresponding cache is disabled.
    TimeSeriesDeduplicationCachePtr tags_deduplication_cache;
    TimeSeriesDeduplicationCachePtr metric_families_deduplication_cache;

    /// Rows of the "tags" and "metric families" tables which this insert is going to write, they are marked as written when the insert is finished.
    TimeSeriesDeduplicationCache::PendingRows pending_tags;
    TimeSeriesDeduplicationCache::PendingRows pending_metric_families;
};

}
