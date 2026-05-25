#pragma once

#include <Core/Block.h>
#include <IO/Progress.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Parsers/IASTHash.h>

#include <cstddef>
#include <optional>


namespace DB
{

struct Settings;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
struct ITemporaryFileLookup;
using TemporaryFileLookupPtr = std::shared_ptr<ITemporaryFileLookup>;
using BlockMarshallingCallback = std::function<Block(const Block & block)>;

struct BuildQueryPipelineSettings
{
    explicit BuildQueryPipelineSettings(ContextPtr from);

    bool enable_multiple_filters_transforms_for_and_chain;

    ExpressionActionsSettings actions_settings;
    QueryStatusPtr process_list_element;
    ProgressCallback progress_callback;
    TemporaryFileLookupPtr temporary_file_lookup;
    BlockMarshallingCallback block_marshalling_callback;

    size_t max_threads;
    size_t aggregation_memory_efficient_merge_threads;
    size_t min_outstreams_per_resize_after_split;
    size_t max_streams_for_union_step;
    double max_streams_for_union_step_to_max_threads_ratio;

    bool use_partial_aggregate_cache;
    bool partial_aggregate_cache_allow_parallel_aggregation_streams;

    /// Plan-time `PartialAggregateCache` probe in `ReadFromMergeTree`. Unset for `GROUPING SETS` or multiple plain `AggregatingStep`s.
    std::optional<IASTHash> partial_aggregate_cache_query_hash;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }
};

}
