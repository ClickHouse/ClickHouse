#pragma once

#include <Interpreters/Aggregator.h>
#include <Parsers/IASTHash.h>
#include <base/types.h>

#include <optional>

namespace DB
{

class PartialAggregateCache;
using PartialAggregateCachePtr = std::shared_ptr<PartialAggregateCache>;
struct Settings;

/// Single SipHash implementation for `PartialAggregateCache::Key::query_hash` (must stay in sync with
/// `AggregatingTransform` and `ReadFromMergeTree` planning probes).
///
/// When `grouping_set_missing_keys` and `grouping_set_index` are set, appends sorted `missing_keys` and the set index,
/// matching `AggregatingStep::transformPipeline` for `GROUPING SETS`.
/// For plain `GROUP BY`, pass `nullptr` and `std::nullopt`.
std::optional<IASTHash> computePartialAggregateCacheQueryHash(
    const PartialAggregateCachePtr & cache,
    const Aggregator::Params & params,
    const Block & input_header,
    bool group_by_use_nulls,
    bool has_sort_description_for_merging,
    const Names * grouping_set_missing_keys,
    std::optional<size_t> grouping_set_index);

/// Plain `GROUP BY` cache key for `BuildQueryPipelineSettings` (no grouping-set tail in the hash).
std::optional<IASTHash> tryComputePartialAggregateCacheQueryHash(
    const Settings & settings,
    const PartialAggregateCachePtr & cache,
    const Aggregator::Params & params,
    const Block & input_header,
    bool group_by_use_nulls,
    bool has_sort_description_for_merging);

}
