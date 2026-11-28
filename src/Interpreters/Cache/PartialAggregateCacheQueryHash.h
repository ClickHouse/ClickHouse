#pragma once

#include <Interpreters/Aggregator.h>
#include <Parsers/IASTHash.h>
#include <base/types.h>

#include <optional>

namespace DB
{

class PartialAggregateCache;
using PartialAggregateCachePtr = std::shared_ptr<PartialAggregateCache>;
class QueryPlan;
struct Settings;

/// Plan-time probing in `ReadFromMergeTree` represents a cache hit as a zero-row chunk carrying
/// `PartialAggregatePlanHitInfo`. Inflating transforms such as `ArrayJoinTransform` rebuild the chunk without
/// preserving `ChunkInfos`, and drop a zero-row input entirely, so the hit would silently disappear and the
/// cached part would contribute neither raw rows nor cached states. Fail-close: no plan-time probing for such
/// plans. Execution-time caching is unaffected because it observes the chunks before `ARRAY JOIN`.
bool planHasArrayJoinStep(const QueryPlan & query_plan);

/// Plan-time probing in `ReadFromMergeTree` removes the hit parts from the read and emits the hit chunks instead,
/// so it is correct only when the optimized plan has exactly one aggregation, the aggregation is executed by
/// `AggregatingTransform` (not in-order, not the GROUP BY top-K heap, not the radix partitioned or in-order
/// `uniqExact`), and every `ReadFromMergeTree` of the plan feeds it. Otherwise the query hash must be dropped.
bool planCanConsumePartialAggregatePlanHits(const QueryPlan & query_plan, bool optimize_uniq_exact_radix_partitioning);

/// Single SipHash implementation for `PartialAggregateCache::Key::query_hash` (must stay in sync with
/// `AggregatingTransform` and `ReadFromMergeTree` planning probes).
///
/// When `grouping_set_missing_keys` and `grouping_set_index` are set, appends sorted `missing_keys` and the set index,
/// matching `AggregatingStep::transformPipeline` for `GROUPING SETS`.
/// For plain `GROUP BY`, pass `nullptr` and `std::nullopt`.
std::optional<IASTHash> computePartialAggregateCacheQueryHash(
    const PartialAggregateCachePtr & cache,
    const Aggregator::Params & params,
    bool group_by_use_nulls,
    bool has_sort_description_for_merging,
    const Names * grouping_set_missing_keys,
    std::optional<size_t> grouping_set_index);

/// Plain `GROUP BY` cache key for `BuildQueryPipelineSettings` (no grouping-set tail in the hash).
std::optional<IASTHash> tryComputePartialAggregateCacheQueryHash(
    const Settings & settings,
    const PartialAggregateCachePtr & cache,
    const Aggregator::Params & params,
    bool group_by_use_nulls,
    bool has_sort_description_for_merging);

}
