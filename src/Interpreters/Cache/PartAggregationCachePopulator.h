#pragma once

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class MergeTreeData;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/// Describes one intermediate step between ReadFromMergeTree and AggregatingStep.
/// Can be either an expression (pure transformation) or a filter (transformation + row filtering).
struct IntermediateStepAction
{
    ExpressionActionsPtr actions;
    String filter_column_name; /// non-empty means this is a filter step
    bool remove_filter_column = false; /// whether the original FilterStep removed the filter column from output
};

/// Build the cache key for one part. The key salts the query hash with the exact mark ranges that
/// were (or will be) aggregated for the part, because the cached state covers only those ranges.
/// The selected ranges are the result of primary key / partition / skip-index analysis and can be
/// narrowed by mechanisms that are not otherwise represented in the key (a source-level filter, a
/// projection-based part/range filter, top-K pruning). Hashing them keeps `{query_hash, table_id,
/// part_name}` from aliasing two reads of the same part that covered different ranges.
PartAggregationCache::Key makePartAggregationCacheKey(
    const IASTHash & query_hash,
    const String & table_id,
    const RangesInDataPart & part);

void populatePartAggregationCache(
    const PartAggregationCachePtr & cache,
    const IASTHash & query_hash,
    const String & table_id,
    const RangesInDataParts & parts,
    const Aggregator::Params & params,
    const Block & aggregator_header,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    const std::vector<IntermediateStepAction> & intermediate_actions = {});

}
