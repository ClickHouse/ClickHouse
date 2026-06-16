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
