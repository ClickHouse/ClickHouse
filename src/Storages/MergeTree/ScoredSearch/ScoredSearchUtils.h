#pragma once
#include <Core/Block_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>
#include <roaring/roaring.hh>

namespace DB
{

class ActionsDAG;
class QueryPipelineBuilder;
class RowScorer;
struct SelectQueryInfo;

/// Per-part prefilter bitmaps, indexed by `part_index_in_query`.
/// A non-null but empty bitmap means no rows of that part survived the WHERE clause.
using PerPartBitmaps = std::vector<std::shared_ptr<roaring::Roaring>>;

/// Shared state for the WHERE-clause bitmap subquery.
/// An empty `subquery_plan` means no WHERE clause arrived;
/// Filled by DelayedCreatingBitmapsStep, consumed by scorers.
struct LazyBitmapSubqueryState
{
    std::optional<QueryPlan> subquery_plan;
    std::optional<PerPartBitmaps> bitmaps;
    UInt64 rows_budget = 0;
};

using LazyBitmapSubqueryStatePtr = std::shared_ptr<LazyBitmapSubqueryState>;

/// Builds a `QueryPlan` for the bitmap subquery:
/// SELECT _part_index, _part_offset FROM <source_table> WHERE <where_clause>
QueryPlan buildBitmapSubquery(
    const MergeTreeData & source_merge_tree,
    RangesInDataPartsPtr ranges,
    MergeTreeData::MutationsSnapshotPtr mutations,
    ActionsDAG where_clause,
    const String & filter_column_name,
    const StorageSnapshotPtr & source_snapshot,
    const SelectQueryInfo & outer_query_info,
    ContextPtr context);

/// Assemble the row-scorer top-K pipeline:
/// `num_streams x ScorerSource` -> `MergingSortedTransform` -> `LimitTransform(k)`.
/// Each source emits a single chunk sorted by the scorer comparator.
void buildScoredTopKPipeline(
    RangesInDataParts ranges_in_data_parts,
    std::shared_ptr<RowScorer> scorer,
    const std::optional<PerPartBitmaps> & bitmaps,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    StorageMetadataPtr source_metadata,
    SharedHeader output_header,
    const SelectQueryInfo & query_info,
    size_t num_streams,
    ContextPtr context,
    QueryPipelineBuilder & pipeline);

}
