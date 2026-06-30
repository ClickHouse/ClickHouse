#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class QueryPipelineBuilder;

/// A `TTL ... GROUP BY ... SET col = agg(...)` clause can assign a column that the table's
/// sorting key depends on (directly, or through an expression such as `toStartOfDay(ts)`).
/// `TTLAggregationAlgorithm` emits aggregated groups in the input (already-sorted) order, so
/// when such a SET rewrites a sort-key column the produced stream is no longer ordered by the
/// sorting key. Both the merge writer and the mutation writer trust the stream order and build
/// the primary index from it, so the resulting part would have an index inconsistent with the
/// data (a `CheckSortedTransform` catches it as a LOGICAL_ERROR in debug builds; release builds
/// write a corrupt part). The merge (MergeTask) and mutation (MutateTask, e.g. MATERIALIZE TTL)
/// pipelines both use this to decide whether they must recompute the sorting key and re-sort.
///
/// A `SET` target is always a physical storage column, while a sorting-key dependency can be a
/// subcolumn (e.g. `ORDER BY t.a` requires `t.a`, whose storage column is `t`). Each dependency
/// is mapped to its storage column before comparing, the same way `extractMergingAndGatheringColumns`
/// does via `getColumnNameInStorage`.
bool groupByTTLAssignsSortKeyColumn(const StorageMetadataPtr & metadata_snapshot);

/// Recompute the sorting-key expression columns from the post-`SET` values and re-sort the
/// pipeline by the sorting key. Used by the mutation path (e.g. `MATERIALIZE TTL`) after a
/// `TTL ... GROUP BY ... SET` step that rewrites a sort-key column, so the written part is
/// ordered consistently with its primary index. The merge path (MergeTask) does the equivalent
/// directly on its `QueryPlan`. Call only when `groupByTTLAssignsSortKeyColumn` returns true.
void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context);

}
