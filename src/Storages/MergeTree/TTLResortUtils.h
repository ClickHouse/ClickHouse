#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class QueryPipelineBuilder;
class ActionsDAG;
class Block;
struct MergeTreeSettings;

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
///
/// The `SET` can also rewrite a column that a MATERIALIZED sort-key column is computed from
/// (e.g. `d Date MATERIALIZED toDate(ts)`, `ORDER BY d`, `... GROUP BY d SET ts = ...`): the
/// aggregation updates `ts` but leaves the stored `d` on its pre-`SET` value, so the part is
/// written with stale sort-key data. Such MATERIALIZED sort-key columns must be recomputed from
/// their default expression before re-sorting; this function returns true for that case too.
bool groupByTTLAssignsSortKeyColumn(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

/// The MATERIALIZED sort-key storage columns whose source columns are rewritten by a
/// `TTL ... GROUP BY ... SET` (e.g. `d` for `d MATERIALIZED toDate(ts)` when `ts` is SET). These
/// hold stale values in the post-TTL stream and must be recomputed from their default expression
/// before the sorting key is recomputed and the stream re-sorted. Empty when the `SET` only
/// rewrites sort-key columns directly.
NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

/// Build an `ActionsDAG` over `header` that drops the stale values of `columns_to_recompute` and
/// recomputes them from their MATERIALIZED default expressions (reading the post-`SET` source
/// columns already present in the stream). All other columns pass through unchanged. Used by both
/// the merge and mutation paths to recompute stale MATERIALIZED sort-key columns before re-sorting.
ActionsDAG buildRecomputeMaterializedColumnsDAG(
    const Block & header,
    const NamesAndTypesList & columns_to_recompute,
    const ColumnsDescription & columns_desc,
    const ContextPtr & context);

/// Sort settings for the re-sort after a `TTL ... GROUP BY ... SET` that rewrites a sort-key
/// column. Background merge and mutation contexts keep the default
/// `max_bytes_before_external_sort = 0`, which disables spilling entirely, so a plain
/// `SortingStep::Settings(context->getSettingsRef())` would buffer the whole post-TTL part in
/// memory (`TTLTransform` passes non-expired rows through unchanged, so on a large merge or
/// `ALTER TABLE ... MATERIALIZE TTL` that is the entire part). Bound the sort by the
/// `ttl_resort_max_bytes_before_external_sort` MergeTree setting instead: past the threshold,
/// sorted runs are spilled to the temporary storage on disk (taken from the global context, so
/// it is available to background operations) and merged back in a streaming fashion.
SortingStep::Settings buildTTLResortSortingSettings(const ContextPtr & context, const MergeTreeSettings & storage_settings);

/// Recompute the sorting-key expression columns from the post-`SET` values and re-sort the
/// pipeline by the sorting key. Used by the mutation path (e.g. `MATERIALIZE TTL`) after a
/// `TTL ... GROUP BY ... SET` step that rewrites a sort-key column, so the written part is
/// ordered consistently with its primary index. The merge path (MergeTask) does the equivalent
/// directly on its `QueryPlan`. Call only when `groupByTTLAssignsSortKeyColumn` returns true.
void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context,
    const MergeTreeSettings & storage_settings);

}
