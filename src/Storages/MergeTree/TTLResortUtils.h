#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class QueryPipelineBuilder;
struct MergeTreeSettings;

/// A `TTL ... GROUP BY ... SET col = agg(...)` clause can assign a column that the table's
/// sorting key depends on (directly, or through an expression such as `toStartOfDay(ts)`).
/// `TTLAggregationAlgorithm` emits aggregated groups in the input (already-sorted) order, so
/// when such a SET rewrites a sort-key column the produced stream is no longer ordered by the
/// sorting key, and a sort-key expression column materialized before the TTL step still holds its
/// pre-`SET` value. Both the merge writer and the mutation writer take the primary-key columns out
/// of the stream by name and build the index from them in stream order, so the resulting part would
/// have an index inconsistent with its data: a directly assigned sort-key column breaks the order
/// itself (a `CheckSortedTransform` catches it as a LOGICAL_ERROR in debug builds; release builds
/// write a corrupt part), while a stale expression column stays ordered and prunes wrongly. The
/// merge (MergeTask) and mutation (MutateTask, e.g. MATERIALIZE TTL) pipelines both use this to
/// decide whether they must recompute the sorting key and re-sort.
///
/// A `SET` target is always a physical storage column, while a sorting-key dependency can be a
/// subcolumn (e.g. `ORDER BY t.a` requires `t.a`, whose storage column is `t`). Each dependency
/// is mapped to its storage column before comparing, the same way `extractMergingAndGatheringColumns`
/// does via `getColumnNameInStorage`.
///
/// `set_targets` are the columns assigned by the `GROUP BY` TTLs that actually fire in this
/// merge/mutation (`getFiringGroupByTTLSetTargets`). Returns false when it is empty, so a
/// not-yet-expired `GROUP BY ... SET` on the sort key does not force a whole-part re-sort.
bool groupByTTLAssignsSortKeyColumn(const StorageMetadataPtr & metadata_snapshot, const NameSet & set_targets);

/// The physical `SET` target columns of ONLY the `GROUP BY` TTLs that can actually fire in a part
/// with these TTL infos at `current_time` (a not-yet-expired `GROUP BY ... SET` contributes nothing).
/// Used by the merge and mutation paths to gate the sort-key re-sort on the columns a FIRING `SET`
/// rewrites, rather than on "some GROUP BY TTL fired somewhere":
/// otherwise a part with a firing `TTL1 GROUP BY k SET payload` and a not-yet-expired
/// `TTL2 GROUP BY ... SET ts` (the only clause touching the sort key) would pay a whole-part re-sort
/// for a `SET` that never ran. `min == 0` (uninitialized info) or a missing info is treated
/// conservatively as "may fire". A forced merge is not proof that a TTL fired: it only requires
/// row-by-row evaluation, and a future TTL can still leave every row unchanged.
///
/// Empty for a table with more than one `GROUP BY` TTL, so no repair runs for such a part at all:
/// an earlier `SET` can rewrite a column a later TTL groups by, which makes that TTL aggregate an
/// input no longer ordered by its keys and merge rows that belong to different groups. That is a
/// separate defect, and re-sorting the result would only hide it behind a correctly ordered part, so
/// such a part is left exactly as it is written without this repair.
///
/// `force_ttl` says an input's TTL info is not calculated at all. Combined part infos cannot express
/// a missing entry (`MergeTreeDataPartTTLInfos::update` merges only the entries a part has), so one
/// input's future minimum would otherwise hide another input's expired rows: assume every TTL fires.
NameSet getFiringGroupByTTLSetTargets(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time,
    bool force_ttl);

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
