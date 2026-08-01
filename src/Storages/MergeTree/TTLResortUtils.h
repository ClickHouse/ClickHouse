#pragma once

#include <optional>

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class QueryPipelineBuilder;
class ActionsDAG;
class Block;
struct MergeTreeSettings;

/// True when any of `group_by_keys` (primary-key column NAMES, possibly computed such as
/// `toStartOfDay(ts)` or a subcolumn such as `t.a`) depends on a physical storage column present in
/// `earlier_set_targets` (the columns assigned by earlier `GROUP BY` TTL `SET`s in the same
/// `TTLTransform`). When true, this TTL's `TTLAggregationAlgorithm` input is no longer ordered by its
/// keys (the earlier `SET` rewrote a column the key derives from), so it must NOT take the streaming
/// flush-on-key-change fast path. A raw name comparison is insufficient: the key can be a computed or
/// subcolumn expression while the `SET` target is always a physical column.
bool groupByKeysAffectedByEarlierSet(
    const Names & group_by_keys,
    const NameSet & earlier_set_targets,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context);

/// True when this `GROUP BY` TTL's expiry (or WHERE) expression reads a physical storage column in
/// `earlier_set_targets` (the `SET` targets of earlier FIRING `GROUP BY` TTLs in the same
/// `TTLTransform`). The per-part precomputed `group_by_ttl.min` proves "won't fire" only for the
/// UNMODIFIED part; once an earlier `SET` rewrites a column this TTL's expiry depends on, that proof
/// is void and this TTL may now fire in the same run. Callers then treat it as firing (conservative),
/// so it is not wrongly kept on the streaming fast path / excluded from the merge repairs. A `SET`
/// target is always a physical column; subcolumn reads are mapped to their storage parent, and an
/// expiry that reads a MATERIALIZED column derived from a `SET` target (e.g. `d MATERIALIZED
/// toDate(ts2)`, expiry `d + 1d`, earlier `SET ts2`) is detected via the materialized dependency graph.
bool groupByTTLExpiryAffectedByEarlierSet(
    const TTLDescription & group_by_ttl,
    const NameSet & earlier_set_targets,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context);

/// Build an `ActionsDAG` over `header` that refreshes the derived columns whose in-stream value went
/// stale after an earlier `GROUP BY` TTL `SET`, so this TTL's `TTLAggregationAlgorithm` sees post-`SET`
/// values. Two kinds of staleness are repaired:
///  - `group_by_keys`: computed/subcolumn keys are recomputed from the primary-key expression, and a
///    MATERIALIZED column used as a key is recomputed from its default expression (together with its
///    transitive affected MATERIALIZED sources). Otherwise the aggregation would group by the pre-`SET`
///    key value.
///  - the columns this TTL's expiry/`WHERE` expression reads: a MATERIALIZED expiry input derived from
///    a `SET` target (e.g. `d MATERIALIZED toDate(ts2)`, expiry `d + 1d`, earlier `SET ts2`) still holds
///    its pre-`SET` value, so `isTTLExpired` would read the stale `d` and wrongly skip aggregation.
/// Returns nullopt when nothing needs refreshing (all keys are plain physical columns and no derived
/// expiry input is affected). Applied before the later `TTLAggregationAlgorithm` consumes the block.
std::optional<ActionsDAG> buildRefreshGroupByKeysDAG(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot,
    const TTLDescription & group_by_ttl,
    const ContextPtr & context);

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

/// As above, but restricted to the given `set_targets` (typically the `SET` targets of only the
/// `GROUP BY` TTLs that actually fire in this merge, from `getFiringGroupByTTLSetTargets`). Returns
/// false when `set_targets` is empty, so a not-yet-expired `GROUP BY ... SET` on the sort key does
/// not force a whole-part re-sort.
bool groupByTTLAssignsSortKeyColumn(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets);

/// The physical `SET` target columns of ONLY the `GROUP BY` TTLs that can actually fire in a part
/// with these TTL infos at `current_time` (a not-yet-expired `GROUP BY ... SET` contributes nothing).
/// Used by the merge path to gate each repair (materialized recompute, sort-key re-sort, ephemeral
/// warning) on the columns a FIRING `SET` rewrites, rather than "some GROUP BY TTL fired somewhere":
/// otherwise a part with a firing `TTL1 GROUP BY k SET payload` and a not-yet-expired
/// `TTL2 GROUP BY ... SET ts` (the only clause touching the sort key) would pay a whole-part re-sort
/// for a `SET` that never ran. `force` (MATERIALIZE TTL / forced merge) returns all `SET` targets.
/// `min == 0` (uninitialized info) or a missing info is treated conservatively as "may fire".
NameSet getFiringGroupByTTLSetTargets(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time,
    bool force,
    const ContextPtr & context);

/// The MATERIALIZED sort-key storage columns whose source columns are rewritten by a
/// `TTL ... GROUP BY ... SET` (e.g. `d` for `d MATERIALIZED toDate(ts)` when `ts` is SET). These
/// hold stale values in the post-TTL stream and must be recomputed from their default expression
/// before the sorting key is recomputed and the stream re-sorted. Empty when the `SET` only
/// rewrites sort-key columns directly.
NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

/// As above, restricted to the given `set_targets` (the firing `GROUP BY` TTLs' `SET` targets).
NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets);

/// EVERY MATERIALIZED storage column whose default expression (transitively) reads a column
/// rewritten by a `TTL ... GROUP BY ... SET` -- not only the sort-key subset. `TTLAggregationAlgorithm`
/// keeps such a column as `any(col)` from the pre-`SET` rows, so its stored value, and any rebuilt
/// skip index / projection that reads it, would be written stale (wrong data, not just a missed
/// optimization). Both the merge and mutation paths recompute these from their default expression
/// before the part is written. Empty when no MATERIALIZED column depends on a `SET` target.
NamesAndTypesList getGroupByTTLSetAffectedMaterializedColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

/// As above, restricted to the given `set_targets` (the firing `GROUP BY` TTLs' `SET` targets), so a
/// MATERIALIZED column affected only by a not-yet-expired `GROUP BY ... SET` is not recomputed.
NamesAndTypesList getGroupByTTLSetAffectedMaterializedColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets);

/// The MATERIALIZED columns whose default expression reads BOTH an EPHEMERAL column and a column
/// rewritten by a `TTL ... GROUP BY ... SET`. Such a column cannot be recomputed during merge/mutation
/// (ephemeral columns are only available at INSERT, never read from disk), so its stored value goes
/// stale with no way to refresh it. Callers warn about each (mirroring `MutationsInterpreter::prepare`
/// for `UPDATE`) rather than silently writing a stale value. Empty when no such column exists.
Names getStaleEphemeralMaterializedColumnsAffectedBySet(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

/// As above, restricted to the given `set_targets` (the firing `GROUP BY` TTLs' `SET` targets), so a
/// not-yet-expired `GROUP BY ... SET` does not trigger a spurious warning.
Names getStaleEphemeralMaterializedColumnsAffectedBySet(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets);

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

/// Add a pipeline transform that recomputes every MATERIALIZED column affected by a
/// `TTL ... GROUP BY ... SET` (see `getGroupByTTLSetAffectedMaterializedColumns`) from its default
/// expression, so no stale MATERIALIZED value (and no rebuilt skip index / projection reading it) is
/// written. Used by the mutation path; must run whenever a `GROUP BY` TTL with a `SET` runs, NOT only
/// when a sort-key column is assigned. Returns true when a step was added. The merge path (MergeTask)
/// does the equivalent directly on its `QueryPlan`.
bool recomputeAffectedMaterializedColumns(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context);

/// Recompute the sorting-key expression columns from the post-`SET` values and re-sort the
/// pipeline by the sorting key. Used by the mutation path (e.g. `MATERIALIZE TTL`) after a
/// `TTL ... GROUP BY ... SET` step that rewrites a sort-key column, so the written part is
/// ordered consistently with its primary index. The merge path (MergeTask) does the equivalent
/// directly on its `QueryPlan`. Call only when `groupByTTLAssignsSortKeyColumn` returns true.
/// (Also recomputes affected MATERIALIZED columns first, so calling this makes a separate
/// `recomputeAffectedMaterializedColumns` call redundant.)
void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context,
    const MergeTreeSettings & storage_settings);

}
