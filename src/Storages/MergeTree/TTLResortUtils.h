#pragma once

#include <optional>

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class QueryPipelineBuilder;
class ActionsDAG;
class Block;

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

/// Build an `ActionsDAG` over `header` that refreshes the derived `group_by_keys` columns whose
/// in-stream value went stale after an earlier `GROUP BY` TTL `SET`: computed/subcolumn keys are
/// recomputed from the primary-key expression, and a MATERIALIZED column used as a key is recomputed
/// from its default expression (together with its transitive affected MATERIALIZED sources). Returns
/// nullopt when no key needs refreshing (all keys are plain physical columns). Applied before the
/// later `TTLAggregationAlgorithm` consumes the block, so it groups by the post-`SET` key values.
std::optional<ActionsDAG> buildRefreshGroupByKeysDAG(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & group_by_keys,
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

/// The MATERIALIZED sort-key storage columns whose source columns are rewritten by a
/// `TTL ... GROUP BY ... SET` (e.g. `d` for `d MATERIALIZED toDate(ts)` when `ts` is SET). These
/// hold stale values in the post-TTL stream and must be recomputed from their default expression
/// before the sorting key is recomputed and the stream re-sorted. Empty when the `SET` only
/// rewrites sort-key columns directly.
NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

/// EVERY MATERIALIZED storage column whose default expression (transitively) reads a column
/// rewritten by a `TTL ... GROUP BY ... SET` -- not only the sort-key subset. `TTLAggregationAlgorithm`
/// keeps such a column as `any(col)` from the pre-`SET` rows, so its stored value, and any rebuilt
/// skip index / projection that reads it, would be written stale (wrong data, not just a missed
/// optimization). Both the merge and mutation paths recompute these from their default expression
/// before the part is written. Empty when no MATERIALIZED column depends on a `SET` target.
NamesAndTypesList getGroupByTTLSetAffectedMaterializedColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);

/// The MATERIALIZED columns whose default expression reads BOTH an EPHEMERAL column and a column
/// rewritten by a `TTL ... GROUP BY ... SET`. Such a column cannot be recomputed during merge/mutation
/// (ephemeral columns are only available at INSERT, never read from disk), so its stored value goes
/// stale with no way to refresh it. Callers warn about each (mirroring `MutationsInterpreter::prepare`
/// for `UPDATE`) rather than silently writing a stale value. Empty when no such column exists.
Names getStaleEphemeralMaterializedColumnsAffectedBySet(
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
    const ContextPtr & context);

}
