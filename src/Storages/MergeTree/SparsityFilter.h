#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <optional>


namespace DB
{

/// Predicate classifier for the `optimize_trivial_count_with_sparsity_filter`
/// rewrite. If a `count()` query has a single WHERE predicate that exactly
/// partitions rows of one column into "default" and "non-default" subsets,
/// the row count can be served from
/// `SerializationInfo::Data::num_rows / num_defaults` (recorded per part in
/// `serialization.json`) without scanning data.
///
/// When can the serialization info be trusted?
/// -------------------------------------------
///
/// The rewrite needs an exact `num_defaults` -- a wrong number would produce
/// the wrong query result. Two independent reliability conditions are checked:
///
///   1. **Per-part flag.** `SerializationInfo::Data::add(const IColumn &)`
///      computes `num_defaults` exactly via `IColumn::getNumberOfDefaultRows`,
///      and the writer marks the entry with `exact_num_defaults: true` in
///      `serialization.json`. The flag is set for every sparse-eligible
///      column regardless of which serialization kind was ultimately chosen
///      (Default or Sparse). `MergeTreeData::getColumnDefaultnessStats`
///      requires the flag on every visible part; parts written by older
///      servers without it cause the rewrite to skip storage-wide.
///
///   2. **Storage-level invariants** (also checked by
///      `MergeTreeData::getColumnDefaultnessStats`):
///        a. No active transaction (`query_context->getCurrentTransaction() == nullptr`).
///           Same restriction as `optimize_trivial_count_query`: the
///           snapshot of visible parts must be stable.
///        b. No lightweight patch parts (`getPatchPartsVectorForInternalUsage().empty()`).
///           Patches apply updates/deletes at read time and are not folded
///           into the base part's `serialization.json`, so `num_defaults`
///           would be stale.
///
/// Pending classic mutations are not a problem: they replace whole parts,
/// and the new part is written with its own `serialization.json`, so the
/// version we read is self-consistent with the parts we sum over.
///
/// The optimisation is best-effort: it never produces wrong answers; it just
/// stops firing when any of these checks fails.

/// How a recognised predicate maps to the column's default partition.
enum class SparsityPredicateClass : uint8_t
{
    /// Predicate is true exactly for rows where the column equals its (data type) default.
    /// e.g. `col = 0` for UInt32, `col IS NULL` for Nullable(T), `empty(col)` for String.
    MatchesDefault,
    /// Predicate is true exactly for rows where the column does not equal its default.
    MatchesNonDefault,
};

struct RecognisedSparsityPredicate
{
    /// Column name as it appears in the storage's columns description.
    String column_name;
    SparsityPredicateClass predicate_class;
};

/// Try to recognise `predicate` as one of the supported "default vs non-default"
/// shapes against a column produced by `table_expression_node`.
///
/// Returns std::nullopt for any other shape.
///
/// Recognised shapes (operands may be in either order for symmetric operators):
///   For any T:
///     - col = literal_default(T)        -> MatchesDefault
///     - col != literal_default(T)       -> MatchesNonDefault
///   For Nullable(T):
///     - col IS NULL / isNull(col)       -> MatchesDefault
///     - col IS NOT NULL / isNotNull(col)-> MatchesNonDefault
///   For String:
///     - empty(col)                      -> MatchesDefault
///     - notEmpty(col)                   -> MatchesNonDefault
///   For Bool (value space is just {false, true}):
///     - col = true                      -> MatchesNonDefault
///     - col != true                     -> MatchesDefault
///   For unsigned integer types (default = 0):
///     - col > 0 / col >= 1              -> MatchesNonDefault
///     - col < 1 / col <= 0              -> MatchesDefault
///
/// This function inspects only the local shape of the predicate and the
/// column's data type; it does not consult any storage. The caller decides
/// whether the column's default count is reliable for its purpose (see the
/// "When can the serialization info be trusted?" section above).
std::optional<RecognisedSparsityPredicate>
classifySparsityPredicate(const QueryTreeNodePtr & predicate, const QueryTreeNodePtr & table_expression_node);

/// Walk a top-level `AND(...)` tree and return every classifiable conjunct. For a
/// non-`and` predicate this returns a single-element vector iff that predicate itself
/// classifies; otherwise empty.
///
/// Use this for pruning consumers (Phase A part-level, Phase B granule-level): for
/// `WHERE a AND b AND ...`, a part / granule can be dropped if ANY classifiable
/// conjunct proves it can't match. The trivial-count rewrite (Layer C-1) intentionally
/// does NOT use this -- count rewrites need exact row counts and other conjuncts may
/// remove rows that the sparsity predicate alone says match.
///
/// `OR` is not yet supported (treated as unclassifiable) -- safe pruning under `OR`
/// requires all branches to be classifiable, which we punt on for now.
std::vector<RecognisedSparsityPredicate>
collectSparsityConjuncts(const QueryTreeNodePtr & predicate, const QueryTreeNodePtr & table_expression_node);

}
