#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <optional>


namespace DB
{

/// Recogniser for `WHERE` predicates that exactly partition a column into its
/// type-default vs non-default subsets. The trivial-count-with-sparsity-filter
/// rewrite uses this to decide whether it can serve `SELECT count()` from the
/// per-column `num_defaults` recorded in `serialization.json` without a scan.
///
/// A wrong `num_defaults` would produce wrong results, so trust the recorded
/// value only when the producer marked it with `exact_num_defaults: true`.
/// Storage-wide preconditions (no active transaction, no patch parts, ...)
/// are checked separately by `MergeTreeData::getColumnDefaultnessStats`.

enum class SparsityPredicateClass : uint8_t
{
    /// Predicate matches only default rows, e.g. `col = 0`, `isNull(col)`, `empty(col)`.
    MatchesDefault,
    /// Predicate matches only non-default rows, e.g. `col != 0`, `isNotNull(col)`.
    MatchesNonDefault,
};

struct RecognisedSparsityPredicate
{
    String column_name;
    SparsityPredicateClass predicate_class;
};

/// Recognise a predicate that partitions one column of `table_expression_node` into
/// default vs non-default rows. Returns nullopt for any other shape. Recognised forms:
///   any T:           col = default(T)               -> MatchesDefault
///                    col != default(T)              -> MatchesNonDefault
///   Nullable(T):     isNull(col)                    -> MatchesDefault
///                    isNotNull(col)                 -> MatchesNonDefault
///   String:          empty(col) / notEmpty(col)
///   Bool:            col = true / col != true       (value space is {false, true})
///   UInt*:           col > 0  / col >= 1            -> MatchesNonDefault
///                    col < 1  / col <= 0            -> MatchesDefault
///   Bool / UInt8:    col                            -> MatchesNonDefault (bare truthy test)
///                    not(col)                       -> MatchesDefault
///                    (wider Int/UInt are rejected as filter columns before reaching here)
std::optional<RecognisedSparsityPredicate>
classifySparsityPredicate(const QueryTreeNodePtr & predicate, const QueryTreeNodePtr & table_expression_node);

}
