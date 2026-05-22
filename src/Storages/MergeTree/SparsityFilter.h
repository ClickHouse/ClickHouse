#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <optional>


namespace DB
{

/// Recogniser for `WHERE` predicates that exactly partition a column into its
/// type-default vs non-default subsets. Pruning consumers use this to decide
/// whether the per-column `num_defaults` recorded in `serialization.json` is
/// enough to drop parts / granules without reading data.
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
std::optional<RecognisedSparsityPredicate>
classifySparsityPredicate(const QueryTreeNodePtr & predicate, const QueryTreeNodePtr & table_expression_node);

/// Flatten top-level `AND(...)` into the classifiable conjuncts within. Pruning
/// consumers can drop a part / granule when any one conjunct rules it out. The
/// count-rewrite path does NOT use this: it needs exact row counts and an
/// unclassified sibling conjunct could remove rows that the recognised one keeps.
///
/// `OR` is treated as unclassifiable: safe pruning would require classifying every
/// branch, which we don't attempt.
std::vector<RecognisedSparsityPredicate>
collectSparsityConjuncts(const QueryTreeNodePtr & predicate, const QueryTreeNodePtr & table_expression_node);

}
