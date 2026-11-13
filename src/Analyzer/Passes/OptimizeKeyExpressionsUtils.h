#pragma once

#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

#include <cstdint>

namespace DB
{

class FunctionNode;
class QueryNode;

/** Which `GROUP BY` keys the other expressions can be calculated from.
  *
  * Some passes replace an expression over a function with an expression over the arguments of the function, for example
  * `toYear(d) = 2024` with a condition on `d`. If `toYear(d)` is a key, `d` does not exist after the aggregation. So an
  * expression over such a key can appear after the aggregation only when these passes are done.
  */
enum class UsableGroupByKeys : uint8_t
{
    /// All keys, for all expressions.
    ALL,
    /// Before these passes: the keys that they do not take apart.
    BEFORE_REPLACEMENTS,
    /// After these passes: all keys. Only for the expressions that have such functions, the others are done before.
    AFTER_REPLACEMENTS,
};

/// Returns true if a pass can replace an expression over `node` with an expression over the arguments of `node`, or if `node` can be
/// replaced differently in an expression and in `GROUP BY`.
bool isReplaceableWithArguments(const QueryTreeNodePtr & node);

/// Returns true if `node` or any part of it is replaceable, or can be the result of a replacement.
bool hasReplaceableWithArguments(const QueryTreeNodePtr & node);

/** Shared helpers for cleaning up the key list of `GROUP BY` and `LIMIT BY`. Both group rows by
  * the distinct combinations of their key values. A key is dropped or rewritten only when this
  * grouping stays the same and the old value can still be computed from the keys that remain, so
  * the result (including the columns that `GROUP BY` outputs) does not change.
  */

/// Returns true if `node` is an ordinary deterministic function computed only from `keys` (every
/// column it reads is one of the keys). Such a value is the same for all rows in a group, so this
/// key can be dropped without changing the grouping. The values of `aggregation_keys` are not
/// computed from the columns they read.
bool isExpressionFunctionOfKeys(
    const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & keys, const QueryTreeNodes & aggregation_keys = {});

/// Returns true if `GROUP BY` keys of `query_node` that are functions of other keys can be removed.
bool canRemoveGroupByKeysThatAreFunctionsOfOtherKeys(const QueryNode & query_node);

/// Removes, in place, every key that is a function of the other keys.
void removeKeysThatAreFunctionsOfOtherKeys(
    QueryTreeNodes & keys,
    const QueryTreeNodes & aggregation_keys = {},
    UsableGroupByKeys usable_keys = UsableGroupByKeys::ALL,
    const QueryTreeNodePtrWithHashIgnoreAliasesSet & keys_to_keep = {});

/// Returns a new key list with injective functions replaced by their non-constant arguments. An
/// injective function gives the same grouping as its arguments, so the result does not change. When
/// `allow_suspicious_types` is false, a function is left as is if unwrapping it would turn a
/// `Dynamic` or `Variant` argument into a key. A node found in `keys_to_keep` is kept as it is and
/// is not unwrapped further: `LIMIT BY` runs after aggregation, so it must not be rewritten past a
/// `GROUP BY` key into columns that no longer exist there.
QueryTreeNodes unwrapInjectiveFunctionsInKeys(
    const QueryTreeNodes & keys, bool allow_suspicious_types, const QueryTreeNodePtrWithHashIgnoreAliasesSet * keys_to_keep = nullptr);

/// Returns true if `function` is an aggregate function that returns the value of its argument if this is the same for all rows.
bool isAggregateFunctionOfEqualValues(const FunctionNode & function);

/// The `GROUP BY` keys of `query_node`, of all grouping sets.
QueryTreeNodes getGroupByKeys(const QueryNode & query_node);

/// Returns true if `node`, in an expression computed after `GROUP BY`, is a key, read from the aggregation result.
/// Under `group_by_use_nulls` the analyzer replaces a key there with a copy converted to `Nullable`, not equal to the key.
bool isGroupByKeyAfterAggregation(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys);

/// Returns true if `query_node` can have several rows with the same values of `GROUP BY` keys. Subtotals are not considered.
bool canHaveSeveralRowsPerGroup(const QueryNode & query_node);

/// Returns true if `query_node`, which is a `SELECT DISTINCT`, can return equal rows.
bool canHaveEqualRowsAfterDistinct(const QueryNode & query_node);

/// Equal expressions can have different names in the planner, for example because it adds a cast to the name of a constant that is folded
/// from an expression: `intDiv(x, 86400)` and `intDiv(x, 24 * 3600)`. Returns true if the names of equal expressions are the same.
bool areConstantsWrittenAlike(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs);

/// The same, but constants can differ in the cast.
bool areConstantsEqual(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs);

/// Returns true if `node` has a function, other than `arrayJoin`, that can return different values for the same arguments.
bool hasNonDeterministicFunction(const QueryTreeNodePtr & node);

/// Returns true if equal expressions have the same value. Their constants can differ in the cast, but not if an expression has an `arrayJoin`
/// or a non-deterministic function, because these are calculated separately for each name.
bool haveSameValue(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs);

/// Returns true if `node` can be calculated after the aggregation from the keys as they are written.
bool canBeCalculatedFromKeys(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys);

/// Returns true if equal `GROUP BY` keys of `query_node` can have different values, because their names are different, and some of them
/// have default values in subtotals.
bool canEqualGroupByKeysDiffer(const QueryNode & query_node);

/// The arguments of the functions `grouping` of `query_node`, which must be `GROUP BY` keys.
QueryTreeNodePtrWithHashIgnoreAliasesSet getGroupingFunctionArguments(const QueryNode & query_node);

}
