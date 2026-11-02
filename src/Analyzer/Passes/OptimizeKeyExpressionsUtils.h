#pragma once

#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Shared helpers for cleaning up the key list of `GROUP BY` and `LIMIT BY`. Both group rows by
  * the distinct combinations of their key values. A key is dropped or rewritten only when this
  * grouping stays the same and the old value can still be computed from the keys that remain, so
  * the result (including the columns that `GROUP BY` outputs) does not change.
  */

/// Returns true if `node` is an ordinary deterministic function computed only from `keys` (every
/// column it reads is one of the keys). Such a value is the same for all rows in a group, so this
/// key can be dropped without changing the grouping.
bool isExpressionFunctionOfKeys(const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & keys);

/// Removes, in place, every key that is a function of the other keys.
void removeKeysThatAreFunctionsOfOtherKeys(QueryTreeNodes & keys);

/// Returns a new key list with injective functions replaced by their non-constant arguments. An
/// injective function gives the same grouping as its arguments, so the result does not change. When
/// `allow_suspicious_types` is false, a function is left as is if unwrapping it would turn a
/// `Dynamic` or `Variant` argument into a key.
QueryTreeNodes unwrapInjectiveFunctionsInKeys(const QueryTreeNodes & keys, bool allow_suspicious_types);

}
