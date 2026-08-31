#pragma once

#include <memory>
#include <mutex>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

struct IndexDescription;
struct KeyDescription;
struct AlternativeKeyExpression;
using AlternativeKeyExpressionPtr = std::shared_ptr<const AlternativeKeyExpression>;

class MergeTreeIndexMinMax;

/** Compute the alternative form of the index key: the names the index expressions get after the
  * query's rewrite passes, applied with the settings of the given query context, together with the
  * rewritten expressions themselves.
  *
  * The index expressions are analyzed when the index description is built, so their column names do
  * not reflect the rewrites that are applied to the query (e.g. `multiIf` with a single condition is
  * rewritten to `if` by `optimize_multiif_to_if`, map element access to a subcolumn read). Index
  * analysis matches filter expressions against index expressions (and their subexpressions) by name,
  * so without the alternative form a rewritten filter expression does not match the index expression,
  * and the index is not used (issue #103128).
  *
  * Both the query analyzer (query tree passes) and the legacy analyzer (`TreeRewriter` AST
  * optimizations) are reproduced, depending on `enable_analyzer` in the context.
  *
  * Returns nullptr when not applicable: the index is on plain columns, no name differs after the
  * rewrites, or the index expression cannot be analyzed (best effort). Otherwise the result's
  * `column_names` is parallel to `index.column_names` and `expression` computes the same index
  * columns in the rewritten form.
  */
AlternativeKeyExpressionPtr getAlternativeIndexExpression(const IndexDescription & index, const ContextPtr & context);

/** The same for a primary or partition key: index analysis matches the filter expression against the
  * key expressions by name as well, so a key on expressions (e.g. `ORDER BY multiIf(v > 0, v, NULL)`
  * or the same as `PARTITION BY`) loses pruning when the query's rewrite passes rename the filter
  * side of the comparison. Returns nullptr when not applicable, see above.
  */
AlternativeKeyExpressionPtr getAlternativeKeyExpression(const KeyDescription & key, const ContextPtr & context);

/** Computes the alternative form of a key at most once, for the query plan's condition factories,
  * which create a `KeyCondition` many times for the same query (once per part, for constant folding).
  */
class LazyAlternativeKeyExpression
{
public:
    AlternativeKeyExpressionPtr get(const KeyDescription & key, const ContextPtr & context) const;

private:
    mutable std::once_flag initialized;
    mutable AlternativeKeyExpressionPtr alternative_key;
};

/** The single entry point for creating a skip index condition that is aware of the query's rewrites:
  * a `minmax` index on expressions is matched not only by the original names of its expressions, but
  * also by their alternative (rewritten) names, see `getAlternativeIndexExpression`. For every other
  * index it is the plain `IMergeTreeIndex::createIndexCondition`.
  *
  * The alternative form of the key is computed lazily, once per factory, because a condition can be
  * created many times for the same query (once per part, for constant folding), and not at all when
  * the query has no filter.
  */
class RewriteAwareIndexConditionFactory
{
public:
    explicit RewriteAwareIndexConditionFactory(MergeTreeIndexPtr index_helper_);

    MergeTreeIndexConditionPtr create(const ActionsDAG::Node * predicate, const ContextPtr & context) const;

private:
    MergeTreeIndexPtr index_helper;
    const MergeTreeIndexMinMax * minmax_index = nullptr;

    mutable std::once_flag alternative_key_initialized;
    mutable AlternativeKeyExpressionPtr alternative_key;
};

}
