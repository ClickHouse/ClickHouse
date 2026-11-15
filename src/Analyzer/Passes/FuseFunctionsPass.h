#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/*
 * This pass replaces several calls of aggregate functions of the same family into one call.
 * Result will be calculated only once because of CSE.
 *
 * Replaces:
 * `sum(x), count(x), avg(x)` with `sumCount(x).1, sumCount(x).2, sumCount(x).1 / toFloat64(sumCount(x).2)`
 * `quantile(0.5)(x), quantile(0.9)(x)` with `quantiles(0.5, 0.9)(x)[1], quantiles(0.5, 0.9)(x)[2]`
 */
class FuseFunctionsPass final : public IQueryTreePass
{
public:
    String getName() override { return "FuseFunctionsPass"; }

    String getDescription() override { return "Replaces several calls of aggregate functions of the same family into one call"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}

