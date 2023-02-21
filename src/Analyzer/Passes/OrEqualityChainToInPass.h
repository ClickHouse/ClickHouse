#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/**
 * This pass replaces chains of equality functions inside an OR with a single IN operator.
 * The replacement is done if:
 *  - rhs of the equality function is a literal
 *  - length of chain is at least 'optimize_min_equality_disjunction_chain_length' long OR lhs is LowCardinality
 *
 * E.g. (optimize_min_equality_disjunction_chain_length = 2)
 * -------------------------------
 * SELECT *
 * FROM table
 * WHERE a = 1 OR b = 'test' OR a = 2;
 *
 * will be transformed into
 *
 * SELECT *
 * FROM TABLE
 * WHERE b = 'test' OR a IN (1, 2);
 * -------------------------------
 */

class OrEqualityChainToInPass final : public IQueryTreePass
{
public:
    String getName() override { return "OrEqualityChainToIn"; }

    String getDescription() override { return "Transform all the 'or's with equality check to a single IN function"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
