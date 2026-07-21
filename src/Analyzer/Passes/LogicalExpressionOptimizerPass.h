#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/**
 * This pass tries to do optimizations on logical expression:
 *
 * 1. Replaces chains of equality functions inside an OR with a single IN operator.
 * The replacement is done if:
 *  - one of the operands  of the equality function is a constant
 *  - length of chain is at least 'optimize_min_equality_disjunction_chain_length' long OR the expression has type of LowCardinality
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
 *
 * 2. Removes duplicate OR checks
 * -------------------------------
 * SELECT *
 * FROM table
 * WHERE a = 1 OR b = 'test' OR a = 1;
 *
 * will be transformed into
 *
 * SELECT *
 * FROM TABLE
 * WHERE a = 1 OR b = 'test';
 * -------------------------------
 *
 * 3. Replaces AND chains with a single constant.
 * The replacement is done if:
 *  - one of the operands of the equality function is a constant
 *  - constants are different for same expression
 * -------------------------------
 * SELECT *
 * FROM table
 * WHERE a = 1 AND b = 'test' AND a = 2;
 *
 * will be transformed into
 *
 * SELECT *
 * FROM TABLE
 * WHERE 0;
 * -------------------------------
 *
 * 4. Removes duplicate AND checks
 * -------------------------------
 * SELECT *
 * FROM table
 * WHERE a = 1 AND b = 'test' AND a = 1;
 *
 * will be transformed into
 *
 * SELECT *
 * FROM TABLE
 * WHERE a = 1 AND b = 'test';
 * -------------------------------
 *
 * 5. Replaces chains of inequality functions inside an AND with a single NOT IN operator.
 * The replacement is done if:
 *  - one of the operands of the inequality function is a constant
 *  - length of chain is at least 'optimize_min_inequality_conjunction_chain_length' long OR the expression has type of LowCardinality
 *
 * E.g. (optimize_min_inequality_conjunction_chain_length = 2)
 * -------------------------------
 * SELECT *
 * FROM table
 * WHERE a <> 1 AND a <> 2;
 *
 * will be transformed into
 *
 * SELECT *
 * FROM TABLE
 * WHERE a NOT IN (1, 2);
 * -------------------------------
 *
 * 6. Remove unnecessary IS NULL checks in JOIN ON clause
 *   - equality check with explicit IS NULL check replaced with <=> operator
 * -------------------------------
 * SELECT * FROM t1 JOIN t2 ON a = b OR (a IS NULL AND b IS NULL)
 * SELECT * FROM t1 JOIN t2 ON a <=> b OR (a IS NULL AND b IS NULL)
 *
 * will be transformed into
 *
 * SELECT * FROM t1 JOIN t2 ON a <=> b
 * -------------------------------
 *
 * 7. Remove redundant equality checks on boolean functions.
 *  - these redundant checks cause the primary index to not be used when if the query involves any primary key columns
 * -------------------------------
 * SELECT * FROM t1 WHERE a IN (n) = 1
 * SELECT * FROM t1 WHERE a IN (n) = 0
 *
 * will be transformed into
 *
 * SELECT * FROM t1 WHERE a IN (n)
 * SELECT * FROM t1 WHERE NOT a IN (n)
 * -------------------------------
 *
 * 8. Extract common subexpressions from AND expressions of a single OR expression only in WHERE and ON expressions.
 * If possible, AND and OR expressions will be flattened during performing this.
 * This might break some lazily evaluated expressions, but this optimization can be turned off by optimize_extract_common_expressions = 0.
 * -------------------------------
 * SELECT * FROM t1 WHERE a AND ((b AND c) OR (b AND d) OR (b AND e))
 * SELECT * FROM t1 WHERE a AND ((b AND c) OR ((b AND d) OR (b AND e))) -- needs flattening
 * SELECT * FROM t1 WHERE (a AND b AND c) OR (a AND b AND d)
 * SELECT * FROM t1 WHERE (a AND b) OR (a AND b AND c)
 *
 * will be transformed into
 *
 * SELECT * FROM t1 WHERE a AND b AND (c OR d AND e)
 * SELECT * FROM t1 WHERE a AND b AND (c OR d AND e)
 * SELECT * FROM t1 WHERE a AND b AND (c OR d)
 * SELECT * FROM t1 WHERE a AND b
 * -------------------------------
 *
 * 9. Populate constant comparison in AND chains. Support operators <, <=, >, >=, = and mix of them.
 * -------------------------------
 * SELECT * FROM table WHERE a < b AND b < c AND c < 5;
 *
 * will be transformed into
 *
 * SELECT * FROM table WHERE a < b AND b < c AND c < 5 AND b < 5 AND a < 5;
 * -------------------------------
 */

class LogicalExpressionOptimizerPass final : public IQueryTreePass
{
public:
    String getName() override { return "LogicalExpressionOptimizer"; }

    String getDescription() override
    {
        return "Transforms chains of logical expressions if possible, i.e. "
            "replace chains of equality functions inside an OR with a single IN operator";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
