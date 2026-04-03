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
 * 3. Replaces chains of inequality functions inside an AND with a single NOT IN operator.
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
 * 4. Remove unnecessary IS NULL checks in JOIN ON clause
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
 * 5. Remove redundant equality checks on boolean functions.
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
 * 6. Extract common subexpressions from AND expressions of a single OR expression only in WHERE and ON expressions.
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
 * 7. Populate constant comparison in AND chains. Support operators <, <=, >, >=, = and mix of them.
 * -------------------------------
 * SELECT * FROM table WHERE a < b AND b < c AND c < 5;
 *
 * will be transformed into
 *
 * SELECT * FROM table WHERE a < b AND b < c AND c < 5 AND b < 5 AND a < 5;
 * -------------------------------
 *
 * 8. Prune redundant and detect conflicting comparison conditions on the same expression
 *     within AND chains.  Controlled by setting `optimize_and_compare_chain_pruning`.
 *     Handles all six comparison operators (=, !=, <, <=, >, >=) and their combinations:
 *     duplicate removal, contradiction detection, and range tightening.
 * -------------------------------
 * SELECT * FROM table WHERE a = 1 AND a = 1;
 * -- duplicate: same condition twice
 * will be transformed into
 * SELECT * FROM table WHERE a = 1;
 *
 * SELECT * FROM table WHERE a = 1 AND a = 2;
 * -- conflict: equals with different constants
 * will be transformed into
 * SELECT * FROM table WHERE false;
 *
 * SELECT * FROM table WHERE a = 3 AND a < 5;
 * -- redundant: a < 5 is implied by a = 3
 * will be transformed into
 * SELECT * FROM table WHERE a = 3;
 *
 * SELECT * FROM table WHERE a = 3 AND a > 5;
 * -- conflict: no value satisfies both
 * will be transformed into
 * SELECT * FROM table WHERE false;
 *
 * SELECT * FROM table WHERE a < 5 AND a < 3;
 * -- tighter bound wins
 * will be transformed into
 * SELECT * FROM table WHERE a < 3;
 *
 * SELECT * FROM table WHERE u > 255 AND i > 0;  -- u is UInt8
 * -- out of range: UInt8 cannot exceed 255
 * will be transformed into
 * SELECT * FROM table WHERE false;
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
