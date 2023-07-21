#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replace monotonous functions in ORDER BY.
 *
 * Skip if
 *  1. they participate in GROUP BY expression,
 *  2. they have more than one arguments
 *  3. they are aggregate functions
 *  4. order by expression matches the sorting key to allow execute reading in order of key
 *
 * Example: SELECT number FROM numbers(3) ORDER BY toFloat32(number);
 * Result: SELECT number FROM numbers(3) ORDER BY number;
 */
class MonotonousFunctionsInOrderByPass final : public IQueryTreePass
{
public:
    String getName() override { return "MonotonousFunctionsInOrderBy"; }

    String getDescription() override
    {
        return "Replace monotonous functions in ORDER BY.";
    }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}
