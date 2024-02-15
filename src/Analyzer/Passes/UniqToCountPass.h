#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Optimize `uniq` and its variants(except uniqUpTo) into `count` over subquery.
 *     Example: 'SELECT uniq(x ...) FROM (SELECT DISTINCT x ...)' to
 *     Result: 'SELECT count() FROM (SELECT DISTINCT x ...)'
 *
 *     Example: 'SELECT uniq(x ...) FROM (SELECT x ... GROUP BY x ...)' to
 *     Result: 'SELECT count() FROM (SELECT x ... GROUP BY x ...)'
 *
 *     Note that we can rewrite all uniq variants except uniqUpTo.
 */
class UniqToCountPass final : public IQueryTreePass
{
public:
    String getName() override { return "UniqToCount"; }

    String getDescription() override
    {
        return "Rewrite uniq and its variants(except uniqUpTo) to count if subquery has distinct or group by clause.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
