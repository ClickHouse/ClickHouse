#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{


/** Replace CROSS JOIN with INNER JOIN.
  * Example:
  *   SELECT * FROM t1 CROSS JOIN t2 WHERE t1.a = t2.a AND t1.b > 10 AND t2.b = t2.c
  * We can move equality condition to ON section of INNER JOIN:
  *   SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.a WHERE t1.b > 10 AND t2.b = t2.c
  */
class CrossToInnerJoinPass final : public IQueryTreePass
{
public:
    String getName() override { return "CrossToInnerJoin"; }

    String getDescription() override
    {
        return "Replace CROSS JOIN with INNER JOIN";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
