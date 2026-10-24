#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Push reading of subcolumns into subqueries (including CTEs and `UNION ALL` subqueries),
  * so that only the requested subcolumns are read from the tables instead of whole columns.
  * For a `UNION ALL` subquery the subcolumn is added to every branch at the same position;
  * the DISTINCT, INTERSECT and EXCEPT modes deduplicate or match rows over all projection
  * columns, so such subqueries are not rewritten.
  *
  * A subcolumn of a column exported by a subquery is resolved into the `getSubcolumn`
  * function over the whole column:
  *     SELECT data.a FROM (SELECT * FROM test)
  * is resolved into
  *     SELECT getSubcolumn(data, 'a') FROM (SELECT data FROM test)
  * and the whole column `data` is read from the table.
  *
  * This pass adds the subcolumn to the subquery projection and replaces the `getSubcolumn`
  * function with a reference to it:
  *     SELECT `data.a` FROM (SELECT `data.a` FROM test)
  * If the whole column is not used anywhere else, it is removed from the subquery projection
  * by the subsequent RemoveUnusedProjectionColumnsPass, and only the subcolumn is read.
  */
class PushSubcolumnsIntoSubqueriesPass final : public IQueryTreePass
{
public:
    String getName() override { return "PushSubcolumnsIntoSubqueries"; }

    String getDescription() override
    {
        return "Replace reading of subcolumns of columns exported by subqueries with reading of the subcolumns inside the subqueries.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
