#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Push reading of subcolumns into subqueries (and CTEs), so that only the requested
  * subcolumns are read from the tables instead of whole columns.
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
