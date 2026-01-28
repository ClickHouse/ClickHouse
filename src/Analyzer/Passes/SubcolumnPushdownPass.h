#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Push subcolumn access into subqueries.
  *
  * When a query accesses a subcolumn (e.g., event.class_name) from a subquery
  * that projects the full column (event), this pass:
  * 1. Modifies the subquery to project the subcolumn directly
  * 2. Replaces getSubcolumn(column, 'subcolumn') with a direct column reference
  *
  * This enables subcolumn pruning - only the needed subcolumns are read.
  *
  * Example:
  * WITH foo AS (SELECT * FROM table)  -- projects 'event' Tuple
  * SELECT event.class_name FROM foo   -- needs only event.class_name
  *
  * Before: getSubcolumn(event, 'class_name'), reads entire 'event' column
  * After: direct event.class_name access, reads only that subcolumn
  */
class SubcolumnPushdownPass final : public IQueryTreePass
{
public:
    String getName() override { return "SubcolumnPushdown"; }

    String getDescription() override
    {
        return "Push subcolumn access into subqueries to enable subcolumn pruning";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
