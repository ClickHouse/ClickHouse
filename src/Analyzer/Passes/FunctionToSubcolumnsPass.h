#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Transform functions to subcolumns. Enabled using setting optimize_functions_to_subcolumns.
  * It can help to reduce amount of read data.
  *
  * Example: SELECT tupleElement(column, subcolumn) FROM test_table;
  * Result: SELECT column.subcolumn FROM test_table;
  *
  * Example: SELECT length(array_column) FROM test_table;
  * Result: SELECT array_column.size0 FROM test_table;
  *
  * Example: SELECT nullable_column IS NULL FROM test_table;
  * Result: SELECT nullable_column.null FROM test_table;
  */
class FunctionToSubcolumnsPass final : public IQueryTreePass
{
public:
    String getName() override { return "FunctionToSubcolumns"; }

    String getDescription() override { return "Rewrite function to subcolumns, for example tupleElement(column, subcolumn) into column.subcolumn"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
