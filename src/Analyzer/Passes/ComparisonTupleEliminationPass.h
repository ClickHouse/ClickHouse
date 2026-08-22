#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite tuples comparison into equivalent comparison of tuples arguments.
  *
  * Example: SELECT id FROM test_table WHERE (id, value) = (1, 'Value');
  * Result: SELECT id FROM test_table WHERE id = 1 AND value = 'Value';
  */
class ComparisonTupleEliminationPass final : public IQueryTreePass
{
public:
    String getName() override { return "ComparisonTupleEliminationPass"; }

    String getDescription() override { return "Rewrite tuples comparison into equivalent comparison of tuples arguments"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
