#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Prune unused subcolumns from `nested()` functions in ARRAY JOIN.
  *
  * When a Nested column is used in ARRAY JOIN, the analyzer creates a `nested()`
  * function with ALL subcolumns as arguments. This pass removes arguments that
  * are not referenced, so that only the needed subcolumns are read from storage.
  *
  * Whole ARRAY JOIN expressions are NOT removed even when unused: in a multi-expression
  * aligned ARRAY JOIN the joined arrays must have equal per-row sizes, and that is validated
  * only at execution, so every expression must reach ArrayJoinAction (see issue #111747).
  *
  * Example: Table has n.a, n.b, n.c.
  *   SELECT n.a FROM t ARRAY JOIN n
  *   Before: ARRAY JOIN nested(['a','b','c'], n.a, n.b, n.c) AS n
  *   After:  ARRAY JOIN nested(['a'], n.a) AS n
  */
class PruneArrayJoinColumnsPass final : public IQueryTreePass
{
public:
    String getName() override { return "PruneArrayJoinColumns"; }

    String getDescription() override { return "Prune unused nested() subcolumns in ARRAY JOIN"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
