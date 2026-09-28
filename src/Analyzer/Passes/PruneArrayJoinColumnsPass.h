#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Prune unused ARRAY JOIN expressions and unused subcolumns from `nested()` functions.
  *
  * 1. If ARRAY JOIN has multiple expressions and some are not referenced
  *    anywhere in the query, remove the unused expressions.
  *
  * 2. When a Nested column is used in ARRAY JOIN, the analyzer creates a `nested()`
  *    function with ALL subcolumns as arguments. This pass removes arguments that
  *    are not referenced, so that only the needed subcolumns are read from storage.
  *
  * Example 1: SELECT b FROM t ARRAY JOIN a, b  =>  ARRAY JOIN b
  *
  * Example 2: Table has n.a, n.b, n.c.
  *   SELECT n.a FROM t ARRAY JOIN n
  *   Before: ARRAY JOIN nested(['a','b','c'], n.a, n.b, n.c) AS n
  *   After:  ARRAY JOIN nested(['a'], n.a) AS n
  */
class PruneArrayJoinColumnsPass final : public IQueryTreePass
{
public:
    String getName() override { return "PruneArrayJoinColumns"; }

    String getDescription() override { return "Prune unused ARRAY JOIN expressions and nested() subcolumns"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
