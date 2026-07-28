#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Prune unused values from ARRAY JOIN: `nested()` subcolumns, and whole unused operands.
  *
  * When a Nested column is used in ARRAY JOIN, the analyzer creates a `nested()`
  * function with ALL subcolumns as arguments. This pass removes arguments that
  * are not referenced, so that only the needed subcolumns are read from storage.
  *
  * Example: Table has n.a, n.b, n.c.
  *   SELECT n.a FROM t ARRAY JOIN n
  *   Before: ARRAY JOIN nested(['a','b','c'], n.a, n.b, n.c) AS n
  *   After:  ARRAY JOIN nested(['a'], n.a) AS n
  *
  * Also replace an unused operand of a multi-operand ARRAY JOIN by an offsets-only carrier.
  *
  * Such an operand is neither removed nor kept whole. In a multi-operand aligned ARRAY JOIN the
  * joined arrays must have equal per-row sizes, and that is validated only at execution, so an
  * operand that is removed is never validated (issue #111747), while an operand that is kept whole
  * is read from storage in full just to be validated. An expression built from its lengths keeps
  * the validation and drops the read.
  *
  * Example: Table has a, b.
  *   SELECT b FROM t ARRAY JOIN a, b
  *   Before: ARRAY JOIN a, b
  *   After:  ARRAY JOIN arrayResize(emptyArrayUInt8(), length(a)), b
  *
  * FunctionToSubcolumnsPass runs after this pass and may then fold `length(a)` to `a.size0`.
  */
class PruneArrayJoinColumnsPass final : public IQueryTreePass
{
public:
    String getName() override { return "PruneArrayJoinColumns"; }

    String getDescription() override { return "Prune unused nested() subcolumns and unused operand values in ARRAY JOIN"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
