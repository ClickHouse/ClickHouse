#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Optimize single `dictGet = LITERAL` into `IN [array keys where value = LITERAL]` subquery
  *
  * Example: SELECT col FROM tab WHERE dictGet(DICT_NAME, DICT_VAL_COL, col) = LITERAL;
  * Result: SELECT col FROM t WHERE col IN (SELECT DICT_KEY_COL FROM dictionary(DICT_NAME) WHERE DICT_VAL_COL = LITERAL);
  */

class InverseDictionaryLookupPass final : public IQueryTreePass
{
public:
    String getName() override { return "CountDistinct"; }

    String getDescription() override { return "Optimize single dictGet to IN subquery"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

private:
    void rewriteDictGetEqual(QueryTreeNodePtr & query_tree_node);
};

}
