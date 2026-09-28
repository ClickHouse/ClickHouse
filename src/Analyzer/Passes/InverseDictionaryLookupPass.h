#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Optimize `dictGetFamily(..., ATTR_COL, COL) = CONSTEXPR` into `COL IN (SELECT ... FROM dictionary WHERE ATTR_COL = CONSTEXPR)`
  *
  * Example: SELECT col FROM tab WHERE dictGet(DICT_NAME, DICT_ATTRIBUTE_COL, col) = CONSTEXPR;
  * Result: SELECT col FROM t WHERE col IN (SELECT DICT_KEY_COL FROM dictionary(DICT_NAME) WHERE DICT_ATTRIBUTE_COL = CONSTEXPR);
  *
  * Example: SELECT (col1, col2) FROM tab WHERE dictGet(DICT_NAME, DICT_ATTRIBUTE_COL, (col1, col2)) = CONSTEXPR;
  * Result: SELECT (col1, col2) FROM t WHERE (col1, col2) IN (SELECT (DICT_KEY_COL1, DICT_KEY_COL2) FROM dictionary(DICT_NAME) WHERE DICT_ATTRIBUTE_COL = CONSTEXPR);
  *
  * Also optimizes `dictGetFamily(..., ATTR_COL, COL) [NOT] IN (CONST_EXPRS)`. The attribute `DEFAULT` returned for keys missing
  * from the dictionary decides the form of the rewrite:
  *
  * Example: SELECT col FROM tab WHERE dictGet(DICT_NAME, DICT_ATTRIBUTE_COL, col) IN (CONST1, CONST2);
  * Result (DEFAULT is not in the list): SELECT col FROM tab WHERE col IN (SELECT DICT_KEY_COL FROM dictionary(DICT_NAME) WHERE DICT_ATTRIBUTE_COL IN (CONST1, CONST2));
  * Result (DEFAULT is in the list): SELECT col FROM tab WHERE col NOT IN (SELECT DICT_KEY_COL FROM dictionary(DICT_NAME) WHERE DICT_ATTRIBUTE_COL NOT IN (CONST1, CONST2));
  *
  * Supported comparison operators: =, !=, <, <=, >, >=, LIKE, ILIKE and their negations.
  * Supported IN operators: IN, NOT IN.
  * NOTE: Does not support `dictGet*OrDefault` functions. Supports all other `dictGet*` functions that have 3 arguments.
  */
class InverseDictionaryLookupPass final : public IQueryTreePass
{
public:
    String getName() override { return "InverseDictionaryLookupPass"; }

    String getDescription() override
    {
        return "Optimize `WHERE dictGetFamily(..., ATTR_COL, COL) = CONSTEXPR` into `COL IN (SELECT ... FROM dictionary WHERE ATTR_COL = "
               "CONSTEXPR)`";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

private:
    void rewriteDictGetEqual(QueryTreeNodePtr & query_tree_node) const;
};

}
