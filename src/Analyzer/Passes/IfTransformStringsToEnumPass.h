#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/**
 * This pass replaces string-type arguments in If and Transform to enum.
 *
 * E.g.
 * -------------------------------
 * SELECT if(number > 5, 'a', 'b')
 * FROM system.numbers;
 *
 * will be transformed into
 *
 * SELECT if(number > 5, _CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'), _CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)'))
 * FROM system.numbers;
 * -------------------------------
 * SELECT transform(number, [2, 4], ['a', 'b'], 'c') FROM system.numbers;
 *
 * will be transformed into
 *
 * SELECT transform(number, [2, 4], _CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'), _CAST('c', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'))
 * FROM system.numbers;
 * -------------------------------
 */
class IfTransformStringsToEnumPass final : public IQueryTreePass
{
public:
    String getName() override { return "IfTransformStringsToEnumPass"; }

    String getDescription() override { return "Replaces string-type arguments in If and Transform to enum"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
