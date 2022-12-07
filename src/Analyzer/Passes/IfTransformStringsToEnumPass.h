#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/**
 * This pass replaces string-type arguments in If and Transform to enum.
 */
class IfTransformStringsToEnumPass final : public IQueryTreePass
{
public:
    String getName() override { return "IfTransformStringsToEnumPass"; }

    String getDescription() override { return "Replaces string-type arguments in If and Transform to enum"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
