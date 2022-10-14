#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/*
 * Try to fuse sum/avg/count with identical arguments to one sumCount call,
 * if we have at least two different functions.
 * E.g. we will replace sum(x) and count(x) with sumCount(x).1 and sumCount(x).2,
 * Result of sumCount() will be calculated only once because of CSE.
 */
class FuseFunctionsPass final : public IQueryTreePass
{
public:
    String getName() override { return "FuseFunctionsPass"; }

    String getDescription() override { return "Replaces sum/avg/count with identical arguments to one sumCount call"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}

