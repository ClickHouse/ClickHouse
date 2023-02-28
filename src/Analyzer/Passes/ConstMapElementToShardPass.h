#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class ConstMapElementToShardPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConstMapElementToShard"; }
    String getDescription() override { return "TODO"; }
    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
