#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class MapElementConstKeyToShardPass final : public IQueryTreePass
{
public:
    String getName() override { return "MapElementConstKeyToShard"; }
    String getDescription() override { return "TODO"; }
    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
