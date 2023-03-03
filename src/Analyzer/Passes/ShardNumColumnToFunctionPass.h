#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite _shard_num column into shardNum() function.
  */
class ShardNumColumnToFunctionPass final : public IQueryTreePass
{
public:
    String getName() override { return "ShardNumColumnToFunctionPass"; }

    String getDescription() override { return "Rewrite _shard_num column into shardNum() function"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}
