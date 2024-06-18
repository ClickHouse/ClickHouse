#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite _shard_num column into shardNum() function.
  *
  * Example: SELECT _shard_num FROM distributed_table;
  * Result: SELECT shardNum() FROM distributed_table;
  */
class ShardNumColumnToFunctionPass final : public IQueryTreePass
{
public:
    String getName() override { return "ShardNumColumnToFunctionPass"; }

    String getDescription() override { return "Rewrite _shard_num column into shardNum() function"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
