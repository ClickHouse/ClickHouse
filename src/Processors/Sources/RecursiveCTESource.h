#pragma once

#include <Processors/ISource.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

class RecursiveCTEChunkGenerator;

class RecursiveCTESource : public ISource
{
public:
    explicit RecursiveCTESource(Block header, QueryTreeNodePtr recursive_cte_union_node_);

    ~RecursiveCTESource() override;

    String getName() const override { return "RecursiveCTESource"; }

    Chunk generate() override;

private:
    std::unique_ptr<RecursiveCTEChunkGenerator> generator;
};

}
