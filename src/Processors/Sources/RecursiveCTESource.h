#pragma once

#include <Processors/ISource.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

class RecursiveCTEChunkGenerator;

class RecursiveCTESource final : public ISource
{
public:
    /// `repeated_build_scope_name_` names the pipeline of the recursive member for the deduplication
    /// of the joins it rebuilds, see `QueryExecutionCounters::makeScopeForPipelineBuiltLater`.
    RecursiveCTESource(SharedHeader header, QueryTreeNodePtr recursive_cte_union_node_, String repeated_build_scope_name_);

    ~RecursiveCTESource() override;

    String getName() const override { return "RecursiveCTESource"; }

    Chunk generate() override;

private:
    std::unique_ptr<RecursiveCTEChunkGenerator> generator;
};

}
