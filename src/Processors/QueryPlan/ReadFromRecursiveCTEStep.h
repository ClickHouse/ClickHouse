#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

class ReadFromRecursiveCTEStep : public ISourceStep
{
public:
    explicit ReadFromRecursiveCTEStep(Block output_header, QueryTreeNodePtr recursive_cte_union_node_);

    String getName() const override { return "ReadFromRecursiveCTEStep"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    QueryTreeNodePtr recursive_cte_union_node;
};

}
