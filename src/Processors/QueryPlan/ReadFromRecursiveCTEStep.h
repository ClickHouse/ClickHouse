#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Block_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

class ReadFromRecursiveCTEStep : public ISourceStep
{
public:
    explicit ReadFromRecursiveCTEStep(SharedHeader output_header, QueryTreeNodePtr recursive_cte_union_node_);

    String getName() const override { return "ReadFromRecursiveCTEStep"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    QueryTreeNodePtr recursive_cte_union_node;
};

}
