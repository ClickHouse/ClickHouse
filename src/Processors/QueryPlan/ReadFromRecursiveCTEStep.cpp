#include <Processors/QueryPlan/ReadFromRecursiveCTEStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/RecursiveCTESource.h>

namespace DB
{

ReadFromRecursiveCTEStep::ReadFromRecursiveCTEStep(Block output_header_, QueryTreeNodePtr recursive_cte_union_node_)
    : ISourceStep(std::move(output_header_))
    , recursive_cte_union_node(std::move(recursive_cte_union_node_))
{
}

void ReadFromRecursiveCTEStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<RecursiveCTESource>(getOutputHeader(), recursive_cte_union_node)));
}

}
