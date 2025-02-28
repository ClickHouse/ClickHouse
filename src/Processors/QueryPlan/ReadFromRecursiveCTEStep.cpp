#include <Processors/QueryPlan/ReadFromRecursiveCTEStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/RecursiveCTESource.h>

namespace DB
{

ReadFromRecursiveCTEStep::ReadFromRecursiveCTEStep(Block output_header, QueryTreeNodePtr recursive_cte_union_node_)
    : ISourceStep(DataStream{.header = std::move(output_header)})
    , recursive_cte_union_node(std::move(recursive_cte_union_node_))
{
}

void ReadFromRecursiveCTEStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<RecursiveCTESource>(getOutputStream().header, recursive_cte_union_node)));
}

}
