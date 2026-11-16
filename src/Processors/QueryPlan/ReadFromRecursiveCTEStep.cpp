#include <Processors/QueryPlan/ReadFromRecursiveCTEStep.h>

#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/RecursiveCTESource.h>

namespace DB
{

ReadFromRecursiveCTEStep::ReadFromRecursiveCTEStep(SharedHeader output_header_, QueryTreeNodePtr recursive_cte_union_node_)
    : ISourceStep(std::move(output_header_))
    , recursive_cte_union_node(std::move(recursive_cte_union_node_))
{
}

void ReadFromRecursiveCTEStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.init(Pipe(std::make_shared<RecursiveCTESource>(getOutputHeader(), recursive_cte_union_node)));

    /// A recursive CTE is produced by a single stateful source: every iteration reads the result of the
    /// previous one, so the source cannot be parallelized internally and the pipeline starts with a single
    /// stream. Left as is, that single stream pins every downstream operator (aggregation, sorting,
    /// expression evaluation) to one thread, even on a many-core server, which is the dominant cost for
    /// queries that aggregate a large recursive CTE result. Fan the single output stream out to
    /// `max_threads` so downstream steps can process the (potentially large) output in parallel. As a side
    /// effect this also enables the sharded `GROUP BY` optimization (`enable_sharding_aggregator`), which
    /// requires more than one input stream.
    if (settings.max_threads > 1)
        pipeline.resize(settings.max_threads);
}

}
