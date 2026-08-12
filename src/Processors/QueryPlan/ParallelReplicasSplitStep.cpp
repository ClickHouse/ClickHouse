#include <Processors/QueryPlan/ParallelReplicasSplitStep.h>

namespace DB
{

void ParallelReplicasSplitStep::transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &)
{
    /// Pass-through when executed directly (no split was applied).
}

}
