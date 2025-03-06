#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>

namespace DB
{

Chunk ReadFromDistributedPlanSource::generate()
{
    if (!executed)
    {
        executeDistributedQuery(unique_query_id, distributed_query_plan, Context::getGlobalContextInstance());
        executed = true;
    }

    return {};
}

}
