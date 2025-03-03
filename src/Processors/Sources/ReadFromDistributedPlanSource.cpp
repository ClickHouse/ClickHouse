#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>

namespace DB
{

Chunk ReadFromDistributedPlanSource::generate()
{
    executeDistributedQuery(distributed_query_plan, Context::getGlobalContextInstance());

    return {};
}

}
