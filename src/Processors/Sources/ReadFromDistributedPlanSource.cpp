#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>

namespace DB
{

Chunk ReadFromDistributedPlanSource::generate()
{
    if (!result)
    {
        auto chunks = executeDistributedQuery(distributed_query_plan, Context::getGlobalContextInstance());
        result = std::make_optional<std::deque<Chunk>>();
        for (auto & chunk : chunks)
            result->emplace_back(std::move(chunk));
    }

    Chunk chunk;
    if (!result->empty())
    {
        chunk = std::move(result->front());
        result->pop_front();
        return chunk;
    }
    else
    {
        return {};
    }
}

}
