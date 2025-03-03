#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>

namespace DB
{

struct DistributedQueryPlan;

Chunks executeDistributedQuery(const DistributedQueryPlan & distributed_query_plan, ContextPtr context);

}
