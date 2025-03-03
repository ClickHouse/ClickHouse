#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

struct DistributedQueryPlan;

void executeDistributedQuery(const DistributedQueryPlan & distributed_query_plan, ContextPtr context);

}
