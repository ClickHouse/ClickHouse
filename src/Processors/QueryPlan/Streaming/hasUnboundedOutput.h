#pragma once

namespace DB
{

class QueryPlan;

bool hasUnboundedOutput(const QueryPlan & query_plan);

}
