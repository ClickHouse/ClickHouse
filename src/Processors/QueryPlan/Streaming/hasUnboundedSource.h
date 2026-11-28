#pragma once

namespace DB
{

class QueryPlan;

bool hasUnboundedSource(const QueryPlan & query_plan);

}
