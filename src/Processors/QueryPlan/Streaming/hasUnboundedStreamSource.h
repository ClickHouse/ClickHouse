#pragma once

namespace DB
{

class QueryPlan;

bool hasUnboundedStreamSource(const QueryPlan & query_plan);

}
