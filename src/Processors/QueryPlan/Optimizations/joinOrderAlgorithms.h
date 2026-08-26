#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

namespace DB
{

DPJoinEntryPtr solveGreedyJoinOrder(QueryGraph & query_graph);

}
