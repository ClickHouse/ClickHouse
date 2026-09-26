#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <functional>
#include <memory>

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

class JoinOrderPropertyContext;

LoggerPtr getJoinOrderOptimizerLogger();

DPJoinEntryPtr solveGreedyJoinOrder(QueryGraph & query_graph, const JoinOrderPropertyContext & properties);

DPJoinEntryPtr solveDPSubJoinOrder(QueryGraph & query_graph, const JoinOrderPropertyContext & properties);

DPJoinEntryPtr solveDPSizeJoinOrder(
    QueryGraph & query_graph,
    const JoinOrderPropertyContext & properties,
    UInt64 max_searched_plans,
    QueryStatusPtr query_status,
    std::function<bool()> interactive_cancel_callback);

DPJoinEntryPtr solveDPHypJoinOrder(
    QueryGraph & query_graph,
    const JoinOrderPropertyContext & properties,
    UInt64 max_searched_plans,
    QueryStatusPtr query_status,
    std::function<bool()> interactive_cancel_callback);

}
