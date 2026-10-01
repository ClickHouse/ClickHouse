#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

std::optional<JoinKind> isValidJoinOrder(
    const QueryGraph & query_graph,
    const BitSet & left_mask,
    const BitSet & right_mask);

VectorWithMemoryTracking<JoinActionRef *> getApplicableExpressions(
    QueryGraph & query_graph,
    const BitSet & left,
    const BitSet & right);

double computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const VectorWithMemoryTracking<JoinActionRef *> & edges,
    const BitSet & left,
    const BitSet & right);

}
