#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrderBitSet.h>

namespace DB
{

bool connects(const JoinActionRef * predicate, const BitSet & left, const BitSet & right);

DPJoinEntryPtr evaluateJoin(
    const QueryGraph & query_graph,
    PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    JoinKind join_kind,
    std::vector<JoinActionRef *> & predicates,
    LoggerPtr log);

}
