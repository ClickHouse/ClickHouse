#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrderBitSet.h>

namespace DB
{

bool connects(const JoinActionRef * predicate, const BitSet & left, const BitSet & right);

/// True if an equi-join predicate (plain or null-safe equals) connects the two relation sets.
/// Non-equi predicates (ranges, OR, ...) are filters over a cross product, not a join key, so they
/// must not count as a connection - otherwise the optimizer may pick a cartesian product as the
/// cheapest join (its size looks tiny when the inputs have no row estimate).
bool hasEquiConnection(const std::vector<JoinActionRef *> & edges, const BitSet & left, const BitSet & right);

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
