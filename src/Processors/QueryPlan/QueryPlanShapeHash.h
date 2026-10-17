#pragma once

#include <base/types.h>

#include <unordered_map>


namespace DB
{

class QueryPlan;

/// Hashes of the shape of a plan: equal for two plans that differ only in their constants, and
/// different whenever planning or optimization made a different choice.
///
/// Each step hashes what was chosen for it -- the step type, the shape of its expressions, its
/// keys, the table and indexes it reads, the join algorithm -- and none of its literals, runtime
/// data or names. Names are left out because the planner names a constant after its value
/// (`5_UInt8`) and that name propagates into every expression above it, so a column is identified
/// by its position in the step's input header instead.
///
/// A step's hash also covers its children and the plans it owns, so the root's hash is the plan's.
///
/// Not stable across releases: a release that changes what the optimizer does changes the plans,
/// and their hashes with them.
struct QueryPlanShapeHashes
{
    UInt64 root = 0;

    /// Hash of the subtree under each step, keyed by `IQueryPlanStep::getUniqID`.
    std::unordered_map<String, UInt64> steps;
};

/// Must run after the plan is optimized and before its pipeline is built, since building the
/// pipeline moves the `ActionsDAG` out of every expression step.
///
/// Only the child plans the steps have already built are hashed, so that hashing a plan does not
/// change the work the query does (see `IQueryPlanStep::getBuiltChildPlans`).
QueryPlanShapeHashes hashQueryPlanShape(const QueryPlan & plan);

/// `hash` as the fixed-width hex string the plan document carries it as. A JSON number cannot carry
/// it: readers that parse numbers as doubles, JavaScript among them, lose everything past 2^53.
String formatQueryPlanShapeHash(UInt64 hash);

}
