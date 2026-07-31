#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// Which unsafe function classes a lambda body contains.
///
/// The two classes are reported separately because their consumers relax them differently: filter
/// push-down can be told to tolerate non-deterministic functions (`allow_non_deterministic_functions`),
/// while statefulness is vetoed for a whole plan step.
struct LambdaBodySafety
{
    bool has_non_deterministic = false;   /// some inner function is !isDeterministicInScopeOfQuery()
    bool has_stateful = false;            /// some inner function is isStateful()
};

/// Report the unsafe classes hidden in the lambda bodies held by `node` itself.
///
/// A lambda body is NOT part of the DAG that contains `node`: it lives in an ActionsDAG owned either
/// by a FunctionCapture (`node` is a FUNCTION) or by a FunctionExpression inside a ColumnFunction
/// (`node` is a COLUMN, the shape a constant-folded lambda takes). Neither wrapper overrides
/// isDeterministicInScopeOfQuery or isStateful, so testing only `node.function_base` says nothing
/// about what the body computes.
///
/// Nested lambdas inside those bodies are followed. `node`'s own metadata and its children are NOT
/// inspected: every caller already walks the outer DAG node by node, so doing it here too would be
/// quadratic.
///
/// Never throws: this is an optimizer predicate, so an unrecognized shape is skipped.
///
/// Deliberately shares no visited state with the whole-DAG entry points below: those walk a whole
/// DAG and can reuse a visited set across its nodes, while this one is called per node and must
/// stay stateless, or a later call would skip a body an earlier node already saw.
LambdaBodySafety inspectLambdaBodies(const ActionsDAG::Node & node);

/// True if any node of `dag` holds a lambda body containing a stateful function.
/// Complements ActionsDAG::hasStatefulFunctions, which only sees the outer DAG.
bool hasStatefulFunctionsInLambdaBodies(const ActionsDAG & dag);

/// True if any node of `dag` holds a lambda body containing a function that is not deterministic in
/// the scope of the query. Complements the outer-DAG-only scans, which cannot see into a body.
bool hasNonDeterministicFunctionsInLambdaBodies(const ActionsDAG & dag);

}
