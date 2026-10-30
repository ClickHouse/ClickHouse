#pragma once

#include "config.h"

#if USE_SUBSTRAIT

#include <expected>
#include <unordered_map>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Substrait/SubstraitCommon.h>
#include <substrait/plan.pb.h>

/// Top layer and public face of the Substrait serializer. Converts QueryPlan to substrait::Plan.

namespace DB
{

struct QueryPlanOptimizationSettings;

/// The plan shape the serializer expects, apply before QueryPlan::optimize. Joins stay logical and filters do not fold into PREWHERE reads.
void applySubstraitSerializationSettings(QueryPlanOptimizationSettings & settings);

substrait::Plan serializeQueryPlanToSubstrait(const QueryPlan & plan);
String serializeQueryPlanToSubstraitJSON(const QueryPlan & plan);

/// Serializes QueryPlan subtrees to Substrait, bottom-up with per-node memoization.
/// A subtree is eligible when this serializer converts it. Converters return a blocking reason instead of throwing.
/// "... has no Substrait representation" is an expected answer, not an error.
/// A step is attempted only after all its children converted, and a successful conversion moves the child relation into the parent.
class SubstraitPlanSerializer
{
public:
    struct Verdict
    {
        bool subtree_eligible = false;
        String blocking_reason;
    };

    /// Converts the subtree bottom-up. Returns the root's verdict.
    const Verdict & visit(const QueryPlan::Node & root);

    /// Verdict for a visited node. Throw if unvisited.
    const Verdict & verdictFor(const QueryPlan::Node & node) const;

    /// Nodes that do not convert while every child subtree does, with reasons. Empty when the whole visited subtree is eligible.
    std::vector<std::pair<const QueryPlan::Node *, String>> blockingSteps(const QueryPlan::Node & root) const;

    /// Builds the final plan for a node with subtree_eligible = true, consuming the subtree's converted relations.
    substrait::Plan takePlan(const QueryPlan::Node & root);

private:
    struct NodeState
    {
        Verdict verdict;
        substrait::Rel rel;
        SubstraitSchema schema;
        bool sort_reaches_output = false; /// Some ClickHouse steps preserve input order while the Substrait equivalents may not.
    };

    std::expected<NodeState, String> convertStep(const QueryPlan::Node & node, std::vector<NodeState *> children);

    std::unordered_map<const QueryPlan::Node *, NodeState> states;
    SubstraitPlanExtensions extensions;
};

}

#endif
