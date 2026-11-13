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
class ActionsDAG;
class AggregatingStep;
class Block;
class DistinctStep;
class JoinStepLogical;
class LimitStep;
class OffsetStep;
class ReadFromMergeTree;
class SortingStep;

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
        std::optional<SubstraitBlockingReason> blocking_reason;
    };

    /// Converts the subtree bottom-up. Returns the root's verdict.
    const Verdict & visit(const QueryPlan::Node & root);

    /// Nodes that do not convert while every child subtree does, with reasons. Empty when the whole visited subtree is eligible.
    std::vector<std::pair<const QueryPlan::Node *, SubstraitBlockingReason>> blockingSteps(const QueryPlan::Node & root) const;

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

    std::expected<NodeState, SubstraitBlockingReason> convertStep(const QueryPlan::Node & node, std::vector<NodeState *> children);
    std::expected<NodeState, SubstraitBlockingReason> convertReadFromMergeTree(const ReadFromMergeTree & read);
    std::expected<NodeState, SubstraitBlockingReason> convertFilterOrExpression(
        const ActionsDAG & dag, const Block & output_header, const std::optional<String> & filter_column_name, NodeState & child);
    std::expected<NodeState, SubstraitBlockingReason> convertAggregation(const AggregatingStep & aggregating, NodeState & child);
    std::expected<NodeState, SubstraitBlockingReason> convertSorting(const SortingStep & sorting, NodeState & child);
    std::expected<NodeState, SubstraitBlockingReason> convertLimit(const LimitStep & limit, NodeState & child);
    std::expected<NodeState, SubstraitBlockingReason> convertOffset(const OffsetStep & offset, NodeState & child);
    std::expected<NodeState, SubstraitBlockingReason> convertUnion(const std::vector<NodeState *> & children);
    std::expected<NodeState, SubstraitBlockingReason> convertDistinct(const DistinctStep & distinct, NodeState & child);
    std::expected<NodeState, SubstraitBlockingReason> convertJoin(const JoinStepLogical & join, NodeState & left, NodeState & right);

    std::unordered_map<const QueryPlan::Node *, NodeState> states;
    SubstraitPlanExtensions extensions;
};

}

#endif
