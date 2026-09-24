#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// One ActionsDAG for a whole `Expression`/`Filter`/`JoinStepLogical` subtree, with the per-node facts
/// needed to decide where a column can be computed.
///
/// Splitting each step's DAG on its own, as lazy materialization used to, leaks intermediate results
/// across the steps between: a value the filter needs and the result also needs becomes a column that
/// has to be carried through every join above it. Merged into one DAG, such a value is just a node, and
/// a caller is free to recompute it from the source columns instead of carrying it.
///
/// The walk stops at any step whose expressions the DAG cannot represent - an aggregation, a window, an
/// exchange, but also an `arrayJoin`, which changes the number of rows. Such a step becomes a source:
/// its output header turns into the DAG's inputs and the subtree below it is left alone, so the columns
/// above it can still be deferred even though nothing below it can. That is also what keeps a caller
/// inside one distributed fragment.
struct MergedPlanDAG
{
    struct Source
    {
        /// The plan node whose output header the inputs below stand for.
        QueryPlan::Node * plan_node = nullptr;
        /// One input node per column of that header, in header order.
        ActionsDAG::NodeRawConstPtrs inputs;
    };

    /// One per side of a join that the join can leave unmatched, where it stands the side's columns at
    /// their defaults or NULLs for the rows that matched nothing on that side.
    struct Stuffing
    {
        QueryPlan::Node * join_node = nullptr;
        /// 0 for the left side of that join, 1 for the right one.
        size_t side = 0;
    };

    /// A list rather than a vector so that joining two of these is a splice and the pointers below stay
    /// put, which saves renumbering every value's stuffing on the way up.
    using Stuffings = std::list<Stuffing>;

    /// Where a value comes from in the plan: the plan node whose step computes it, and the node of that
    /// step's own DAG it is a copy of. A source's columns have the source's plan node and no step node,
    /// since a source has no DAG of its own here.
    struct Origin
    {
        QueryPlan::Node * plan_node = nullptr;
        const ActionsDAG::Node * step_node = nullptr;
    };

    /// The DAG, plus the sources every node reads. See `getSources`.
    JoinExpressionActions expression_actions;

    /// Filter conditions met on the way up, bottom-up, a join's residual filter included. These decide
    /// which rows survive, so they are computed early wherever a caller draws its frontier.
    ActionsDAG::NodeRawConstPtrs filter_nodes;

    /// Conditions the joins match rows on. A join computes them itself, so they are computed early too.
    ActionsDAG::NodeRawConstPtrs join_condition_nodes;

    /// A position in this vector is the source index reported by `getSources`, which is how the sources
    /// of a value are read off a `BitSet`, so this one has to stay indexable.
    std::vector<Source> sources;
    Stuffings stuffings;

    /// The values with a join above their own computation point, whatever its kind. Letting one of these
    /// cross the `LIMIT` means a column every join above it replicates, and a hash join copies into its
    /// build side, over every row that reaches there.
    NodeSet nodes_with_join_above;

    /// The nearest stuffing above a node's own computation point, for the nodes that have one. Kept
    /// because it cannot be recovered from the DAG afterwards, and it is the only one such a node needs:
    /// a mask column emitted at a join's unmatched side is carried through the joins above it and
    /// stuffed by them in turn, so it already answers "did every join above this point match".
    std::unordered_map<const ActionsDAG::Node *, const Stuffing *> nearest_stuffing;

    /// Every value, mapped to where it comes from. Needed to rebuild the plan step by step: the steps are
    /// not interchangeable - a filter decides which rows the steps above it see, so moving something it
    /// sits below to beneath it can throw where the query does not - so whatever is decided about a value
    /// has to be carried out in the step that computes it.
    std::unordered_map<const ActionsDAG::Node *, Origin> origins;

    /// The other direction, per step: every node of the step's own DAG, its inputs included, mapped to
    /// the value of this DAG it stands for. The keys point into the steps of the plan as they are, so
    /// this stays valid only while those steps are not changed.
    std::unordered_map<const QueryPlan::Node *, ActionsDAG::NodeMapping> step_mappings;

    const ActionsDAG & getDAG() const { return *expression_actions.getActionsDAG(); }

    /// Nodes for the columns of the subtree's output header, in order.
    const ActionsDAG::NodeRawConstPtrs & getOutputs() const { return getDAG().getOutputs(); }

    /// Which sources the node reads. Empty for a constant.
    const BitSet & getSources(const ActionsDAG::Node * node) const;

    /// The stuffing that decides whether this node has a value of its own at all. Where it is set, a
    /// join above this node replaced the node's value by a default or a NULL for the rows that matched
    /// nothing, so the value only means anything where that side matched.
    ///
    /// This cannot be read off the sources a node reads. A node computed *above* a join is not gated by
    /// that join - it ran on the stuffed rows as well, and reproducing it means running it on them again
    /// - while the sources it reads are gated by it. In
    /// `(select k, c + 1 as y from C left join D) r`, joined again from the left, `y` is gated by the
    /// outer join only: at a row where the outer join matched but the inner one did not, `y` is a proper
    /// value computed from a stuffed `d`, and gating it on D's mask as well would throw it away.
    const Stuffing * getNearestStuffing(const ActionsDAG::Node * node) const;

    /// Whether a join sits above this value's own computation point. Not derivable from the stuffings: an
    /// `INNER JOIN` stuffs neither side and replicates all the same.
    bool hasJoinAbove(const ActionsDAG::Node * node) const { return nodes_with_join_above.contains(node); }

    const Origin & getOrigin(const ActionsDAG::Node * node) const;
};

/// Always succeeds: where a step cannot be represented the walk stops and that step becomes a source, so
/// the worst answer is a DAG of one source and no expressions, which a caller finds nothing to defer in.
MergedPlanDAG buildMergedPlanDAG(QueryPlan::Node & root);

}
