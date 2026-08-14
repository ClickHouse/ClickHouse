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
/// A step whose expressions the DAG cannot represent - an aggregation, a window, an exchange - becomes
/// an opaque source instead: its output header turns into DAG inputs and the subtree below it is left
/// alone. That is also what keeps a caller inside one distributed fragment.
struct MergedPlanDAG
{
    struct Source
    {
        /// The plan node whose output header the inputs below stand for.
        QueryPlan::Node * plan_node = nullptr;
        /// One input node per column of that header, in header order.
        ActionsDAG::NodeRawConstPtrs inputs;
        /// Whether a join above this source can produce rows its columns took no part in, where they
        /// stand at their default or NULL. Only then does it matter that a value was computed below that
        /// join: recomputing it above would run it on those rows, and for `x + 1` over a stuffed `x = 0`
        /// that gives 1 where the plan gives the default 0.
        bool may_be_stuffed = false;
    };

    /// The DAG, plus the sources every node reads. See `getSources`.
    JoinExpressionActions expression_actions;

    /// Filter conditions met on the way up, bottom-up, a join's residual filter included. These decide
    /// which rows survive, so they are computed early wherever a caller draws its frontier.
    ActionsDAG::NodeRawConstPtrs filter_nodes;

    /// Conditions the joins match rows on. A join computes them itself, so they are computed early too.
    ActionsDAG::NodeRawConstPtrs join_condition_nodes;

    /// A position in this vector is the source index reported by `getSources`.
    std::vector<Source> sources;

    /// Nodes computed where only one source's rows existed, that is below every join on their path.
    /// Kept because it cannot be recovered from the DAG afterwards: a node computed above a join that
    /// happens to read one source looks exactly the same.
    NodeSet nodes_below_joins;

    const ActionsDAG & getDAG() const { return *expression_actions.getActionsDAG(); }

    /// Nodes for the columns of the subtree's output header, in order.
    const ActionsDAG::NodeRawConstPtrs & getOutputs() const { return getDAG().getOutputs(); }

    /// Which sources the node reads. Empty for a constant.
    const BitSet & getSources(const ActionsDAG::Node * node) const;

    /// The source whose rows alone are enough to recompute this node, if there is one. Unset for a node
    /// reading more than one source, and for a node computed above a join: there it also ran on the rows
    /// the join stuffed with defaults or NULLs, and recomputing it on the matched rows only would not
    /// reproduce that.
    std::optional<size_t> getDenseSource(const ActionsDAG::Node * node) const;

    /// The source on whose own rows this node has to be recomputed, if it cannot be recomputed after the
    /// join instead. That is the case only where the join can stuff rows, see `Source::may_be_stuffed`;
    /// with one source and no join above it, nothing is stuffed and anything may be recomputed late.
    std::optional<size_t> getSourceToRecomputeOn(const ActionsDAG::Node * node) const;
};

/// Returns nullopt when the subtree cannot be represented: it computes an `arrayJoin`, which changes the
/// number of rows; it holds correlated expressions; or the DAG built for it does not reproduce the
/// header of some step, which means the model of that step is wrong and the result cannot be trusted.
std::optional<MergedPlanDAG> buildMergedPlanDAG(QueryPlan::Node & root);

}
