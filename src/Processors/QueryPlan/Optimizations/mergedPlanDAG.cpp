#include <Processors/QueryPlan/Optimizations/mergedPlanDAG.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/typeid_cast.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

const BitSet & MergedPlanDAG::getSources(const ActionsDAG::Node * node) const
{
    return JoinActionRef(node, expression_actions).getSourceRelations();
}

const MergedPlanDAG::Stuffing * MergedPlanDAG::getNearestStuffing(const ActionsDAG::Node * node) const
{
    if (const auto it = nearest_stuffing.find(node); it != nearest_stuffing.end())
        return it->second;

    return nullptr;
}

const MergedPlanDAG::Origin & MergedPlanDAG::getOrigin(const ActionsDAG::Node * node) const
{
    const auto it = origins.find(node);
    if (it == origins.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Value {} of the merged plan DAG has no origin", node->result_name);

    return it->second;
}

namespace
{

/// Which side of a join can end up with rows its columns took no part in. A kind that keeps every row of
/// one side has to fill the other side in for the rows that matched nothing there.
std::pair<bool, bool> findStuffedSides(JoinKind kind)
{
    switch (kind)
    {
        case JoinKind::Inner:
        case JoinKind::Cross:
        case JoinKind::Comma:
            return {false, false};
        /// Includes LEFT ANTI, where the right side stands at its defaults for every row produced.
        case JoinKind::Left:
            return {false, true};
        case JoinKind::Right:
            return {true, false};
        case JoinKind::Full:
            return {true, true};
        /// `PASTE` matches rows by position rather than by a condition. Take the careful answer.
        case JoinKind::Paste:
            return {true, true};
    }

    return {true, true};
}

/// `unite` and `mergeInplace` splice their nodes onto the end of the list, so what a merge added is the
/// tail behind whatever was last before it, and a list iterator taken beforehand stays valid.
using NodeIterator = ActionsDAG::Nodes::const_iterator;

std::optional<NodeIterator> lastNodeBefore(const ActionsDAG & dag)
{
    const auto & nodes = dag.getNodes();
    if (nodes.empty())
        return {};
    return std::prev(nodes.end());
}

NodeIterator firstNodeAdded(const ActionsDAG & dag, const std::optional<NodeIterator> & last_before)
{
    return last_before ? std::next(*last_before) : dag.getNodes().begin();
}

bool outputsMatchHeader(const ActionsDAG::NodeRawConstPtrs & outputs, const Block & header)
{
    if (outputs.size() != header.columns())
        return false;

    for (size_t position = 0; position < outputs.size(); ++position)
    {
        const auto & column = header.getByPosition(position);
        if (outputs[position]->result_name != column.name || !outputs[position]->result_type->equals(*column.type))
            return false;
    }

    return true;
}

/// Starts a DAG that reads one source: the columns `node` produces become its inputs, and `node` itself
/// becomes the source those inputs are attributed to. Whatever `node`'s step does to produce them is not
/// looked at - either it is a read, which is what a caller defers columns of, or it is a step this
/// cannot represent, and stopping here is how such a step is handled.
MergedPlanDAG startDAGFromSource(QueryPlan::Node & node)
{
    MergedPlanDAG merged;

    MergedPlanDAG::Source source;
    source.plan_node = &node;

    /// The columns of a source are where the DAG bottoms out, so they are its inputs. They are outputs
    /// as well, because that is what the step above binds its own inputs against.
    auto & dag_outputs = merged.expression_actions.getActionsDAG()->getOutputs();
    for (const auto & column : *node.step->getOutputHeader())
    {
        const auto * input = merged.expression_actions.addInput(column.name, column.type, /*source_relation=*/0).getNode();
        source.inputs.push_back(input);
        dag_outputs.push_back(input);
        merged.origins.emplace(input, MergedPlanDAG::Origin{&node, nullptr});
    }

    merged.sources.push_back(std::move(source));
    return merged;
}

/// Merges a step's expressions on top of what the subtree below produces. `step_dag` is bound to that
/// subtree by name, in order, which is the rule `ActionsDAG::updateHeader` follows to build the step's
/// input block, so a name occurring twice binds the same way here as it does at execution.
bool mergeStepExpressions(MergedPlanDAG & merged, const ActionsDAG & step_dag, ActionsDAG::NodeMapping & clone_mapping)
{
    auto & dag = *merged.expression_actions.getActionsDAG();

    const size_t inputs_before = dag.getInputs().size();

    ActionsDAG::NodeMapping inputs_mapping;
    dag.mergeInplace(step_dag.clone(clone_mapping), inputs_mapping, /*remove_dangling_inputs=*/true);

    /// An input of the step that matched no column below became an input of the merged DAG, which means
    /// the columns do not line up the way this builder assumes.
    if (dag.getInputs().size() != inputs_before)
        return false;

    /// The step's own nodes are spliced in rather than copied again, so a node of the clone is a node of
    /// the merged DAG now - except for the inputs, which the merge resolved to nodes below.
    for (auto & [original, cloned] : clone_mapping)
    {
        if (original->type != ActionsDAG::ActionType::INPUT)
            continue;

        auto it = inputs_mapping.find(cloned);
        if (it != inputs_mapping.end())
            cloned = it->second;
    }

    return true;
}

/// A subtree, plus what the build still has to track about it on the way up.
struct Built
{
    MergedPlanDAG dag;

    /// Values no join below has gated yet. A stuffing join above gates exactly these, so each value is
    /// visited once over the whole build rather than once per join it sits under.
    ActionsDAG::NodeRawConstPtrs ungated_nodes;

    /// The last value created below the outermost join seen so far. Everything before it in the node
    /// list is what that join sits above - and an inner join sits above a subset of the same values, so
    /// filling `nodes_with_join_above` once from here at the end says the same thing as filling it at
    /// every join would.
    std::optional<NodeIterator> last_node_below_top_join;
};

Built buildImpl(QueryPlan::Node & node);

/// Returns nullopt when the step cannot be represented, which is not a failure: the caller stops the
/// walk there and the step becomes a source.
std::optional<Built> tryBuildFromStep(QueryPlan::Node & node)
{
    auto * step = node.step.get();

    if (step->hasCorrelatedExpressions())
        return {};

    const auto mergeAndTrack = [&node](Built & built, const ActionsDAG & step_dag, ActionsDAG::NodeMapping & clone_mapping)
    {
        auto & dag = *built.dag.expression_actions.getActionsDAG();
        const auto last_before = lastNodeBefore(dag);

        if (!mergeStepExpressions(built.dag, step_dag, clone_mapping))
            return false;

        /// An input of the step stands for a value computed below, which has its origin already.
        for (const auto & [original, merged_node] : clone_mapping)
            if (original->type != ActionsDAG::ActionType::INPUT)
                built.dag.origins.emplace(merged_node, MergedPlanDAG::Origin{&node, original});

        built.dag.step_mappings.emplace(&node, clone_mapping);

        /// Nothing below this step gates what it computes: a join further down gated the values this
        /// reads, and their answers already stand where that join left them.
        for (auto it = firstNodeAdded(dag, last_before); it != dag.getNodes().end(); ++it)
            built.ungated_nodes.push_back(&*it);

        return true;
    };

    if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
    {
        const auto & step_dag = expression_step->getExpression();
        /// An `arrayJoin` changes the number of rows, so what it produces cannot be had above the
        /// `LIMIT`, which counted them. Stopping here defers the columns above it all the same.
        ///
        /// Reading columns lazily from below an `arrayJoin` is not out of the question - the row index
        /// would be replicated along with every other column, which is what the transform already sorts
        /// out for the replication a join does - but nothing downstream of this is ready to be asked
        /// that yet.
        if (step_dag.hasArrayJoin())
            return {};

        auto built = buildImpl(*node.children.front());

        ActionsDAG::NodeMapping clone_mapping;
        if (!mergeAndTrack(built, step_dag, clone_mapping))
            return {};

        if (!outputsMatchHeader(built.dag.getOutputs(), *step->getOutputHeader()))
            return {};

        return built;
    }

    if (auto * filter_step = typeid_cast<FilterStep *>(step))
    {
        const auto & step_dag = filter_step->getExpression();
        if (step_dag.hasArrayJoin())
            return {};

        auto built = buildImpl(*node.children.front());

        ActionsDAG::NodeMapping clone_mapping;
        if (!mergeAndTrack(built, step_dag, clone_mapping))
            return {};

        /// FilterStep erases the first output of that name from its header, so find it the same way.
        auto & dag_outputs = built.dag.expression_actions.getActionsDAG()->getOutputs();
        const auto filter_it = std::ranges::find_if(
            dag_outputs, [&](const auto * output) { return output->result_name == filter_step->getFilterColumnName(); });

        if (filter_it == dag_outputs.end())
            return {};

        built.dag.filter_nodes.push_back(*filter_it);
        if (filter_step->removesFilterColumn())
            dag_outputs.erase(filter_it);

        if (!outputsMatchHeader(built.dag.getOutputs(), *step->getOutputHeader()))
            return {};

        return built;
    }

    auto * join_step = typeid_cast<JoinStepLogical *>(step);
    if (!join_step || node.children.size() != 2)
        return {};

    const auto & step_dag = join_step->getActionsDAG();
    if (step_dag.hasArrayJoin())
        return {};

    auto built = buildImpl(*node.children.front());
    auto right = buildImpl(*node.children.back());

    /// The join reads the left header first, so uniting in that order lines the columns up with its
    /// inputs, including the columns both sides happen to name alike.
    const size_t source_shift = built.dag.sources.size();
    auto [right_dag, right_sources] = right.dag.expression_actions.detachActionsDAG();
    built.dag.expression_actions.getActionsDAG()->unite(std::move(right_dag));

    for (auto & [right_node, node_sources] : right_sources)
        node_sources.shift(source_shift);
    built.dag.expression_actions.setNodeSources(right_sources);

    built.dag.sources.append_range(std::move(right.dag.sources));
    built.dag.filter_nodes.append_range(right.dag.filter_nodes);
    built.dag.join_condition_nodes.append_range(right.dag.join_condition_nodes);

    /// Both of these move rather than copy: the stuffings are a list, so this is a relink, and the
    /// values map points at them, so nothing has to be renumbered.
    built.dag.stuffings.splice(built.dag.stuffings.end(), right.dag.stuffings);
    built.dag.nearest_stuffing.merge(right.dag.nearest_stuffing);
    built.dag.origins.merge(right.dag.origins);
    built.dag.step_mappings.merge(right.dag.step_mappings);

    /// This join sits above everything either side computed, and above everything the joins below them
    /// sit above, so its own mark is the only one the result needs.
    auto & dag = *built.dag.expression_actions.getActionsDAG();
    built.last_node_below_top_join = lastNodeBefore(dag);

    /// Every value still ungated on a side this join can leave unmatched is gated by it. A value gated
    /// further down keeps that one: it is the nearer, and its mask is stuffed by this join in turn, so
    /// it answers for both.
    const auto & join_operator = join_step->getJoinOperator();
    const auto stuffed_sides = findStuffedSides(join_operator.kind);
    const std::array<bool, 2> side_is_stuffed{stuffed_sides.first, stuffed_sides.second};
    const std::array<ActionsDAG::NodeRawConstPtrs *, 2> side_ungated{&built.ungated_nodes, &right.ungated_nodes};

    ActionsDAG::NodeRawConstPtrs still_ungated;
    for (size_t side = 0; side < 2; ++side)
    {
        if (!side_is_stuffed[side])
        {
            still_ungated.append_range(*side_ungated[side]);
            continue;
        }

        const auto & stuffing = built.dag.stuffings.emplace_back(MergedPlanDAG::Stuffing{&node, side});
        for (const auto * ungated : *side_ungated[side])
            built.dag.nearest_stuffing.emplace(ungated, &stuffing);
    }
    built.ungated_nodes = std::move(still_ungated);

    ActionsDAG::NodeMapping clone_mapping;
    if (!mergeAndTrack(built, step_dag, clone_mapping))
        return {};

    /// A join outputs what its DAG outputs and drops the rest, while the merge kept the columns of both
    /// sides that the join does not read. Those come after in the output list.
    auto & dag_outputs = dag.getOutputs();
    if (dag_outputs.size() < step_dag.getOutputs().size())
        return {};
    dag_outputs.resize(step_dag.getOutputs().size());

    for (const auto * conditions : {&join_operator.residual_filter, &join_operator.expression})
    {
        for (const auto & condition : *conditions)
        {
            auto it = clone_mapping.find(condition.getNode());
            if (it == clone_mapping.end())
                return {};

            if (conditions == &join_operator.residual_filter)
                built.dag.filter_nodes.push_back(it->second);
            else
                built.dag.join_condition_nodes.push_back(it->second);
        }
    }

    if (!outputsMatchHeader(built.dag.getOutputs(), *step->getOutputHeader()))
        return {};

    return built;
}

Built buildImpl(QueryPlan::Node & node)
{
    /// A step that cannot be represented - because of what it computes, or because the DAG built for it
    /// does not reproduce its header, which would mean optimizing on a wrong model of the plan - is
    /// where the walk stops, with that step standing in as a source.
    if (auto built = tryBuildFromStep(node))
        return std::move(*built);

    Built built;
    built.dag = startDAGFromSource(node);
    built.ungated_nodes = built.dag.sources.front().inputs;
    return built;
}

}

MergedPlanDAG buildMergedPlanDAG(QueryPlan::Node & root)
{
    auto built = buildImpl(root);

    /// Filled once, from the outermost join: everything created before it is what some join sits above.
    if (built.last_node_below_top_join)
    {
        const auto & nodes = built.dag.getDAG().getNodes();
        for (auto it = nodes.begin(); it != std::next(*built.last_node_below_top_join); ++it)
            built.dag.nodes_with_join_above.insert(&*it);
    }

    return std::move(built.dag);
}

}
