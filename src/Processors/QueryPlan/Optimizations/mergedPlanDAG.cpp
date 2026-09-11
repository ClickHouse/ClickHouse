#include <Processors/QueryPlan/Optimizations/mergedPlanDAG.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

const BitSet & MergedPlanDAG::getSources(const ActionsDAG::Node * node) const
{
    return JoinActionRef(node, expression_actions).getSourceRelations();
}

std::optional<size_t> MergedPlanDAG::getNearestStuffing(const ActionsDAG::Node * node) const
{
    if (const auto it = nearest_stuffing.find(node); it != nearest_stuffing.end())
        return it->second;

    return {};
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

MergedPlanDAG makeOpaqueSource(QueryPlan::Node & node)
{
    MergedPlanDAG merged;

    MergedPlanDAG::Source source;
    source.plan_node = &node;

    /// The columns of an opaque source are where the DAG bottoms out, so they are its inputs. They are
    /// outputs as well, because that is what the step above binds its own inputs against.
    auto & dag_outputs = merged.expression_actions.getActionsDAG()->getOutputs();
    for (const auto & column : *node.step->getOutputHeader())
    {
        const auto * input = merged.expression_actions.addInput(column.name, column.type, /*source_relation=*/0).getNode();
        source.inputs.push_back(input);
        dag_outputs.push_back(input);
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

std::optional<MergedPlanDAG> buildImpl(QueryPlan::Node & node)
{
    auto * step = node.step.get();

    if (step->hasCorrelatedExpressions())
        return {};

    if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
    {
        const auto & step_dag = expression_step->getExpression();
        /// An `arrayJoin` changes the number of rows, so its result cannot be recomputed elsewhere.
        if (step_dag.hasArrayJoin())
            return {};

        auto merged = buildImpl(*node.children.front());
        if (!merged)
            return {};

        ActionsDAG::NodeMapping clone_mapping;
        if (!mergeStepExpressions(*merged, step_dag, clone_mapping))
            return {};

        if (!outputsMatchHeader(merged->getOutputs(), *step->getOutputHeader()))
            return {};

        return merged;
    }

    if (auto * filter_step = typeid_cast<FilterStep *>(step))
    {
        const auto & step_dag = filter_step->getExpression();
        if (step_dag.hasArrayJoin())
            return {};

        auto merged = buildImpl(*node.children.front());
        if (!merged)
            return {};

        ActionsDAG::NodeMapping clone_mapping;
        if (!mergeStepExpressions(*merged, step_dag, clone_mapping))
            return {};

        /// FilterStep erases the first output of that name from its header, so find it the same way.
        auto & dag_outputs = merged->expression_actions.getActionsDAG()->getOutputs();
        const auto filter_it = std::ranges::find_if(
            dag_outputs, [&](const auto * output) { return output->result_name == filter_step->getFilterColumnName(); });

        if (filter_it == dag_outputs.end())
            return {};

        merged->filter_nodes.push_back(*filter_it);
        if (filter_step->removesFilterColumn())
            dag_outputs.erase(filter_it);

        if (!outputsMatchHeader(merged->getOutputs(), *step->getOutputHeader()))
            return {};

        return merged;
    }

    if (auto * join_step = typeid_cast<JoinStepLogical *>(step))
    {
        if (node.children.size() != 2)
            return {};

        const auto & step_dag = join_step->getActionsDAG();
        if (step_dag.hasArrayJoin())
            return {};

        auto merged = buildImpl(*node.children.front());
        if (!merged)
            return {};

        auto right = buildImpl(*node.children.back());
        if (!right)
            return {};

        /// A join gates every value computed below it on the side it can leave unmatched, so the nodes
        /// of each side are collected before they are all spliced into one list.
        const auto collectNodes = [](const MergedPlanDAG & subtree)
        {
            ActionsDAG::NodeRawConstPtrs collected;
            for (const auto & subtree_node : subtree.getDAG().getNodes())
                collected.push_back(&subtree_node);
            return collected;
        };
        const std::array<ActionsDAG::NodeRawConstPtrs, 2> side_nodes{collectNodes(*merged), collectNodes(*right)};

        /// The join reads the left header first, so uniting in that order lines the columns up with its
        /// inputs, including the columns both sides happen to name alike.
        const size_t source_shift = merged->sources.size();
        auto [right_dag, right_sources] = right->expression_actions.detachActionsDAG();
        merged->expression_actions.getActionsDAG()->unite(std::move(right_dag));

        for (auto & [right_node, node_sources] : right_sources)
            node_sources.shift(source_shift);
        merged->expression_actions.setNodeSources(right_sources);

        merged->sources.append_range(std::move(right->sources));
        merged->filter_nodes.append_range(right->filter_nodes);
        merged->join_condition_nodes.append_range(right->join_condition_nodes);

        merged->nodes_with_join_above.insert(right->nodes_with_join_above.begin(), right->nodes_with_join_above.end());

        const size_t stuffing_shift = merged->stuffings.size();
        merged->stuffings.append_range(std::move(right->stuffings));
        for (const auto & [right_node, stuffing] : right->nearest_stuffing)
            merged->nearest_stuffing.emplace(right_node, stuffing + stuffing_shift);

        ActionsDAG::NodeMapping clone_mapping;
        if (!mergeStepExpressions(*merged, step_dag, clone_mapping))
            return {};

        /// A join outputs what its DAG outputs and drops the rest, while the merge kept the columns of
        /// both sides that the join does not read. Those come after in the output list.
        auto & dag_outputs = merged->expression_actions.getActionsDAG()->getOutputs();
        if (dag_outputs.size() < step_dag.getOutputs().size())
            return {};
        dag_outputs.resize(step_dag.getOutputs().size());

        const auto & join_operator = join_step->getJoinOperator();

        /// Every value computed below this join, on either side, would be replicated by it if it crossed
        /// the `LIMIT` as a column.
        for (const auto & nodes : side_nodes)
            merged->nodes_with_join_above.insert(nodes.begin(), nodes.end());

        /// Every value computed below this join on a side it can leave unmatched is gated by it, unless
        /// a join further down already gates it: that one is the nearer of the two, and its mask column
        /// is stuffed by this join in turn, so it answers for both.
        const auto stuffed_sides = findStuffedSides(join_operator.kind);
        for (size_t side = 0; side < 2; ++side)
        {
            if (!(side == 0 ? stuffed_sides.first : stuffed_sides.second))
                continue;

            const size_t stuffing = merged->stuffings.size();
            merged->stuffings.push_back({&node, side});
            for (const auto * side_node : side_nodes[side])
                merged->nearest_stuffing.try_emplace(side_node, stuffing);
        }

        for (const auto * conditions : {&join_operator.residual_filter, &join_operator.expression})
        {
            for (const auto & condition : *conditions)
            {
                auto it = clone_mapping.find(condition.getNode());
                if (it == clone_mapping.end())
                    return {};

                if (conditions == &join_operator.residual_filter)
                    merged->filter_nodes.push_back(it->second);
                else
                    merged->join_condition_nodes.push_back(it->second);
            }
        }

        if (!outputsMatchHeader(merged->getOutputs(), *step->getOutputHeader()))
            return {};

        return merged;
    }

    return makeOpaqueSource(node);
}

}

std::optional<MergedPlanDAG> buildMergedPlanDAG(QueryPlan::Node & root)
{
    if (!root.step->hasOutputHeader())
        return {};

    return buildImpl(root);
}

}
