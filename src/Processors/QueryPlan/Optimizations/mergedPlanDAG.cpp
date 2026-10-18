#include <Processors/QueryPlan/Optimizations/mergedPlanDAG.h>

#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Common/typeid_cast.h>

#include <ranges>

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

size_t MergedPlanDAG::getSourceOfInput(const ActionsDAG::Node * input) const
{
    const auto & input_sources = getSources(input);
    if (input->type != ActionsDAG::ActionType::INPUT || input_sources.count() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node {} of the merged plan DAG is not an input of one source, it reads {}", input->result_name, input_sources.count());
    return *input_sources.begin();
}

const MergedPlanDAG::PaddedSide * MergedPlanDAG::getNearestPaddedSideOfSource(size_t source) const
{
    const auto & inputs = sources.at(source).inputs;
    return inputs.empty() ? nullptr : getNearestPaddedSide(inputs.front());
}

const MergedPlanDAG::PaddedSide * MergedPlanDAG::getNearestPaddedSide(const ActionsDAG::Node * node) const
{
    if (const auto it = nearest_padded_side.find(node); it != nearest_padded_side.end())
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
std::array<bool, 2> findPaddedSides(JoinKind kind)
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
        /// `PASTE` matches rows by position rather than by a condition, and cuts both sides to the shorter one rather than
        /// padding either. The careful answer below costs at most a mask that is never false, or a value carried that
        /// could have been recomputed, and does not depend on that.
        case JoinKind::Paste:
            break;
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

    merged.step_outputs.emplace(&node, source.inputs);
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

    /// Values no join below has gated yet. A join above that pads their side gates exactly these, so each value is
    /// visited once over the whole build rather than once per join it sits under.
    ActionsDAG::NodeRawConstPtrs ungated_nodes;

    /// The last value created below the outermost join seen so far. Everything before it in the node
    /// list is what that join sits above - and an inner join sits above a subset of the same values, so
    /// filling `nodes_with_join_above` once from here at the end says the same thing as filling it at
    /// every join would.
    std::optional<NodeIterator> last_node_below_top_join;

    /// Values a join's own DAG computes before the join: its keys, and what it passes through, such as
    /// the `toNullable` wrappers of `join_use_nulls`. Those are below that join even though they come after
    /// the mark above in the node list.
    ActionsDAG::NodeRawConstPtrs pre_join_nodes;
};

}

bool canEnterStep(const QueryPlan::Node & node)
{
    const auto * step = node.step.get();

    if (step->hasCorrelatedExpressions())
        return false;

    /// A step that cannot prune its columns keeps them all, which a caller pruning the plan cannot have.
    if (!step->canRemoveUnusedColumns())
        return false;

    /// An `arrayJoin` changes the number of rows, so what it produces cannot be had above the `LIMIT`, which
    /// counted them. Stopping here defers the columns above it all the same.
    ///
    /// Reading columns lazily from below an `arrayJoin` is not out of the question - the row index would be
    /// replicated along with every other column, which is what the transform already sorts out for the
    /// replication a join does - but nothing downstream of this is ready to be asked that yet.
    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
        return node.children.size() == 1 && !expression_step->getExpression().hasArrayJoin();

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        return node.children.size() == 1 && !filter_step->getExpression().hasArrayJoin();

    if (typeid_cast<const BuildRuntimeFilterStep *>(step))
        return node.children.size() == 1;

    /// A join reads a prepared storage, as a direct join does, from a lookup that has to stay its immediate child, which a
    /// step added above the lookup - a copy of a carried column - would not be. Such a join is a source as a whole.
    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step))
        return node.children.size() == 2 && !join_step->getActionsDAG().hasArrayJoin()
            && std::ranges::none_of(node.children, [](const auto * child) { return typeid_cast<const JoinStepLogicalLookup *>(child->step.get()); });

    return false;
}

namespace
{

/// The node of the merged DAG a node of a step's own DAG became. Every node of the step's DAG has one.
const ActionsDAG::Node * cloneOf(const ActionsDAG::NodeMapping & clone_mapping, const ActionsDAG::Node * node)
{
    const auto it = clone_mapping.find(node);
    if (it == clone_mapping.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} of a step has no node in the merged plan DAG", node->result_name);
    return it->second;
}

/// Unites what the right side of a join built into what the left side built; the sources of the right side are numbered
/// after the ones of the left side. The join reads the left header first, so uniting in that order lines the columns up
/// with its inputs, including the columns both sides happen to name alike.
void uniteRightSide(Built & built, Built & right)
{
    const size_t source_shift = built.dag.sources.size();
    auto [right_dag, right_sources] = right.dag.expression_actions.detachActionsDAG();
    built.dag.expression_actions.getActionsDAG()->unite(std::move(right_dag));

    for (auto & [right_node, node_sources] : right_sources)
        node_sources.shift(source_shift);
    built.dag.expression_actions.setNodeSources(right_sources);

    built.dag.sources.append_range(std::move(right.dag.sources));
    built.dag.filter_nodes.append_range(right.dag.filter_nodes);
    built.dag.join_condition_nodes.append_range(right.dag.join_condition_nodes);
    built.dag.step_read_nodes.append_range(right.dag.step_read_nodes);

    /// Both of these move rather than copy: the padded sides are a list, so this is a relink, and the
    /// values map points at them, so nothing has to be renumbered.
    built.dag.padded_sides.splice(built.dag.padded_sides.end(), right.dag.padded_sides);
    built.dag.nearest_padded_side.merge(right.dag.nearest_padded_side);
    built.dag.origins.merge(right.dag.origins);
    built.dag.step_outputs.merge(right.dag.step_outputs);
    built.pre_join_nodes.append_range(right.pre_join_nodes);
}

/// The padded sides of the join of `node`, by side, null for a side it does not pad. Every value still ungated on a
/// side this join can leave unmatched is gated by it. A value gated further down keeps that one: it is the nearer, and
/// its mask is padded by this join in turn, so it answers for both.
std::array<const MergedPlanDAG::PaddedSide *, 2> gatePaddedSides(Built & built, Built & right, QueryPlan::Node & node, JoinKind kind)
{
    const auto side_is_padded = findPaddedSides(kind);
    const std::array<ActionsDAG::NodeRawConstPtrs *, 2> side_ungated{&built.ungated_nodes, &right.ungated_nodes};

    std::array<const MergedPlanDAG::PaddedSide *, 2> join_padded_sides{nullptr, nullptr};
    ActionsDAG::NodeRawConstPtrs still_ungated;
    for (size_t side = 0; side < 2; ++side)
    {
        if (!side_is_padded[side])
        {
            still_ungated.append_range(*side_ungated[side]);
            continue;
        }

        const auto & padded_side = built.dag.padded_sides.emplace_back(MergedPlanDAG::PaddedSide{&node, side});
        join_padded_sides[side] = &padded_side;
        for (const auto * ungated : *side_ungated[side])
            built.dag.nearest_padded_side.emplace(ungated, &padded_side);
    }
    built.ungated_nodes = std::move(still_ungated);
    return join_padded_sides;
}

/// The join's own DAG computes some values before the join - its keys, and the columns it hands on, wrapped in
/// `toNullable` under `join_use_nulls` - and the rest after it, from `actions_after_join` up. A value of the first kind
/// reads one side only and is padded by the join like any column of that side, so it is gated by the padded side of the
/// join it is on, and it is below the join. The side is the one of the join's inputs it reads, which the join's own DAG
/// knows: the nodes below that the merge put in place of those inputs need not read any table, as a constant of a
/// subquery does not.
void trackPreJoinNodes(
    Built & built,
    const JoinStepLogical & join_step,
    const ActionsDAG::NodeMapping & clone_mapping,
    const std::array<const MergedPlanDAG::PaddedSide *, 2> & join_padded_sides)
{
    ActionsDAG::NodeRawConstPtrs roots;
    for (const auto * after_join : join_step.getActionsAfterJoin())
        roots.push_back(cloneOf(clone_mapping, after_join));
    for (const auto & key : join_step.getJoinOperator().expression)
        roots.push_back(cloneOf(clone_mapping, key.getNode()));

    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> original_of;
    NodeSet created_here;
    for (const auto & [original, merged_node] : clone_mapping)
    {
        if (original->type == ActionsDAG::ActionType::INPUT)
            continue;
        created_here.insert(merged_node);
        original_of.emplace(merged_node, original);
    }

    const auto is_below = [&](const ActionsDAG::Node * candidate) { return !created_here.contains(candidate); };

    NodeSet gated_here;
    for (const auto * pre_join : findReachableNodes(roots, is_below))
    {
        if (!created_here.contains(pre_join))
            continue;

        built.pre_join_nodes.push_back(pre_join);

        const JoinActionRef in_join(original_of.at(pre_join), join_step.getExpressionActions());
        const MergedPlanDAG::PaddedSide * padded_side = nullptr;
        if (in_join.fromLeft())
            padded_side = join_padded_sides[0];
        else if (in_join.fromRight())
            padded_side = join_padded_sides[1];

        if (padded_side && built.dag.nearest_padded_side.emplace(pre_join, padded_side).second)
            gated_here.insert(pre_join);
    }

    std::erase_if(built.ungated_nodes, [&](const auto * ungated) { return gated_here.contains(ungated); });
}

/// Records the residual filter of the join as filters and its keys as join conditions.
void collectJoinConditions(Built & built, const JoinOperator & join_operator, const ActionsDAG::NodeMapping & clone_mapping)
{
    const std::array<std::pair<const std::vector<JoinActionRef> *, ActionsDAG::NodeRawConstPtrs *>, 2> conditions_and_targets{{
        {&join_operator.residual_filter, &built.dag.filter_nodes},
        {&join_operator.expression, &built.dag.join_condition_nodes},
    }};

    for (const auto & [conditions, target] : conditions_and_targets)
        for (const auto & condition : *conditions)
            target->push_back(cloneOf(clone_mapping, condition.getNode()));
}

/// Builds a step for which `canEnterStep` holds on top of what its children built, in their order. Returns nullopt
/// when the step cannot be represented, which is not a failure: the caller stops the walk there and the step becomes
/// a source.
std::optional<Built> tryBuildFromStep(QueryPlan::Node & node, std::vector<Built> & children)
{
    auto * step = node.step.get();

    /// The DAG built for the step has to reproduce its header, or the walk stops there.
    const auto finish = [&](Built & built, const ActionsDAG::NodeRawConstPtrs & outputs) -> std::optional<Built>
    {
        if (!outputsMatchHeader(outputs, *step->getOutputHeader()))
            return {};

        built.dag.step_outputs.emplace(&node, outputs);
        return std::move(built);
    };

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

        /// Nothing below this step gates what it computes: a join further down gated the values this
        /// reads, and their answers already stand where that join left them.
        for (auto it = firstNodeAdded(dag, last_before); it != dag.getNodes().end(); ++it)
            built.ungated_nodes.push_back(&*it);

        return true;
    };

    if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
    {
        const auto & step_dag = expression_step->getExpression();
        auto built = std::move(children.front());

        ActionsDAG::NodeMapping clone_mapping;
        if (!mergeAndTrack(built, step_dag, clone_mapping))
            return {};

        return finish(built, built.dag.getOutputs());
    }

    if (auto * runtime_filter_step = typeid_cast<BuildRuntimeFilterStep *>(step))
    {
        auto built = std::move(children.front());

        /// The step outputs what it reads, so the outputs stand as they are. The key name occurs once in the header,
        /// which `canEnterStep` checked.
        const auto outputs = built.dag.getOutputs();
        const auto & key_name = runtime_filter_step->getFilterColumnName();
        const auto key_it = std::ranges::find_if(outputs, [&](const auto * output) { return output->result_name == key_name; });
        if (key_it == outputs.end())
            return {};

        built.dag.step_read_nodes.push_back(*key_it);
        return finish(built, outputs);
    }

    if (auto * filter_step = typeid_cast<FilterStep *>(step))
    {
        const auto & step_dag = filter_step->getExpression();
        auto built = std::move(children.front());

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

        return finish(built, built.dag.getOutputs());
    }

    auto * join_step = typeid_cast<JoinStepLogical *>(step);
    const auto & step_dag = join_step->getActionsDAG();
    const auto & join_operator = join_step->getJoinOperator();

    auto built = std::move(children.front());
    auto & right = children.back();

    uniteRightSide(built, right);

    /// This join sits above everything either side computed, and above everything the joins below them
    /// sit above, so its own mark is the only one the result needs.
    auto & dag = *built.dag.expression_actions.getActionsDAG();
    built.last_node_below_top_join = lastNodeBefore(dag);

    const auto join_padded_sides = gatePaddedSides(built, right, node, join_operator.kind);

    ActionsDAG::NodeMapping clone_mapping;
    if (!mergeAndTrack(built, step_dag, clone_mapping))
        return {};

    trackPreJoinNodes(built, *join_step, clone_mapping, join_padded_sides);

    /// A join outputs what its DAG outputs and drops the rest, while the merge kept the columns of both
    /// sides that the join does not read. Those come after in the output list.
    auto & dag_outputs = dag.getOutputs();
    if (dag_outputs.size() < step_dag.getOutputs().size())
        return {};
    dag_outputs.resize(step_dag.getOutputs().size());

    collectJoinConditions(built, join_operator, clone_mapping);

    return finish(built, built.dag.getOutputs());
}

Built buildFromSource(QueryPlan::Node & node)
{
    Built built;
    built.dag = startDAGFromSource(node);
    built.ungated_nodes = built.dag.sources.front().inputs;
    return built;
}

}

MergedPlanDAG buildMergedPlanDAG(QueryPlan::Node & root)
{
    /// Bottom-up, with the subtrees built so far on a stack: a step is built once its children are, from their results.
    /// A step that cannot be represented - because of what it computes, or because the DAG built for it does not
    /// reproduce its header, which would mean optimizing on a wrong model of the plan - is where the walk stops, with
    /// that step standing in as a source, and what its children built is thrown away.
    struct Frame
    {
        QueryPlan::Node * node;
        bool children_built = false;
    };

    std::vector<Frame> stack{{&root}};
    std::vector<Built> results;
    while (!stack.empty())
    {
        auto & frame = stack.back();
        auto * node = frame.node;

        if (!frame.children_built)
        {
            if (!canEnterStep(*node))
            {
                results.push_back(buildFromSource(*node));
                stack.pop_back();
                continue;
            }

            /// The children are built left to right, so their results stand on the stack in their order.
            frame.children_built = true;
            for (auto * child : node->children | std::views::reverse)
                stack.push_back({child});
            continue;
        }

        stack.pop_back();

        std::vector<Built> children;
        const auto first_child = results.end() - node->children.size();
        children.insert(children.end(), std::make_move_iterator(first_child), std::make_move_iterator(results.end()));
        results.erase(first_child, results.end());

        if (auto built = tryBuildFromStep(*node, children))
            results.push_back(std::move(*built));
        else
            results.push_back(buildFromSource(*node));
    }

    auto built = std::move(results.front());

    /// Filled once, from the outermost join: everything created before it is what some join sits above,
    /// and so is what a join's own DAG computes before that join.
    if (built.last_node_below_top_join)
    {
        const auto & nodes = built.dag.getDAG().getNodes();
        for (auto it = nodes.begin(); it != std::next(*built.last_node_below_top_join); ++it)
            built.dag.nodes_with_join_above.insert(&*it);
    }
    built.dag.nodes_with_join_above.insert(built.pre_join_nodes.begin(), built.pre_join_nodes.end());

    return std::move(built.dag);
}

}
