#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::unique_ptr<ActionsDAG>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Stores the list of columns required to compute a node in the DAG.
struct NodeInfo
{
    NameSet required_columns;
};

/// Fills the list of required columns for a node in the DAG.
void fillRequiredColumns(const ActionsDAG::Node * node, std::unordered_map<const ActionsDAG::Node *, NodeInfo> & nodes_info)
{
    if (nodes_info.contains(node))
        return;

    auto & node_info = nodes_info[node];

    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        node_info.required_columns.insert(node->result_name);
        return;
    }

    for (const auto & child : node->children)
    {
        fillRequiredColumns(child, nodes_info);
        const auto & child_info = nodes_info[child];
        node_info.required_columns.insert(child_info.required_columns.begin(), child_info.required_columns.end());
    }
}

/// Stores information about a node that has already been cloned or added to one of the new DAGs.
/// This allows to avoid cloning the same sub-DAG into multiple step DAGs but reference previously cloned nodes from earlier steps.
struct DAGNodeRef
{
    ActionsDAG * dag;
    const ActionsDAG::Node * node;
};

/// ResultNode -> DAGNodeRef
using OriginalToNewNodeMap = std::unordered_map<const ActionsDAG::Node *, DAGNodeRef>;
using NodeNameToLastUsedStepMap = std::unordered_map<const ActionsDAG::Node *, size_t>;

/// Clones the part of original DAG responsible for computing the original_dag_node and adds it to the new DAG.
const ActionsDAG::Node & addClonedDAGToDAG(
    size_t step,
    const ActionsDAG::Node * original_dag_node,
    const ActionsDAGPtr & new_dag,
    OriginalToNewNodeMap & node_remap,
    NodeNameToLastUsedStepMap & node_to_step_map)
{
    /// Look for the node in the map of already known nodes
    if (node_remap.contains(original_dag_node))
    {
        /// If the node is already in the new DAG, return it
        const auto & node_ref = node_remap.at(original_dag_node);
        if (node_ref.dag == new_dag.get())
            return *node_ref.node;

        /// If the node is known from the previous steps, add it as an input, except for constants
        if (original_dag_node->type != ActionsDAG::ActionType::COLUMN)
        {
            /// If the node was found in node_remap, it was not added to outputs yet.
            /// The only exception is the filter node, which is always the first one.
            if (node_ref.dag->getOutputs().at(0) != node_ref.node)
                node_ref.dag->getOutputs().push_back(node_ref.node);

            const auto & new_node = new_dag->addInput(node_ref.node->result_name, node_ref.node->result_type);
            node_remap[original_dag_node] = {new_dag.get(), &new_node};

            /// Remember the index of the last step which reuses this node.
            /// We cannot remove this node from the outputs before that step.
            node_to_step_map[original_dag_node] = step;
            return new_node;
        }
    }

    /// If the node is an input, add it as an input
    if (original_dag_node->type == ActionsDAG::ActionType::INPUT)
    {
        const auto & new_node = new_dag->addInput(original_dag_node->result_name, original_dag_node->result_type);
        node_remap[original_dag_node] = {new_dag.get(), &new_node};
        return new_node;
    }

    /// If the node is a column, add it as an input
    if (original_dag_node->type == ActionsDAG::ActionType::COLUMN)
    {
        const auto & new_node = new_dag->addColumn(
            ColumnWithTypeAndName(original_dag_node->column, original_dag_node->result_type, original_dag_node->result_name));
        node_remap[original_dag_node] = {new_dag.get(), &new_node};
        return new_node;
    }

    if (original_dag_node->type == ActionsDAG::ActionType::ALIAS)
    {
        const auto & alias_child = addClonedDAGToDAG(step, original_dag_node->children[0], new_dag, node_remap, node_to_step_map);
        const auto & new_node = new_dag->addAlias(alias_child, original_dag_node->result_name);
        node_remap[original_dag_node] = {new_dag.get(), &new_node};
        return new_node;
    }

    /// If the node is a function, add it as a function and add its children
    if (original_dag_node->type == ActionsDAG::ActionType::FUNCTION)
    {
        ActionsDAG::NodeRawConstPtrs new_children;
        for (const auto & child : original_dag_node->children)
        {
            const auto & new_child = addClonedDAGToDAG(step, child, new_dag, node_remap, node_to_step_map);
            new_children.push_back(&new_child);
        }

        const auto & new_node = new_dag->addFunction(original_dag_node->function_base, new_children, original_dag_node->result_name);
        node_remap[original_dag_node] = {new_dag.get(), &new_node};
        return new_node;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node type in PREWHERE actions: {}", original_dag_node->type);
}

const ActionsDAG::Node & addFunction(
        const ActionsDAGPtr & new_dag,
        const FunctionOverloadResolverPtr & function,
        ActionsDAG::NodeRawConstPtrs children)
{
    const auto & new_node = new_dag->addFunction(function, children, "");
    return new_node;
}

/// Adds a CAST node with the regular name ("CAST(...)") or with the provided name.
/// This is different from ActionsDAG::addCast() because it set the name equal to the original name effectively hiding the value before cast,
/// but it might be required for further steps with its original uncast type.
const ActionsDAG::Node & addCast(
        const ActionsDAGPtr & dag,
        const ActionsDAG::Node & node_to_cast,
        const DataTypePtr & to_type)
{
    if (node_to_cast.result_type->equals(*to_type))
        return node_to_cast;  /// NOLINT(bugprone-return-const-ref-from-parameter)

    const auto & new_node = dag->addCast(node_to_cast, to_type, {}, nullptr);
    return new_node;
}

/// Normalizes the filter node by adding AND with a constant true.
/// This:
/// 1. produces a result with the proper Nullable or non-Nullable UInt8 type and
/// 2. makes sure that the result contains only 0 or 1 values even if the source column contains non-boolean values.
const ActionsDAG::Node & addAndTrue(
    const ActionsDAGPtr & dag,
    const ActionsDAG::Node & filter_node_to_normalize)
{
    Field const_true_value(true);

    ColumnWithTypeAndName const_true_column;
    const_true_column.column = DataTypeUInt8().createColumnConst(0, const_true_value);
    const_true_column.type = std::make_shared<DataTypeUInt8>();

    const auto * const_true_node = &dag->addColumn(std::move(const_true_column));
    ActionsDAG::NodeRawConstPtrs children = {&filter_node_to_normalize, const_true_node};
    FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    return addFunction(dag, func_builder_and, children);
}

}

/// We want to build a sequence of steps that will compute parts of the prewhere condition.
/// Each step reads some new columns and computes some new expressions and a filter condition.
/// The last step computes the final filter condition and the remaining expressions that are required for the main query.
/// The goal of this is to, when it is possible, filter out many rows in early steps so that the remaining steps will
/// read less data from the storage.
/// NOTE: The result of executing the steps is exactly the same as if we would execute the original DAG in single step.
///
/// The steps are built in the following way:
/// 1. List all condition nodes that are combined with AND into PREWHERE condition
/// 2. Collect the set of columns that are used in each condition
/// 3. Sort condition nodes by the number of columns used in them and the overall size of those columns
/// 4. Group conditions with the same set of columns into a single read/compute step
/// 5. Build DAGs for each step:
///    - DFS from the condition root node:
///      - If the node was not computed yet, add it to the DAG and traverse its children
///      - If the node was already computed by one of the previous steps, add it as output for that step and as input for the current step
///      - If the node was already computed by the current step just stop traversing
/// 6. Find all outputs of the original DAG
/// 7. Find all outputs that were computed in the already built DAGs, mark these nodes as outputs in the steps where they were computed
/// 8. Add computation of the remaining outputs to the last step with the procedure similar to 4
bool tryBuildPrewhereSteps(
    PrewhereInfoPtr prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    PrewhereExprInfo & prewhere,
    bool force_short_circuit_execution)
{
    if (!prewhere_info)
        return true;

    /// 1. List all condition nodes that are combined with AND into PREWHERE condition
    const auto & condition_root = prewhere_info->prewhere_actions.findInOutputs(prewhere_info->prewhere_column_name);
    const bool is_conjunction = (condition_root.type == ActionsDAG::ActionType::FUNCTION && condition_root.function_base->getName() == "and");
    if (!is_conjunction)
        return false;
    auto condition_nodes = condition_root.children;

    /// 2. Collect the set of columns that are used in the condition
    std::unordered_map<const ActionsDAG::Node *, NodeInfo> nodes_info;
    for (const auto & node : condition_nodes)
    {
        fillRequiredColumns(node, nodes_info);
    }

    /// 3. Sort condition nodes by the number of columns used in them and the overall size of those columns
    /// TODO: not sorting for now because the conditions are already sorted by Where Optimizer

    /// 4. Group conditions with the same set of columns into a single read/compute step
    std::vector<std::vector<const ActionsDAG::Node *>> condition_groups;
    for (const auto & node : condition_nodes)
    {
        const auto & node_info = nodes_info[node];
        if (!condition_groups.empty() && nodes_info[condition_groups.back().back()].required_columns == node_info.required_columns)
            condition_groups.back().push_back(node);    /// Add to the last group
        else
            condition_groups.push_back({node}); /// Start new group
    }

    /// 5. Build DAGs for each step
    struct Step
    {
        ActionsDAGPtr actions;
        /// Original condition, in case if we have only one condition, and it was not cast
        const ActionsDAG::Node * original_node;
        /// Result condition node
        const ActionsDAG::Node * result_node;
    };
    std::vector<Step> steps;

    OriginalToNewNodeMap node_remap;
    NodeNameToLastUsedStepMap node_to_step;

    for (size_t step_index = 0; step_index < condition_groups.size(); ++step_index)
    {
        const auto & condition_group = condition_groups[step_index];
        ActionsDAGPtr step_dag = std::make_unique<ActionsDAG>();
        const ActionsDAG::Node * original_node = nullptr;
        const ActionsDAG::Node * result_node;

        std::vector<const ActionsDAG::Node *> new_condition_nodes;
        for (const auto * node : condition_group)
        {
            const auto & node_in_new_dag = addClonedDAGToDAG(step_index, node, step_dag, node_remap, node_to_step);
            new_condition_nodes.push_back(&node_in_new_dag);
        }

        if (new_condition_nodes.size() > 1)
        {
            /// Add AND function to combine the conditions
            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
            const auto & and_function_node = addFunction(step_dag, func_builder_and, new_condition_nodes);
            result_node = &and_function_node;
        }
        else
        {
            result_node = new_condition_nodes.front();
        }

        step_dag->getOutputs().insert(step_dag->getOutputs().begin(), result_node);
        steps.push_back({std::move(step_dag), original_node, result_node});
    }

    /// 6. Find all outputs of the original DAG
    auto original_outputs = prewhere_info->prewhere_actions.getOutputs();
    steps.back().actions->getOutputs().clear();
    /// 7. Find all outputs that were computed in the already built DAGs, mark these nodes as outputs in the steps where they were computed
    /// 8. Add computation of the remaining outputs to the last step with the procedure similar to 4
    std::unordered_set<const ActionsDAG::Node *> all_outputs;
    for (const auto * output : original_outputs)
    {
        all_outputs.insert(output);
        if (node_remap.contains(output))
        {
            const auto & new_node_info = node_remap[output];
            auto & new_outputs = new_node_info.dag->getOutputs();
            // If not `remove_prewhere_column` then column present in all_outputs, but it's already in the outputs
            if (std::ranges::find(new_outputs, new_node_info.node) == new_outputs.end())
                new_outputs.push_back(new_node_info.node);
        }
        else if (output->result_name == prewhere_info->prewhere_column_name)
        {
            /// Special case for final PREWHERE column: it is an AND combination of all conditions,
            /// but we have only the condition for the last step here. We know that the combined filter is equivalent to
            /// to the last condition after filters from previous steps are applied. We just need to CAST the last condition
            /// to the type of combined filter. We do this in 2 steps:
            /// 1. AND the last condition with constant True. This is needed to make sure that in the last step filter has UInt8 type
            ///    but contains values other than 0 and 1 (e.g. if it is (number%5) it contains 2,3,4)
            /// 2. CAST the result to the exact type of the PREWHERE column from the original DAG
            auto & last_step_dag = steps.back().actions;
            auto & last_step_result_node = steps.back().result_node;
            /// Build AND(last_step_result_node, true)
            const auto & and_node = addAndTrue(last_step_dag, *last_step_result_node);
            /// Build CAST(and_node, type of PREWHERE column)
            const auto & cast_node = addCast(last_step_dag, and_node, output->result_type);
            /// Add alias for the result with the name of the PREWHERE column
            const auto & prewhere_result_node = last_step_dag->addAlias(cast_node, output->result_name);
            last_step_dag->getOutputs().push_back(&prewhere_result_node);
            steps.back().result_node = &prewhere_result_node;
        }
        else
        {
            const auto & node_in_new_dag = addClonedDAGToDAG(steps.size() - 1, output, steps.back().actions, node_remap, node_to_step);
            steps.back().actions->getOutputs().push_back(&node_in_new_dag);
        }
    }

    /// 9. Build PrewhereExprInfo
    {
        for (size_t step_index = 0; step_index < steps.size(); ++step_index)
        {
            auto & step = steps[step_index];
            PrewhereExprStep new_step
            {
                .type = PrewhereExprStep::Filter,
                .actions = std::make_shared<ExpressionActions>(std::move(*step.actions), actions_settings),
                .filter_column_name = step.result_node->result_name,
                /// Don't remove if it's in the list of original outputs
                .remove_filter_column =
                    step.original_node && !all_outputs.contains(step.original_node) && node_to_step[step.original_node] <= step_index,
                .need_filter = force_short_circuit_execution,
                .perform_alter_conversions = true,
                .mutation_version = std::nullopt,
            };

            prewhere.steps.push_back(std::make_shared<PrewhereExprStep>(std::move(new_step)));
        }

        prewhere.steps.back()->remove_filter_column = prewhere_info->remove_prewhere_column;
        prewhere.steps.back()->need_filter = prewhere_info->need_filter;
    }

    return true;
}

}
