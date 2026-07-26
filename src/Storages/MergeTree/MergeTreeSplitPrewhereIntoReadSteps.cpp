#include <Storages/MergeTree/MergeTreeSplitPrewhereIntoReadSteps.h>

#include <Columns/ColumnConst.h>
#include <Storages/ColumnsDescription.h>


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
    /// Column names resolved to their physical storage names (subcolumn suffix stripped).
    /// Used for grouping: conditions on subcolumns of the same storage column are placed into one step.
    NameSet required_storage_columns;
    /// True if computing this node may throw an exception, so it must not be evaluated on rows
    /// that a preceding condition rejects.
    bool may_throw = false;
};

/// Returns the argument types of a function node, as expected by
/// IFunctionBase::isSuitableForShortCircuitArgumentsExecution.
/// Mirrors getDataTypesWithConstInfoFromNodes in ExpressionActions.cpp, which is file local there.
DataTypesWithConstInfo getArgumentTypesWithConstInfo(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    DataTypesWithConstInfo types;
    types.reserve(nodes.size());
    for (const auto & child : nodes)
        types.push_back({child->result_type, child->column != nullptr});
    return types;
}

/// Resolves a column name to its storage (physical) name.
/// For subcolumns like `map.key_k0`, returns `map`.
/// For regular columns, returns the name unchanged.
String resolveStorageColumnName(const String & column_name, const ColumnsDescription * columns)
{
    if (columns)
    {
        if (auto col = columns->tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name))
            return col->getNameInStorage();
    }
    return column_name;
}

/// Fills the list of required columns for a node in the DAG.
void fillRequiredColumns(
    const ActionsDAG::Node * node,
    std::unordered_map<const ActionsDAG::Node *, NodeInfo> & nodes_info,
    const ColumnsDescription * columns)
{
    if (nodes_info.contains(node))
        return;

    auto & node_info = nodes_info[node];

    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        node_info.required_columns.insert(node->result_name);
        node_info.required_storage_columns.insert(resolveStorageColumnName(node->result_name, columns));
        return;
    }

    for (const auto & child : node->children)
    {
        fillRequiredColumns(child, nodes_info, columns);
        const auto & child_info = nodes_info[child];
        node_info.required_columns.insert(child_info.required_columns.begin(), child_info.required_columns.end());
        node_info.required_storage_columns.insert(child_info.required_storage_columns.begin(), child_info.required_storage_columns.end());
        node_info.may_throw = node_info.may_throw || child_info.may_throw;
    }

    /// Reuse the predicate that the short-circuit machinery uses to decide which nodes must be
    /// guarded (findLazyExecutedNodes in ExpressionActions.cpp). IExecutableFunction::canThrow
    /// delegates to it as well, through FunctionToExecutableFunctionAdaptor. It is imprecise in
    /// both directions: it reports true for merely expensive functions, and false for some
    /// functions that do throw while parsing row values (for example addDays(String, ...) via
    /// FunctionDateOrDateTimeAddInterval). There is no sound can-throw oracle in the tree yet,
    /// see the TODO on canThrow in IFunctionAdaptors.h.
    if (!node_info.may_throw && node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
        && node->function_base->isSuitableForShortCircuitArgumentsExecution(getArgumentTypesWithConstInfo(node->children)))
        node_info.may_throw = true;
}

/// Appends the conditions combined with AND into `atoms`, descending into nested AND nodes.
/// The order of the original conditions is preserved: `and(A, and(B, C))` yields `A, B, C`.
/// ActionsDAG::extractConjunctionAtoms is not reused here because it walks with a stack and thus
/// reverses sibling order, while the step boundaries below and the evaluation order promised for a
/// user written PREWHERE both depend on the original order.
void flattenConjunction(const ActionsDAG::Node * node, ActionsDAG::NodeRawConstPtrs & atoms)
{
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "and")
    {
        for (const auto * child : node->children)
            flattenConjunction(child, atoms);
        return;
    }

    atoms.push_back(node);
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
            original_dag_node->column, original_dag_node->result_type, original_dag_node->result_name);
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

    auto const_true_type = std::make_shared<DataTypeUInt8>();
    auto const_true_column = const_true_type->createColumnConst(0, const_true_value);

    const auto * const_true_node = &dag->addColumn(std::move(const_true_column), std::move(const_true_type), "");
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
    bool force_short_circuit_execution,
    const ColumnsDescription * columns)
{
    if (!prewhere_info)
        return true;

    /// 1. List all condition nodes that are combined with AND into PREWHERE condition
    const auto & condition_root = prewhere_info->prewhere_actions.findInOutputs(prewhere_info->prewhere_column_name);
    const bool is_conjunction = (condition_root.type == ActionsDAG::ActionType::FUNCTION && condition_root.function_base->getName() == "and");
    if (!is_conjunction)
        return false;
    /// Nested conjunctions are flattened, so that a condition list like
    /// `and(existing_prewhere, and(guard, throwing))` (built by optimizePrewhere when a moved WHERE
    /// is merged into an existing PREWHERE) is grouped condition by condition below instead of
    /// treating the inner AND as one atomic condition.
    ActionsDAG::NodeRawConstPtrs condition_nodes;
    flattenConjunction(&condition_root, condition_nodes);

    /// 2. Collect the set of columns that are used in the condition
    std::unordered_map<const ActionsDAG::Node *, NodeInfo> nodes_info;
    for (const auto & node : condition_nodes)
    {
        fillRequiredColumns(node, nodes_info, columns);
    }

    /// 3. Sort condition nodes by the number of columns used in them and the overall size of those columns
    /// TODO: not sorting for now because the conditions are already sorted by Where Optimizer

    /// 4. Group adjacent conditions that read from the same set of physical storage columns into a single step.
    /// Conditions on subcolumns of the same column (e.g. `map.key_k0` and `map.key_k1`) are placed into one group
    /// when they appear next to each other in the condition list.
    ///
    /// The condition list is the flattened conjunction, so a nested AND contributes its own conditions
    /// rather than a single opaque one. The flattening keeps the original left to right order, for the
    /// same evaluation order reason spelled out below.
    ///
    /// Only adjacent conditions are merged to preserve the user's explicit PREWHERE evaluation order.
    /// Non-adjacent conditions on the same storage column are kept in separate steps even though this
    /// may cause redundant reads, because the user may have intentionally interleaved a guard predicate
    /// (e.g. `PREWHERE tags['safe'] != '' AND value > 0 AND toUInt64(tags['unsafe']) > 0` — the
    /// `value > 0` step must filter rows before evaluating the potentially-throwing conversion).
    ///
    /// Adjacency alone is not enough: all conditions of one step are evaluated on the same unfiltered
    /// block, so a condition that may throw must never share a step with a preceding condition.
    /// `MergeTreeWhereOptimizer` groups conditions by their physical storage columns, which makes a
    /// guard and a throwing predicate over the same column adjacent, so the may_throw check below is
    /// what keeps the guard effective.
    std::vector<std::vector<const ActionsDAG::Node *>> condition_groups;
    /// Indices of groups whose first condition may throw. Steps are not required to materialize their
    /// filter, so the preceding step is asked to do it, otherwise the throwing condition is evaluated
    /// on the rows that step rejects. Recorded for every such group regardless of which columns the
    /// two steps read, because a step never filters the block it hands over on its own.
    std::unordered_set<size_t> groups_requiring_filtered_input;
    for (const auto & node : condition_nodes)
    {
        const auto & node_info = nodes_info[node];
        const bool merge_into_previous_group = !condition_groups.empty() && !node_info.may_throw
            && nodes_info[condition_groups.back().front()].required_storage_columns == node_info.required_storage_columns;

        if (merge_into_previous_group)
        {
            condition_groups.back().push_back(node);
            continue;
        }

        if (!condition_groups.empty() && node_info.may_throw)
            groups_requiring_filtered_input.insert(condition_groups.size());

        condition_groups.push_back({node});
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
        const ActionsDAG::Node * result_node = nullptr;

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
                /// A step that precedes a may_throw condition must materialize its filter, so that
                /// the throwing condition is not evaluated on the rows this step rejects.
                .need_filter = force_short_circuit_execution || groups_requiring_filtered_input.contains(step_index + 1),
                .perform_alter_conversions = true,
                .columns_overwritten_by_chain = {},
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
