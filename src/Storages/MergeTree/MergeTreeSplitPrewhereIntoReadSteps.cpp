#include <Storages/MergeTree/MergeTreeSplitPrewhereIntoReadSteps.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>


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
    /// The required columns read from the table, that is, not filled by an index reader.
    NameSet required_table_columns;
    /// Names of the indexes whose readers fill the virtual columns required to compute this node.
    /// One reader fills all virtual columns of an index, so they count as one.
    NameSet required_indexes;
    /// True if computing this node may throw an exception or is expensive, so it must not be evaluated
    /// on rows that a preceding condition rejects.
    bool may_throw = false;

    /// True if both nodes read the same columns and indexes, so their conditions may share a step.
    bool readsSameColumns(const NodeInfo & other) const
    {
        return required_table_columns == other.required_table_columns && required_indexes == other.required_indexes;
    }
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

bool isTopKFilter(const ActionsDAG::Node * node)
{
    return node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "__topKFilter";
}

/// Fills the info of an input column. A virtual column of an index is resolved to the name of the index whose reader fills it.
void resolveNodeInfo(NodeInfo & node_info, const String & column_name, const IndexReadTasks * index_read_tasks)
{
    node_info.required_columns.insert(column_name);

    if (index_read_tasks)
    {
        for (const auto & [index_name, index_task] : *index_read_tasks)
        {
            if (std::ranges::any_of(index_task.columns, [&](const auto & column) { return column.name == column_name; }))
            {
                node_info.required_indexes.insert(index_name);
                return;
            }
        }
    }

    node_info.required_table_columns.insert(column_name);
}

/// Fills the list of required columns for a node in the DAG.
void fillRequiredColumns(
    const ActionsDAG::Node * node,
    std::unordered_map<const ActionsDAG::Node *, NodeInfo> & nodes_info,
    const IndexReadTasks * index_read_tasks)
{
    if (nodes_info.contains(node))
        return;

    auto & node_info = nodes_info[node];

    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        resolveNodeInfo(node_info, node->result_name, index_read_tasks);
        return;
    }

    for (const auto & child : node->children)
    {
        fillRequiredColumns(child, nodes_info, index_read_tasks);
        const auto & child_info = nodes_info[child];
        node_info.required_columns.insert(child_info.required_columns.begin(), child_info.required_columns.end());
        node_info.required_table_columns.insert(child_info.required_table_columns.begin(), child_info.required_table_columns.end());
        node_info.required_indexes.insert(child_info.required_indexes.begin(), child_info.required_indexes.end());
        node_info.may_throw = node_info.may_throw || child_info.may_throw;
    }

    /// Reuse the predicate that the short-circuit machinery uses to decide which nodes must be
    /// guarded (findLazyExecutedNodes in ExpressionActions.cpp). IExecutableFunction::canThrow
    /// delegates to it as well, through FunctionToExecutableFunctionAdaptor. It is imprecise in
    /// both directions: it reports true for merely expensive functions, and false for some
    /// functions that do throw while parsing row values (for example addDays(String, ...) via
    /// FunctionDateOrDateTimeAddInterval). Use it to reproduce the existing step boundaries.
    /// Splitting an alias needs the stricter IFunctionBase::canThrow contract below.
    if (!node_info.may_throw && node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
        && (node->function_base->isSuitableForShortCircuitArgumentsExecution(getArgumentTypesWithConstInfo(node->children))
            || node->function_base->isExpensive()))
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

struct ConditionGroup
{
    ActionsDAG::NodeRawConstPtrs nodes;
    bool needs_filtered_input;
    bool ends_cache_prefix = false;
};

using NodesInfo = std::unordered_map<const ActionsDAG::Node *, NodeInfo>;

/// Group adjacent atoms from one conjunction; a later refinement may split an existing
/// group but must never merge its last atom with the next group's first atom.
void appendGroups(
    std::vector<ConditionGroup> & groups,
    const ActionsDAG::NodeRawConstPtrs & nodes,
    const NodesInfo & info,
    bool read_ahead_columns,
    size_t cache_prefix = 0)
{
    for (size_t i = 0; i < nodes.size(); ++i)
    {
        const auto * node = nodes[i];
        const auto & current = info.at(node);
        const bool same_columns = i != 0 && current.readsSameColumns(info.at(groups.back().nodes.front()));
        /// `__topKFilter` is the first condition (see `buildTopKDynamicFilterPrewhere`). In its own step the condition runs only
        /// on the rows the threshold passes once it is selective, because the `__topKFilter` step's filter stays optional.
        const bool follows_top_k_filter = same_columns && read_ahead_columns && isTopKFilter(groups.back().nodes.front())
            && canBeMovedToTheFrontOfAndChain(node);

        if (same_columns && i != cache_prefix && !current.may_throw && !follows_top_k_filter)
            groups.back().nodes.push_back(node);
        else
            groups.push_back({{node}, current.may_throw});
        if (i + 1 == cache_prefix)
            groups.back().ends_cache_prefix = true;
    }
}

/// Walk only conjunctions, in the written order. In particular, a candidate below OR or NOT
/// cannot reject a row on its own. Keep the original DAG for its outputs and cache hashes.
class ConjunctionSplitter
{
public:
    ConjunctionSplitter(const PrewhereInfo & info_, const ExpressionActionsSettings & settings_)
        : info(info_)
        , settings(settings_)
    {
    }

    const ActionsDAG::Node * getConjunction(const ActionsDAG::Node * node, bool through_aliases)
    {
        const auto * function = node->getWithoutAlias();
        if (function->type != ActionsDAG::ActionType::FUNCTION || !function->function_base || function->function_base->getName() != "and")
            return nullptr;
        if (node == function)
            return function;

        if (!through_aliases || !info.need_filter)
            return nullptr;

        if (!can_split_aliases.has_value())
        {
            /// A sibling output may depend on seeing rows before the original filter is applied.
            const auto & dag = info.prewhere_actions;
            can_split_aliases
                = std::ranges::all_of(
                      dag.getNodes(),
                      [](const auto & action)
                      {
                          return allNodeFunctions(
                              action,
                              [](const auto & f)
                              { return f.isDeterministicInScopeOfQuery() && !f.isStateful() && !f.hasObservableSideEffects(); });
                      })
                && std::ranges::all_of(
                      dag.getOutputs(),
                      [&](const auto * output)
                      { return output->result_name == info.prewhere_column_name || canEvaluateOnDifferentRows(output); });
        }
        if (!*can_split_aliases)
            return nullptr;

        if (canEvaluateOnDifferentRows(function) || hasGuardedLastArgument(node, function))
            return function;
        return nullptr;
    }

    void flatten(const ActionsDAG::Node * node, ActionsDAG::NodeRawConstPtrs & atoms, bool through_aliases)
    {
        if (const auto * conjunction = getConjunction(node, through_aliases))
        {
            for (const auto * child : conjunction->children)
                flatten(child, atoms, through_aliases);
        }
        else
            atoms.push_back(node);
    }

    /// Refine the original schedule within its existing filter and cache boundaries.
    /// A group with an unguarded, possibly throwing expression may see rows from before
    /// the accumulated filter is applied. Keep all steps up to that group as they were.
    void splitAliases(std::vector<ConditionGroup> & groups, const NodesInfo & nodes_info, bool read_ahead_columns)
    {
        auto can_expand = [&](const ConditionGroup & group)
        {
            return std::ranges::any_of(
                group.nodes, [&](const auto * node) { return node->type == ActionsDAG::ActionType::ALIAS && getConjunction(node, true); });
        };
        if (std::ranges::none_of(groups, can_expand))
            return;

        std::unordered_set<size_t> to_expand;
        for (size_t i = 0; i < groups.size(); ++i)
        {
            const auto & group = groups[i];
            const bool safe = std::ranges::all_of(group.nodes, [&](const auto * node) { return canEvaluateOnDifferentRows(node); });
            if ((group.nodes.size() == 1 || safe) && can_expand(group))
                to_expand.insert(i);
            else if (!group.needs_filtered_input && !safe)
                to_expand.clear();
        }
        if (to_expand.empty())
            return;

        std::vector<ConditionGroup> refined;
        for (size_t i = 0; i < groups.size(); ++i)
        {
            auto & group = groups[i];
            if (!to_expand.contains(i))
            {
                refined.push_back(std::move(group));
                continue;
            }

            ActionsDAG::NodeRawConstPtrs atoms;
            for (const auto * node : group.nodes)
                flatten(node, atoms, true);
            const size_t first = refined.size();
            appendGroups(refined, atoms, nodes_info, read_ahead_columns);
            refined[first].needs_filtered_input |= group.needs_filtered_input;
            refined.back().ends_cache_prefix = group.ends_cache_prefix;
        }
        groups.swap(refined);
    }

private:
    const PrewhereInfo & info;
    const ExpressionActionsSettings & settings;
    std::optional<bool> can_split_aliases;
    std::unordered_map<const ActionsDAG::Node *, bool> safe_nodes;
    std::unordered_map<const ActionsDAG::Node *, size_t> uses;

    /// Splitting changes the blocks on which an argument runs. In particular, a PREWHERE
    /// filter discards NULL, whereas a short-circuit AND still evaluates later arguments on NULL.
    bool canEvaluateOnDifferentRows(const ActionsDAG::Node * node)
    {
        if (auto it = safe_nodes.find(node); it != safe_nodes.end())
            return it->second;

        bool safe = !WhichDataType(node->result_type).isFunction();
        if (safe && node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & function = *node->function_base;
            safe = function.isDeterministic() && !function.isStateful() && !function.hasObservableSideEffects()
                && (function.getName() == "and" || !function.canThrow(getArgumentTypesWithConstInfo(node->children)));
        }
        else if (
            node->type != ActionsDAG::ActionType::INPUT && node->type != ActionsDAG::ActionType::COLUMN
            && node->type != ActionsDAG::ActionType::ALIAS)
            safe = false;
        safe = safe && std::ranges::all_of(node->children, [&](const auto * child) { return canEvaluateOnDifferentRows(child); });
        safe_nodes.emplace(node, safe);
        return safe;
    }

    bool hasSingleUse(const ActionsDAG::Node * node)
    {
        if (uses.empty())
        {
            for (const auto & action : info.prewhere_actions.getNodes())
                for (const auto * child : action.children)
                    ++uses[child];
            for (const auto * output : info.prewhere_actions.getOutputs())
                ++uses[output];
        }

        while (uses.at(node) == 1 && node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children.front();
        return uses.at(node) == 1;
    }

    /// A private lazy call, and private lazy calls in its arguments, already run on the
    /// guard's mask. A shared call may be eager, and a nested short-circuit function can
    /// impose a different mask on its children. Keep those expressions intact.
    bool canEvaluateOnGuardedRows(const ActionsDAG::Node * node)
    {
        if (canEvaluateOnDifferentRows(node))
            return true;
        const auto * unwrapped = node->getWithoutAlias();
        if (unwrapped->type != ActionsDAG::ActionType::FUNCTION || !hasSingleUse(node))
            return false;
        const auto & function = *unwrapped->function_base;
        IFunctionBase::ShortCircuitSettings short_circuit;
        return function.isDeterministic() && !function.isStateful() && !function.hasObservableSideEffects()
            && !function.isShortCircuit(short_circuit, unwrapped->children.size())
            && function.isSuitableForShortCircuitArgumentsExecution(getArgumentTypesWithConstInfo(unwrapped->children))
            && std::ranges::all_of(unwrapped->children, [&](const auto * child) { return canEvaluateOnGuardedRows(child); });
    }

    /// In and(input..., lazy_predicate), the eager non-null inputs already decide precisely
    /// which rows reach the private last argument. It may throw on those rows.
    bool hasGuardedLastArgument(const ActionsDAG::Node * alias, const ActionsDAG::Node * conjunction)
    {
        if (settings.short_circuit_function_evaluation == ShortCircuitFunctionEvaluation::DISABLE)
            return false;

        const auto & children = conjunction->children;
        for (size_t i = 0; i + 1 < children.size(); ++i)
        {
            const auto * guard = children[i]->getWithoutAlias();
            if (guard->type != ActionsDAG::ActionType::INPUT || canContainNull(*guard->result_type))
                return false;
        }

        return hasSingleUse(alias) && canEvaluateOnGuardedRows(children.back());
    }
};

/// Appends the INPUT nodes of the sub-DAG rooted at `node` to `inputs`, each at most once.
void collectInputNodes(
    const ActionsDAG::Node * node,
    std::unordered_set<const ActionsDAG::Node *> & visited,
    ActionsDAG::NodeRawConstPtrs & inputs)
{
    if (!visited.insert(node).second)
        return;

    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        inputs.push_back(node);
        return;
    }

    for (const auto * child : node->children)
        collectInputNodes(child, visited, inputs);
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

}

bool isDynamicRuntimePrewhereFilter(const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return false;

    const auto & name = node->function_base->getName();
    /// `__topKFilter`: the dynamic threshold filter `optimizeTopK` injects for `ORDER BY ... LIMIT`.
    /// `__applyFilter`: a join runtime filter pushed into the probe-side scan.
    if (name == "__topKFilter" || name == "__applyFilter")
        return true;

    /// An ANTI join wraps its runtime filter as `or(__applyFilter(...), isNull(key))`.
    if (name == "or")
        return std::ranges::any_of(node->children, [](const auto * child) { return isDynamicRuntimePrewhereFilter(child); });

    return false;
}

bool prewhereIsEntirelyQueryPredicate(const ActionsDAG & prewhere_actions, const String & filter_column_name)
{
    ActionsDAG::NodeRawConstPtrs atoms;
    flattenConjunction(&prewhere_actions.findInOutputs(filter_column_name), atoms);
    return std::ranges::none_of(atoms, [](const auto * node) { return isDynamicRuntimePrewhereFilter(node); });
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
    bool read_ahead_columns,
    const IndexReadTasks * index_read_tasks)
{
    if (!prewhere_info)
        return true;

    /// Steps pass columns to each other by name, which cannot tell an input from a computed node
    /// that reuses its name (e.g. a `Buffer` converting its destination's column).
    if (prewhere_info->prewhere_actions.hasInputNameShadowedByComputedNode())
        return false;

    /// 1. List all condition nodes that are combined with AND into PREWHERE condition
    const auto & condition_root = prewhere_info->prewhere_actions.findInOutputs(prewhere_info->prewhere_column_name);
    ConjunctionSplitter splitter(*prewhere_info, actions_settings);
    const auto * conjunction = splitter.getConjunction(&condition_root, true);
    if (!conjunction)
        return false;

    /// The cache hashes identify the original direct children, before any nested AND is split.
    /// A rewritten prefix no longer owns the cache entry. If it still matches, place the boundary
    /// after the last atom of its last child, never inside an expanded conjunct.
    size_t attributed_children = 0;
    if (const auto & attribution = prewhere_info->query_condition_cache_attribution; attribution && conjunction == &condition_root)
    {
        const auto & hashes = attribution->conjunct_hashes;
        bool matches = !hashes.empty() && hashes.size() < conjunction->children.size();
        for (size_t i = 0; matches && i < hashes.size(); ++i)
            matches = conjunction->children[i]->getHash() == hashes[i];
        if (matches)
            attributed_children = hashes.size();
    }

    ActionsDAG::NodeRawConstPtrs condition_nodes;
    size_t num_attributed_conditions = 0;
    if (conjunction != &condition_root)
        condition_nodes.push_back(&condition_root);
    else
    {
        for (size_t i = 0; i < conjunction->children.size(); ++i)
        {
            splitter.flatten(conjunction->children[i], condition_nodes, false);
            if (i + 1 == attributed_children)
                num_attributed_conditions = condition_nodes.size();
        }
    }

    /// 2. Collect the set of columns that are used in the condition
    std::unordered_map<const ActionsDAG::Node *, NodeInfo> nodes_info;
    for (const auto & node : condition_nodes)
    {
        fillRequiredColumns(node, nodes_info, index_read_tasks);
    }

    /// 3. Sort condition nodes by the number of columns used in them and the overall size of those columns
    /// TODO: not sorting for now because the conditions are already sorted by Where Optimizer

    /// 4. Group adjacent conditions that read the same set of columns and indexes into a single step.
    /// A subcolumn is a separate column here, so a condition on a subcolumn and a condition on
    /// the whole column get separate steps, while conditions on virtual columns of one index share one.
    ///
    /// The condition list is the flattened conjunction, so a nested AND contributes its own conditions
    /// rather than a single opaque one. The flattening keeps the original left to right order, for the
    /// same evaluation order reason spelled out below.
    ///
    /// Only adjacent conditions are merged to preserve the user's explicit PREWHERE evaluation order.
    /// Non-adjacent conditions on the same columns are kept in separate steps even though this
    /// may cause redundant reads, because the user may have intentionally interleaved a guard predicate
    /// (e.g. `PREWHERE tags['safe'] != '' AND value > 0 AND toUInt64(tags['unsafe']) > 0` — the
    /// `value > 0` step must filter rows before evaluating the potentially-throwing conversion).
    ///
    /// Adjacency alone is not enough: all conditions of one step are evaluated on the same unfiltered
    /// block, so a condition that may throw must never share a step with a preceding condition.
    /// `MergeTreeWhereOptimizer` groups conditions by the columns they read, which makes a guard and
    /// a throwing predicate over the same column adjacent, so the may_throw check below is what keeps
    /// the guard effective.
    std::vector<ConditionGroup> condition_groups;
    appendGroups(condition_groups, condition_nodes, nodes_info, read_ahead_columns, num_attributed_conditions);
    splitter.splitAliases(condition_groups, nodes_info, read_ahead_columns);

    /// For every group, the index of the group whose step reads its columns. A group that was split
    /// from the previous one only because it may throw reads the same columns, so with
    /// `read_ahead_columns` its columns are read by the first step of that run and only the evaluation
    /// waits for the filter. Otherwise every such step would deserialize the same columns again
    /// (for example a whole `Map` for each of its keys). Reading is safe only when the reader does not
    /// convert the column or fill it from a default expression, because that runs on the whole block
    /// before any filter, so the caller decides (see `MergeTreeReaderSettings::read_ahead_prewhere_columns`).
    std::vector<size_t> group_read_step;
    for (size_t i = 0; i < condition_groups.size(); ++i)
    {
        const bool same_columns
            = i && nodes_info.at(condition_groups[i].nodes.front()).readsSameColumns(nodes_info.at(condition_groups[i - 1].nodes.front()));
        group_read_step.push_back(read_ahead_columns && same_columns ? group_read_step.back() : i);
    }

    /// Inputs of the later groups that are read by each step, see group_read_step.
    std::vector<ActionsDAG::NodeRawConstPtrs> inputs_read_ahead(condition_groups.size());
    {
        std::vector<std::unordered_set<const ActionsDAG::Node *>> visited(condition_groups.size());
        for (size_t group_index = 0; group_index < condition_groups.size(); ++group_index)
        {
            const size_t read_step = group_read_step[group_index];
            if (read_step == group_index)
                continue;
            for (const auto * node : condition_groups[group_index].nodes)
                collectInputNodes(node, visited[read_step], inputs_read_ahead[read_step]);
        }
    }

    /// 5. Build DAGs for each step
    struct Step
    {
        ActionsDAGPtr actions;
        /// Original condition if the step has one condition, null for an AND of several.
        const ActionsDAG::Node * original_node;
        /// Result condition node
        const ActionsDAG::Node * result_node;
    };
    std::vector<Step> steps;

    OriginalToNewNodeMap node_remap;
    NodeNameToLastUsedStepMap node_to_step;

    for (size_t step_index = 0; step_index < condition_groups.size(); ++step_index)
    {
        const auto & condition_group = condition_groups[step_index].nodes;
        ActionsDAGPtr step_dag = std::make_unique<ActionsDAG>();
        const ActionsDAG::Node * original_node = nullptr;
        const ActionsDAG::Node * result_node = nullptr;

        std::vector<const ActionsDAG::Node *> new_condition_nodes;
        for (const auto * node : condition_group)
        {
            const auto & node_in_new_dag = addClonedDAGToDAG(step_index, node, step_dag, node_remap, node_to_step);
            new_condition_nodes.push_back(&node_in_new_dag);
        }

        /// Only added as inputs here: the later step that uses such a column adds it to the outputs of this one.
        for (const auto * input : inputs_read_ahead[step_index])
            addClonedDAGToDAG(step_index, input, step_dag, node_remap, node_to_step);

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
            original_node = condition_group.front();
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
    ActionsDAG::NodeRawConstPtrs outputs_computed_in_last_step;
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
            /// The PREWHERE column is an AND of all conditions, which after the earlier steps have
            /// filtered is equivalent to the last condition alone, so only its type has to match.
            auto & last_step_dag = steps.back().actions;
            auto & last_step_result_node = steps.back().result_node;
            const auto & condition_node = last_step_dag->addBooleanCondition(*last_step_result_node, output->result_type, nullptr);
            /// Add alias for the result with the name of the PREWHERE column
            const auto & prewhere_result_node = last_step_dag->addAlias(condition_node, output->result_name);
            last_step_dag->getOutputs().push_back(&prewhere_result_node);
            steps.back().result_node = &prewhere_result_node;
        }
        else
        {
            const auto & node_in_new_dag = addClonedDAGToDAG(steps.size() - 1, output, steps.back().actions, node_remap, node_to_step);
            steps.back().actions->getOutputs().push_back(&node_in_new_dag);
            outputs_computed_in_last_step.push_back(output);
        }
    }

    /// 9. Build PrewhereExprInfo
    {
        /// Deferring a step filter needs a reader that neither converts nor fills PREWHERE columns, and a filtering last step.
        const bool can_skip_forced_filters = force_short_circuit_execution && read_ahead_columns && prewhere_info->need_filter;

        for (size_t step_index = 0; step_index < steps.size(); ++step_index)
        {
            auto & step = steps[step_index];
            const size_t next = step_index + 1;
            const bool next_step_can_see_rejected_rows = can_skip_forced_filters && next < steps.size()
                && std::ranges::all_of(condition_groups[next].nodes, canBeMovedToTheFrontOfAndChain)
                && (next + 1 < steps.size() || std::ranges::all_of(outputs_computed_in_last_step, canBeMovedToTheFrontOfAndChain));

            /// Count the step toward the PREWHERE-effectiveness events only when every one of its conditions is
            /// a user query predicate. A step that carries any synthesized dynamic filter (`ORDER BY ... LIMIT`
            /// top-K, a join runtime filter) is not attributed to PREWHERE: its conditions are evaluated together
            /// on the same block, so a mixed step's rejections cannot be split between the user predicate and the
            /// dynamic filter, and counting it would attribute the dynamic filter's rejections to PREWHERE.
            const bool step_is_query_prewhere = std::ranges::none_of(
                condition_groups[step_index].nodes, [](const auto * node) { return isDynamicRuntimePrewhereFilter(node); });

            PrewhereExprStep new_step
            {
                .type = PrewhereExprStep::Filter,
                .actions = std::make_shared<ExpressionActions>(std::move(*step.actions), actions_settings),
                .filter_column_name = step.result_node->result_name,
                /// Don't remove a condition that is an original output or that a later step uses.
                /// An AND of several conditions is a node the steps create, nothing else references it.
                /// A stored column used as the condition stays: a later step may read it as the parent of a subcolumn.
                .remove_filter_column = !step.original_node
                    || (step.original_node->type != ActionsDAG::ActionType::INPUT && !all_outputs.contains(step.original_node)
                        && node_to_step[step.original_node] <= step_index),
                /// A step that precedes a may_throw condition must materialize its filter, so that
                /// the throwing condition is not evaluated on the rows this step rejects.
                .need_filter = (next < condition_groups.size() && condition_groups[next].needs_filtered_input)
                    || (force_short_circuit_execution && !next_step_can_see_rejected_rows),
                .perform_alter_conversions = true,
                .columns_overwritten_by_chain = {},
                .mutation_version = std::nullopt,
                .query_condition_cache_attribution_boundary = condition_groups[step_index].ends_cache_prefix,
                .is_query_prewhere = step_is_query_prewhere,
                /// A split step that is not entirely a user predicate carries a synthesized dynamic filter: the
                /// only non-user conjuncts the splitter sees come from the query predicate's dynamic filters.
                .is_synthesized_dynamic_filter = !step_is_query_prewhere,
            };

            prewhere.steps.push_back(std::make_shared<PrewhereExprStep>(std::move(new_step)));
        }

        prewhere.steps.back()->remove_filter_column = prewhere_info->remove_prewhere_column;
        prewhere.steps.back()->need_filter = prewhere_info->need_filter;
    }

    return true;
}

}
