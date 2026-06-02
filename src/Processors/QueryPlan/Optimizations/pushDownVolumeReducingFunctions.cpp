#include <Core/Block.h>
#include <Core/Names.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <ranges>
#include <stack>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace DB::QueryPlanOptimizations
{
namespace
{

/// Bail if the parent DAG has duplicate names among its inputs or outputs: our
/// rewrite adds inputs and resolves columns by name, which is ambiguous when
/// names collide.
bool hasDuplicatedNamesInInputOrOutputs(const ActionsDAG & actions)
{
    std::unordered_set<std::string_view> seen;
    for (const auto * input : actions.getInputs())
        if (!seen.insert(input->result_name).second)
            return true;

    seen.clear();
    for (const auto * output : actions.getOutputs())
        if (!seen.insert(output->result_name).second)
            return true;

    return false;
}

bool isSupportedArgumentType(const String & function_name, const DataTypePtr & type)
{
    if (function_name == "lengthUTF8")
        return isStringOrFixedString(type);

    if (function_name == "length" || function_name == "empty" || function_name == "notEmpty")
        return isStringOrFixedString(type) || isArray(type) || isMap(type);

    return false;
}

/// True iff the function's argument sub-DAG only references INPUTs / COLUMNs / ALIASes.
/// A nested FUNCTION or ARRAY_JOIN under the candidate would mean we either
/// duplicate work (computing it twice) or smuggle disallowed semantics below
/// the child step.
bool hasOnlyInputAndConstantChildren(const ActionsDAG::Node & root)
{
    std::stack<const ActionsDAG::Node *> nodes;
    for (const auto * child : root.children)
        nodes.push(child);

    while (!nodes.empty())
    {
        const auto * node = nodes.top();
        nodes.pop();

        if (node->type == ActionsDAG::ActionType::FUNCTION || node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;

        for (const auto * child : node->children)
            nodes.push(child);
    }

    return true;
}

/// Reject functions whose subtree references a PLACEHOLDER — those refer to
/// columns supplied externally (e.g. correlated subquery), not present in
/// any header we can see.
bool subtreeHasNoPlaceholder(const ActionsDAG::Node & root)
{
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(&root);

    while (!nodes.empty())
    {
        const auto * node = nodes.top();
        nodes.pop();

        if (node->type == ActionsDAG::ActionType::PLACEHOLDER)
            return false;

        for (const auto * child : node->children)
            nodes.push(child);
    }

    return true;
}

bool isVolumeReducingCandidate(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || !node.function_base->isVolumeReducing())
        return false;

    if (!node.function_base->isDeterministic() || !node.function_base->isDeterministicInScopeOfQuery())
        return false;

    if (node.children.size() != 1)
        return false;

    if (!isSupportedArgumentType(node.function_base->getName(), node.children.front()->result_type))
        return false;

    if (!hasOnlyInputAndConstantChildren(node))
        return false;

    if (!subtreeHasNoPlaceholder(node))
        return false;

    return true;
}

bool isSupportedChild(const QueryPlanStepPtr & child)
{
    if (typeid_cast<const ExpressionStep *>(child.get()))
        return true;

    if (typeid_cast<const FilterStep *>(child.get()))
        return true;

    if (const auto * sorting = typeid_cast<const SortingStep *>(child.get()))
        return !sorting->hasPartitions();

    if (const auto * limit = typeid_cast<const LimitStep *>(child.get()))
        return !limit->withTies();

    /// DISTINCT can change multiplicity if a column is dropped too early.
    if (typeid_cast<const DistinctStep *>(child.get()))
        return false;

    return false;
}

/// Returns the child step's effective ActionsDAG, or `nullptr` for steps
/// (Sort / Limit) that own no DAG.
const ActionsDAG * getChildActionsDag(const QueryPlanStepPtr & child)
{
    if (const auto * expression = typeid_cast<const ExpressionStep *>(child.get()))
        return &expression->getExpression();
    if (const auto * filter = typeid_cast<const FilterStep *>(child.get()))
        return &filter->getExpression();
    return nullptr;
}

/// Walk up the ALIAS chain from a child output node back to its underlying
/// INPUT in the same DAG. Returns the INPUT node, or nullptr if the chain
/// is interrupted by a FUNCTION / COLUMN / ARRAY_JOIN / PLACEHOLDER (i.e.
/// the column is *computed* by the child, not just renamed).
const ActionsDAG::Node * resolveThroughAliases(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS)
    {
        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    if (!node || node->type != ActionsDAG::ActionType::INPUT)
        return nullptr;
    return node;
}

/// For each candidate FUNCTION node F in the parent, find the corresponding
/// child input column name by following the ALIAS chain inside `child_dag`.
/// Returns `nullopt` if any candidate cannot be resolved (because the column
/// is computed inside the child, not just renamed) — that is the bail
/// signal for the caller.
///
/// When the child has no DAG (Sort / Limit), the parent's INPUT names equal
/// the child's input header names verbatim — no ALIAS chain to walk.
std::optional<std::unordered_map<const ActionsDAG::Node *, String>>
mapCandidatesToChildInputs(
    const std::unordered_set<const ActionsDAG::Node *> & candidates,
    const ActionsDAG * child_dag,
    const Block & child_input_header)
{
    std::unordered_map<const ActionsDAG::Node *, String> result;

    for (const auto * candidate : candidates)
    {
        const auto * arg = candidate->children.front();
        if (arg->type != ActionsDAG::ActionType::INPUT)
            return std::nullopt;

        const String & parent_input_name = arg->result_name;

        if (!child_dag)
        {
            /// No DAG to walk — parent's input name must already exist in
            /// the child's input header.
            if (!child_input_header.has(parent_input_name))
                return std::nullopt;
            result.emplace(candidate, parent_input_name);
            continue;
        }

        const auto * child_output = child_dag->tryFindInOutputs(parent_input_name);
        if (!child_output)
            return std::nullopt;

        const auto * child_input = resolveThroughAliases(child_output);
        if (!child_input)
            return std::nullopt;

        if (!child_input_header.has(child_input->result_name))
            return std::nullopt;

        result.emplace(candidate, child_input->result_name);
    }

    return result;
}

/// One volume-reducing function to recompute below the child step. Captured
/// from a candidate FUNCTION node *before* the parent rewrite, because
/// `removeUnusedActions` may delete the candidate node and invalidate the
/// pointer.
struct PushedFunction
{
    FunctionBasePtr function_base;
    String result_name;
    String arg_child_input_name;
};

/// Build the new pushed-down DAG. Inputs are the child step's full input
/// header (so the recomputed functions can read their argument); outputs are
/// the passthrough columns (minus `columns_to_drop`) plus one new FUNCTION
/// node per pushed function, named exactly like the original candidate so the
/// child step's column-passthrough machinery surfaces them at the parent's
/// expected INPUT name.
///
/// `columns_to_drop` lets the caller stop passing through a wide source column
/// that is no longer needed above the child step. This is what makes the
/// optimization pay off for pure-passthrough children (`Sort` / `Limit`),
/// which carry every input column through and cannot be pruned afterwards by
/// `removeUnusedColumns`.
ActionsDAG buildPushedDag(
    const std::vector<PushedFunction> & pushed_functions,
    const Block & child_input_header,
    const NameSet & columns_to_drop)
{
    ActionsDAG dag;
    std::unordered_map<String, const ActionsDAG::Node *> input_by_name;
    ActionsDAG::NodeRawConstPtrs outputs;
    outputs.reserve(child_input_header.columns() + pushed_functions.size());

    for (const auto & column : child_input_header)
    {
        const auto & input = dag.addInput(column);
        input_by_name.emplace(column.name, &input);
        if (!columns_to_drop.contains(column.name))
            outputs.push_back(&input);
    }

    for (const auto & pushed_function : pushed_functions)
    {
        auto it = input_by_name.find(pushed_function.arg_child_input_name);
        chassert(it != input_by_name.end());
        const auto & fn_node = dag.addFunction(pushed_function.function_base, {it->second}, pushed_function.result_name);
        outputs.push_back(&fn_node);
    }

    dag.getOutputs() = std::move(outputs);
    return dag;
}

/// Columns the child step itself must keep reading regardless of what the
/// parent needs. For a `SortingStep` these are the sort-key columns. A
/// `LimitStep` needs no column (`WITH TIES`, which would, is excluded by
/// `isSupportedChild`). Other child types are not pruned at construction time
/// (their `removeUnusedColumns` handles it), so they report an empty set.
NameSet childRequiredPassthroughColumns(const QueryPlanStepPtr & child)
{
    NameSet names;
    if (const auto * sorting = typeid_cast<const SortingStep *>(child.get()))
        for (const auto & column : sorting->getSortDescription())
            names.insert(column.column_name);
    return names;
}

/// Rewrite the parent's ActionsDAG so that each candidate FUNCTION is
/// replaced by an INPUT of the same name and type. The candidate must
/// appear directly in the parent's outputs; nested candidates (e.g. inside
/// `lengthUTF8(s) + 1` where `length(s)` is the candidate) are not
/// supported by this rewrite path and the caller must bail before getting
/// here.
///
/// Returns false if any candidate is not in the parent's outputs — the
/// caller should treat it as a bail-out.
bool rewriteParentToConsumePushed(
    ActionsDAG & parent_actions,
    const std::unordered_set<const ActionsDAG::Node *> & candidates)
{
    auto & outputs = parent_actions.getOutputs();
    std::unordered_set<const ActionsDAG::Node *> remaining = candidates;

    for (auto *& output : outputs)
    {
        auto it = remaining.find(output);
        if (it == remaining.end())
            continue;

        const auto * candidate = *it;
        const auto & new_input = parent_actions.addInput(candidate->result_name, candidate->result_type);
        output = &new_input;
        remaining.erase(it);
    }

    if (!remaining.empty())
        return false;

    /// Remove inputs that became dead after replacing the candidate FUNCTIONs
    /// (e.g. the original wide `s` input once `lengthUTF8(s)` is an INPUT).
    /// This keeps the parent's surviving INPUT set in sync with the narrowed
    /// child output header that the pushed step may now produce. A wide source
    /// column that is still referenced elsewhere (e.g. `SELECT s, length(s)`)
    /// stays, because it is reachable from another output.
    parent_actions.removeUnusedActions(/*allow_remove_inputs=*/true, /*allow_constant_folding=*/false);
    return true;
}

}

/// Pushes volume-reducing functions (`length`, `empty`, ...) from an
/// ExpressionStep down before its child step, so the child step processes a
/// fixed-size scalar passthrough instead of the original String / Array /
/// Map column.
///
///   Expression                   Expression  [INPUT lengthUTF8(...)]
///     └── Child                    └── Child  [passthrough adds new col]
///           └── X        →               └── Expression  [pushed: lengthUTF8(s)]
///                                              └── X
///
/// Implementation: walk the child step's ALIAS chain to map the candidate's
/// parent-side input name (`__table1.s`) back to the child's own input name
/// (`s`); build a new pushed ExpressionStep below the child whose outputs are
/// (i) the child's input columns, plus (ii) one recomputed candidate FUNCTION
/// per candidate, named identically to the parent's original FUNCTION node.
/// The child's passthrough machinery surfaces the new column up to the parent,
/// where the candidate FUNCTION node is rewritten to a same-named INPUT —
/// eliminating the duplicate computation.
///
/// For pure-passthrough children (`Sort` / `Limit`) the pushed step also stops
/// passing through the wide source column once it is no longer needed above the
/// child, so the wide `String` / `Array` / `Map` value never enters the step.
/// `removeUnusedColumns` cannot achieve this on its own because it does not
/// propagate column pruning through a passthrough step to its child.
size_t tryPushDownVolumeReducingFunction(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (parent_node->children.size() != 1)
        return 0;

    auto * parent_expression = typeid_cast<ExpressionStep *>(parent_node->step.get());
    if (!parent_expression)
        return 0;

    auto & parent_actions = parent_expression->getExpression();
    if (parent_actions.hasArrayJoin() || parent_actions.hasStatefulFunctions() || parent_actions.hasNonDeterministic()
        || hasDuplicatedNamesInInputOrOutputs(parent_actions))
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    if (child_node->children.size() != 1 || !isSupportedChild(child_node->step))
        return 0;

    /// Collect candidate FUNCTION nodes; bail if none.
    std::unordered_set<const ActionsDAG::Node *> candidates;
    for (const auto & node : parent_actions.getNodes())
        if (isVolumeReducingCandidate(node))
            candidates.insert(&node);
    if (candidates.empty())
        return 0;

    /// Every candidate must appear directly in parent's outputs — nested
    /// uses (e.g. `length(s) + 1`) are not handled by this rewrite.
    {
        std::unordered_set<const ActionsDAG::Node *> output_set(
            parent_actions.getOutputs().begin(), parent_actions.getOutputs().end());
        for (const auto * candidate : candidates)
            if (!output_set.contains(candidate))
                return 0;
    }

    /// Map each candidate's parent-side INPUT name to a name in the child
    /// step's input header by walking the ALIAS chain inside the child DAG.
    const auto & child_input_header = *child_node->step->getInputHeaders().front();
    const ActionsDAG * child_dag = getChildActionsDag(child_node->step);
    auto candidate_to_child_input = mapCandidatesToChildInputs(candidates, child_dag, child_input_header);
    if (!candidate_to_child_input)
        return 0;

    /// Capture everything we need to recompute each candidate below the child
    /// *before* the rewrite, because `removeUnusedActions` may delete the
    /// candidate nodes and invalidate the pointers.
    std::vector<PushedFunction> pushed_functions;
    pushed_functions.reserve(candidates.size());
    for (const auto * candidate : candidates)
        pushed_functions.push_back({candidate->function_base, candidate->result_name, candidate_to_child_input->at(candidate)});

    /// Rewrite parent in-place: replace candidate FUNCTION nodes in outputs
    /// with same-named INPUTs and prune dead actions (including the now-dead
    /// wide source inputs).
    if (!rewriteParentToConsumePushed(parent_actions, candidates))
        return 0;

    /// Decide which wide source columns to stop passing through. We only do
    /// this for pure-passthrough children (`Sort` / `Limit`): they carry every
    /// column through and `removeUnusedColumns` cannot prune them afterwards,
    /// so a wide argument column would otherwise stay inside the step. A column
    /// is safe to drop iff the child step does not need it and the rewritten
    /// parent no longer references it (its INPUT survived `removeUnusedActions`
    /// only if it is still used, e.g. `SELECT s, length(s)`).
    NameSet columns_to_drop;
    const bool child_is_passthrough
        = typeid_cast<const SortingStep *>(child_node->step.get()) || typeid_cast<const LimitStep *>(child_node->step.get());
    if (child_is_passthrough)
    {
        const NameSet child_required = childRequiredPassthroughColumns(child_node->step);

        NameSet parent_still_needs;
        for (const auto * input : parent_actions.getInputs())
            parent_still_needs.insert(input->result_name);

        for (const auto & pushed_function : pushed_functions)
        {
            const auto & arg = pushed_function.arg_child_input_name;
            if (!child_required.contains(arg) && !parent_still_needs.contains(arg))
                columns_to_drop.insert(arg);
        }
    }

    /// Build the pushed DAG (passthrough minus dropped columns + new FUNCTION nodes).
    auto pushed = buildPushedDag(pushed_functions, child_input_header, columns_to_drop);

    /// Splice the new pushed step into the plan tree.
    auto & pushed_node = nodes.emplace_back();
    pushed_node.children.swap(child_node->children);
    child_node->children = {&pushed_node};

    pushed_node.step = std::make_unique<ExpressionStep>(pushed_node.children.front()->step->getOutputHeader(), std::move(pushed));
    pushed_node.step->setStepDescription(
        fmt::format("{} [volume-reducing functions]", parent_expression->getStepDescription()), settings.max_step_description_length);

    /// Update the child's input header to reflect the new (wider) header
    /// produced by the pushed step. The parent ExpressionStep's input
    /// header is implicitly the child's output header, which the parent
    /// step recomputes lazily via its own actions.
    child_node->step->updateInputHeader(pushed_node.step->getOutputHeader());

    /// The parent's ActionsDAG now expects `lengthUTF8(__table1.s)` as a
    /// new INPUT (via `addInput` above). Trigger the parent's output-header
    /// recompute to surface that change.
    parent_node->step->updateInputHeader(child_node->step->getOutputHeader());

    return 3;
}

}
