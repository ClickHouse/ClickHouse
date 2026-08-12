#include <Core/Block.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <fmt/format.h>

#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace DB::QueryPlanOptimizations
{
namespace
{

/// `lengthUTF8` is only defined for `String` / `FixedString`, while `length`, `empty` and
/// `notEmpty` also accept `Array` and `Map`. `empty` / `notEmpty` are additionally defined for
/// fixed-size types (`UUID`, `IPv4`, `IPv6`, `QBit`), but replacing a fixed-size column with
/// another fixed-size column reduces no volume, so those arguments are not accepted.
bool isSupportedArgumentType(const String & function_name, const DataTypePtr & type)
{
    if (function_name == "lengthUTF8")
        return isStringOrFixedString(type);

    if (function_name == "length" || function_name == "empty" || function_name == "notEmpty")
        return isStringOrFixedString(type) || isArray(type) || isMap(type);

    return false;
}

/// Follow a chain of renames back to the column it started from.
const ActionsDAG::Node * resolveAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        node = node->children.front();

    return node;
}

/// The `ActionsDAG` owned by the step, or `nullptr` for steps that own none (`Sorting`, `Limit`).
ActionsDAG * tryGetStepActions(IQueryPlanStep * step)
{
    if (auto * expression = typeid_cast<ExpressionStep *>(step))
        return &expression->getExpression();

    if (auto * filter = typeid_cast<FilterStep *>(step))
        return &filter->getExpression();

    return nullptr;
}

/// Names of the input columns some output of `actions` depends on. An input nothing depends on is
/// tolerated by `ActionsDAG::updateHeader` and by `ExpressionActions::execute` when the column is
/// missing from the block, so it does not keep the column alive.
NameSet collectInputsNeededByOutputs(const ActionsDAG & actions)
{
    NameSet names;
    std::unordered_set<const ActionsDAG::Node *> visited;
    ActionsDAG::NodeRawConstPtrs stack(actions.getOutputs().begin(), actions.getOutputs().end());
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;

        if (node->type == ActionsDAG::ActionType::INPUT)
            names.insert(node->result_name);

        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }

    return names;
}

/// Whether the wide argument column is worth taking out of the step, and which of its input columns
/// the step reads on its own regardless of what the parent needs (those can never stop being
/// produced below the step).
///
/// Only steps that really move the column around are worth pushing below: `Sorting` buffers and
/// merges every column of every row, `Filter` copies the surviving rows of every column. Other 1:1
/// steps gain nothing and would fight the optimizations that move them in the opposite direction
/// (`tryMergeExpressions` merges an `Expression` back with the pushed step, `tryPushDownLimit`
/// moves a `Limit` below it).
bool canPushBelowStep(IQueryPlanStep * step, NameSet & columns_pinned_by_step)
{
    if (const auto * sorting = typeid_cast<const SortingStep *>(step))
    {
        /// `PARTITION BY` reads columns which are not part of the sort description.
        if (sorting->hasPartitions())
            return false;

        for (const auto & column : sorting->getSortDescription())
            columns_pinned_by_step.insert(column.column_name);

        return true;
    }

    if (const auto * filter = typeid_cast<const FilterStep *>(step))
    {
        columns_pinned_by_step.insert(filter->getFilterColumnName());
        return true;
    }

    return false;
}

bool hasDuplicatedNames(const Block & header)
{
    std::unordered_set<std::string_view> seen;
    for (const auto & column : header)
        if (!seen.insert(column.name).second)
            return true;

    return false;
}

bool hasDuplicatedInputNames(const ActionsDAG & actions)
{
    std::unordered_set<std::string_view> seen;
    for (const auto * input : actions.getInputs())
        if (!seen.insert(input->result_name).second)
            return true;

    return false;
}

/// One function to recompute below the child step. Captured from the parent's node before the
/// rewrite, because the parent's `ActionsDAG` is replaced by the second part of the split.
struct PushedFunction
{
    FunctionBasePtr function_base;
    /// Exactly the name of the parent's node, so that the second part of the split finds it.
    String result_name;
    /// Name of the argument column in the child step's *input* header, which may differ from the
    /// name the parent sees when the child renames the column.
    String argument_name;
};

}

std::unordered_map<const ActionsDAG::Node *, ActionsDAG::NodeRawConstPtrs>
collectVolumeReducingFunctionsReplacingTheirArgument(const ActionsDAG & actions)
{
    /// The column a node stands for, for inputs and for chains of renames of an input.
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> column_of_node;
    for (const auto & node : actions.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::INPUT)
            column_of_node.emplace(&node, &node);
        else if (node.type == ActionsDAG::ActionType::ALIAS)
            if (const auto * source = resolveAliases(&node); source->type == ActionsDAG::ActionType::INPUT)
                column_of_node.emplace(&node, source);
    }

    std::unordered_map<const ActionsDAG::Node *, ActionsDAG::NodeRawConstPtrs> result;
    for (const auto & node : actions.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || !node.function_base->isVolumeReducing())
            continue;

        /// The function is going to be evaluated on a different set of rows.
        if (!node.function_base->isDeterministic() || !node.function_base->isDeterministicInScopeOfQuery())
            continue;

        if (node.children.size() != 1)
            continue;

        auto it = column_of_node.find(node.children.front());
        if (it == column_of_node.end())
            continue;

        if (!isSupportedArgumentType(node.function_base->getName(), it->second->result_type))
            continue;

        result[it->second].push_back(&node);
    }

    if (result.empty())
        return result;

    /// A column is replaced only if nothing but those functions reads it: otherwise the wide column
    /// is needed anyway and computing the functions early only adds to the data being carried.
    std::unordered_set<const ActionsDAG::Node *> has_other_readers;
    for (const auto & node : actions.getNodes())
    {
        const auto * reader_column = column_of_node.contains(&node) ? column_of_node.at(&node) : nullptr;
        for (const auto * child : node.children)
        {
            auto it = column_of_node.find(child);
            if (it == column_of_node.end() || it->second == reader_column)
                continue;

            auto functions = result.find(it->second);
            if (functions == result.end())
                continue;

            if (std::find(functions->second.begin(), functions->second.end(), &node) == functions->second.end())
                has_other_readers.insert(it->second);
        }
    }

    for (const auto * output : actions.getOutputs())
        if (auto it = column_of_node.find(output); it != column_of_node.end())
            has_other_readers.insert(it->second);

    for (auto it = result.begin(); it != result.end();)
    {
        /// Two functions producing the same name would be indistinguishable in a header.
        NameSet names;
        bool has_duplicated_names = false;
        for (const auto * function : it->second)
            has_duplicated_names |= !names.insert(function->result_name).second;

        if (has_other_readers.contains(it->first) || has_duplicated_names)
            it = result.erase(it);
        else
            ++it;
    }

    return result;
}

/// Pushes volume-reducing functions (`length`, `lengthUTF8`, `empty`, `notEmpty`) from an
/// `Expression` / `Filter` step below its child step, so that the wide `String` / `FixedString` /
/// `Array` / `Map` argument is replaced by the fixed-size result:
///
///   Expression  [lengthUTF8(s)]        Expression  [INPUT lengthUTF8(s)]
///     └── Sorting                        └── Sorting        [carries a UInt64, not a String]
///           └── X              →               └── Expression  [pushed: lengthUTF8(s)]
///                                                    └── X
///
/// The parent is rewritten with `ActionsDAG::split`: the function nodes form the first part, which
/// is discarded, and the second part becomes the parent's `ActionsDAG` and reads the results as
/// inputs. The step below the child is built separately because the child may rename the column
/// (`Change column names to column identifiers` is usually merged into the `Filter`), so the pushed
/// functions have to read the argument under its name in the child's input header while producing
/// the result under the name the parent expects.
///
/// The rewrite is only applied when the wide argument column really stops flowing through the child
/// step: it is removed from the child's output and — unless the child itself reads it, like a
/// `Filter` whose condition mentions it — it is not even produced below the child anymore. Without
/// that the only effect would be computing the function earlier, on more rows.
size_t tryPushDownVolumeReducingFunction(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (parent_node->children.size() != 1)
        return 0;

    auto * parent_actions = tryGetStepActions(parent_node->step.get());
    if (!parent_actions)
        return 0;

    /// `ARRAY JOIN` changes the number of rows, so the two split parts are not interchangeable.
    /// Stateful and non-deterministic functions stay in the second part and keep seeing the same
    /// rows in the same order, so they do not prevent the rewrite.
    if (parent_actions->hasArrayJoin())
        return 0;

    /// The rewrite resolves columns by name, which is ambiguous with duplicated names.
    if (hasDuplicatedInputNames(*parent_actions))
        return 0;

    auto functions_by_argument = collectVolumeReducingFunctionsReplacingTheirArgument(*parent_actions);
    if (functions_by_argument.empty())
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    if (child_node->children.size() != 1)
        return 0;

    NameSet columns_pinned_by_child;
    if (!canPushBelowStep(child_node->step.get(), columns_pinned_by_child))
        return 0;

    const Block & child_input_header = *child_node->children.front()->step->getOutputHeader();
    const Block & child_output_header = *child_node->step->getOutputHeader();
    if (hasDuplicatedNames(child_input_header) || hasDuplicatedNames(child_output_header))
        return 0;

    ActionsDAG * child_actions = tryGetStepActions(child_node->step.get());
    const NameSet child_inputs_in_use = child_actions ? collectInputsNeededByOutputs(*child_actions) : NameSet{};

    std::unordered_set<const ActionsDAG::Node *> functions_to_split;
    std::vector<PushedFunction> pushed_functions;
    /// Names of the columns the child stops surfacing, on the parent's and on the child's side.
    NameSet columns_to_stop_surfacing;
    NameSet columns_to_stop_producing;
    for (const auto & [argument, functions] : functions_by_argument)
    {
        /// Name of the column as the parent receives it, and as the child receives it. They differ
        /// when the child renames the column on its way up.
        const String & name_above = argument->result_name;
        String name_below = name_above;

        if (child_actions)
        {
            if (const auto * surfaced = child_actions->tryFindInOutputs(name_above))
            {
                const auto * source = resolveAliases(surfaced);
                if (source->type != ActionsDAG::ActionType::INPUT)
                    continue;
                name_below = source->result_name;
            }
            else if (child_inputs_in_use.contains(name_above))
            {
                /// A column the child consumes but does not output cannot reach the parent at all.
                continue;
            }
        }

        if (columns_pinned_by_child.contains(name_below))
            continue;

        const auto * column_below = child_input_header.findByName(name_below);
        if (!column_below || !column_below->type->equals(*argument->result_type))
            continue;

        for (const auto * function : functions)
            pushed_functions.push_back({function->function_base, function->result_name, name_below});
        functions_to_split.insert(functions.begin(), functions.end());
        columns_to_stop_surfacing.insert(name_above);
        columns_to_stop_producing.insert(name_below);
    }

    if (pushed_functions.empty())
        return 0;

    /// A result must not shadow a column that already exists around the child step, otherwise the
    /// parent's new input could bind to that column instead of the computed one.
    for (const auto & pushed_function : pushed_functions)
        if (child_input_header.has(pushed_function.result_name) || child_output_header.has(pushed_function.result_name))
            return 0;

    /// Stop the child from surfacing the replaced columns. Input declarations are kept on purpose:
    /// dropping one would turn the column into an unmatched passthrough and put it right back into
    /// the child's output. A column that stays in use (a `Filter` reading it in its condition) is
    /// still produced below the child, it just does not leave the child anymore.
    std::optional<ActionsDAG> pruned_child_actions;
    if (child_actions)
    {
        auto pruned = child_actions->clone();
        pruned.removeFromOutputs(columns_to_stop_surfacing);
        pruned.removeUnusedActions(/*allow_remove_inputs=*/false, /*allow_constant_folding=*/false);

        for (const auto & name : collectInputsNeededByOutputs(pruned))
            columns_to_stop_producing.erase(name);

        pruned_child_actions = std::move(pruned);
    }

    /// The step below the child: the child's input columns minus the replaced ones, plus one node
    /// per pushed function named exactly like the parent's node, so that the second part of the
    /// split below finds it as an input after the child passes it through.
    ActionsDAG pushed_actions;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> input_by_name;
    ActionsDAG::NodeRawConstPtrs pushed_outputs;
    pushed_outputs.reserve(child_input_header.columns() + pushed_functions.size());
    for (const auto & column : child_input_header)
    {
        const auto & input = pushed_actions.addInput(column);
        input_by_name.emplace(input.result_name, &input);
        if (!columns_to_stop_producing.contains(column.name))
            pushed_outputs.push_back(&input);
    }

    for (const auto & pushed_function : pushed_functions)
    {
        const auto & function = pushed_actions.addFunction(
            pushed_function.function_base, {input_by_name.at(pushed_function.argument_name)}, pushed_function.result_name);
        pushed_outputs.push_back(&function);
    }
    pushed_actions.getOutputs() = std::move(pushed_outputs);

    /// The second part of the split reads the results as inputs of the same name.
    auto split_result = parent_actions->split(functions_to_split);

    /// All the bail-outs are behind us, the plan may be modified now.
    *parent_actions = std::move(split_result.second);
    if (pruned_child_actions)
        *child_actions = std::move(*pruned_child_actions);

    auto & pushed_node = nodes.emplace_back();
    pushed_node.children.swap(child_node->children);
    child_node->children = {&pushed_node};
    pushed_node.step = std::make_unique<ExpressionStep>(pushed_node.children.front()->step->getOutputHeader(), std::move(pushed_actions));

    /// The pass can fire again on a step whose description already carries the marker (the pushed
    /// step gets merged into the child and the result is pushed further down). Without the guard
    /// the marker would be appended once per round.
    auto base_description = parent_node->step->getStepDescription();
    constexpr std::string_view marker = " [volume-reducing functions]";
    auto description = base_description.find(marker) != std::string_view::npos
        ? std::string{base_description}
        : fmt::format("{}{}", base_description, marker);
    pushed_node.step->setStepDescription(std::move(description), settings.max_step_description_length);

    child_node->step->updateInputHeader(pushed_node.step->getOutputHeader());
    parent_node->step->updateInputHeader(child_node->step->getOutputHeader());

    return 3;
}

}
