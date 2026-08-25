#include <Core/Block.h>
#include <Core/Names.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
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

/// The optimization applies only to `String` / `FixedString`. Although `length`, `empty` and
/// `notEmpty` also accept `Array` and `Map`, a composite path can share its payload with other
/// expressions under a different nested-column name. The optimizer cannot prove that removing one
/// such path removes the payload from a sort or filter, so it must not push those functions down.
/// `empty` / `notEmpty` are additionally defined for fixed-size types (`UUID`, `IPv4`, `IPv6`,
/// `QBit`), but replacing a fixed-size column with another fixed-size column reduces no volume.
bool isSupportedArgumentType(const String & function_name, const DataTypePtr & type)
{
    if (function_name == "lengthUTF8")
    {
        if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(type.get()))
            return fixed_string->getN() > sizeof(UInt64);

        return isStringOrFixedString(type);
    }

    if (function_name == "length" || function_name == "empty" || function_name == "notEmpty")
        return isStringOrFixedString(type);

    return false;
}

/// A `Filter` evaluates the pushed function for every input row, whereas the original plan
/// evaluates it only for rows which pass. Restrict this path to operations whose cost does not
/// depend on the payload size. `lengthUTF8` scans every byte, and `empty` / `notEmpty` scan a
/// `FixedString` to find a non-zero byte, so a selective predicate that also reads the argument
/// can make either rewrite slower.
bool isCheapToEvaluateBeforeFilter(const IFunctionBase & function, const DataTypePtr & type)
{
    const auto & function_name = function.getName();
    return function_name == "length"
        || ((function_name == "empty" || function_name == "notEmpty") && typeid_cast<const DataTypeString *>(type.get()));
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

/// Find actions below a chain of one-input, header-preserving steps. `SortingStep` itself owns no
/// actions, and steps such as `LimitStep` can sit between it and the `ExpressionStep` that created
/// aliases of the same wide input.
ActionsDAG * findActionsBelowHeaderPreservingSteps(QueryPlan::Node * node)
{
    while (node && node->children.size() == 1)
    {
        if (auto * actions = tryGetStepActions(node->step.get()))
            return actions;

        const auto & input_headers = node->step->getInputHeaders();
        if (input_headers.size() != 1 || !blocksHaveEqualStructure(*input_headers.front(), *node->step->getOutputHeader()))
            return nullptr;

        node = node->children.front();
    }

    return nullptr;
}

/// Names of the input columns some output of `actions` depends on. An input nothing depends on is
/// tolerated by `ActionsDAG::updateHeader` and by `ExpressionActions::execute` when the column is
/// missing from the block, so it does not keep the column alive.
NameSet collectInputsNeededByNode(const ActionsDAG::Node * root)
{
    NameSet names;
    std::unordered_set<const ActionsDAG::Node *> visited;
    ActionsDAG::NodeRawConstPtrs stack{root};
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

NameSet collectInputsNeededByOutputs(const ActionsDAG & actions)
{
    NameSet names;
    for (const auto * output : actions.getOutputs())
    {
        auto output_inputs = collectInputsNeededByNode(output);
        names.insert(output_inputs.begin(), output_inputs.end());
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

std::unordered_set<const ActionsDAG::Node *> collectVolumeReducingFunctionsToKeepBelow(
    const ActionsDAG & actions, const ActionsDAG::Node * low_part_root)
{
    /// A column the DAG surfaces crosses the step no matter where the function is computed, so the
    /// function may be lifted as before.
    std::unordered_set<const ActionsDAG::Node *> surfaced;
    for (const auto * output : actions.getOutputs())
        surfaced.insert(resolveAliases(output));

    std::unordered_set<const ActionsDAG::Node *> split_nodes;
    std::unordered_set<const ActionsDAG::Node *> low_part_nodes;
    if (low_part_root)
    {
        ActionsDAG::NodeRawConstPtrs stack{low_part_root};
        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();
            if (!low_part_nodes.insert(node).second)
                continue;

            stack.insert(stack.end(), node->children.begin(), node->children.end());
        }
    }

    for (const auto & node : actions.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || !node.function_base->isVolumeReducing())
            continue;

        if (!node.function_base->isDeterministic() || !node.function_base->isDeterministicInScopeOfQuery())
            continue;

        if (node.children.size() != 1)
            continue;

        /// Unlike in `collectVolumeReducingFunctionsReplacingTheirArgument`, the argument does not
        /// have to be an `INPUT` with no other readers: `tryMergeExpressions` can merge the pushed
        /// step with the step below it (the argument becomes a computed column) or with a `Filter`
        /// whose condition reads the argument. Lifting the function in those cases would make the
        /// wide argument cross the step again, undoing `tryPushDownVolumeReducingFunction` and
        /// re-triggering it forever.
        const auto * argument = resolveAliases(node.children.front());
        if (!isSupportedArgumentType(node.function_base->getName(), argument->result_type))
            continue;

        /// `trySplitFilter` also visits expressions which were never rewritten by
        /// `tryPushDownVolumeReducingFunction`, for example after an outer filter is merged into
        /// a subquery projection. Keep a function below a filter only when the dedicated rewrite
        /// could have moved it there: the predicate must read its argument, the function must be
        /// cheap on every input row, and the predicate must not already calculate the same scalar.
        if (low_part_root)
        {
            if (!low_part_nodes.contains(argument)
                || !isCheapToEvaluateBeforeFilter(*node.function_base, argument->result_type))
                continue;

            bool predicate_computes_function = false;
            for (const auto * low_part_node : low_part_nodes)
            {
                if (low_part_node == &node
                    || low_part_node->type != ActionsDAG::ActionType::FUNCTION
                    || !low_part_node->function_base
                    || low_part_node->children.size() != 1
                    || low_part_node->function_base->getName() != node.function_base->getName())
                    continue;

                if (resolveAliases(low_part_node->children.front()) == argument)
                {
                    predicate_computes_function = true;
                    break;
                }
            }

            if (predicate_computes_function)
                continue;
        }

        if (surfaced.contains(argument))
            continue;

        /// Keeping a volume-reducing function below the barrier only pays off if no calculation
        /// that is still lifted also needs its argument. Otherwise the wide argument crosses the
        /// barrier anyway and evaluating the function before a row-reducing step is a regression.
        bool argument_is_needed_by_lifted_node = false;
        for (const auto & reader : actions.getNodes())
        {
            if (low_part_nodes.contains(&reader))
                continue;

            if (reader.type == ActionsDAG::ActionType::ALIAS)
                continue;

            for (const auto * child : reader.children)
            {
                if (resolveAliases(child) != argument)
                    continue;

                const bool is_another_supported_volume_reducing_function = reader.type == ActionsDAG::ActionType::FUNCTION
                    && reader.function_base && reader.function_base->isVolumeReducing() && reader.function_base->isDeterministic()
                    && reader.function_base->isDeterministicInScopeOfQuery() && reader.children.size() == 1
                    && isSupportedArgumentType(reader.function_base->getName(), argument->result_type);

                /// A sibling function is harmless only if it can stay below the filter too.
                /// Otherwise its argument still crosses the filter, and evaluating this function
                /// for rejected rows is a regression. For example, `length` is cheap but
                /// `lengthUTF8` scans the payload, so neither can stay below a filter that
                /// reads their shared argument.
                const bool can_keep_sibling_below = is_another_supported_volume_reducing_function
                    && (!low_part_root || isCheapToEvaluateBeforeFilter(*reader.function_base, argument->result_type));

                if (&reader != &node && !can_keep_sibling_below)
                {
                    argument_is_needed_by_lifted_node = true;
                    break;
                }
            }

            if (argument_is_needed_by_lifted_node)
                break;
        }

        if (argument_is_needed_by_lifted_node)
            continue;

        split_nodes.insert(&node);
    }

    if (split_nodes.empty())
        return split_nodes;

    /// Prefer splitting at the output itself, so that an alias of a kept function does not end up
    /// alone in the lifted part.
    for (const auto * output : actions.getOutputs())
        if (split_nodes.contains(resolveAliases(output)))
            split_nodes.insert(output);

    return split_nodes;
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
    const auto * child_filter = typeid_cast<const FilterStep *>(child_node->step.get());
    NameSet child_filter_inputs;
    if (child_filter)
    {
        const auto * filter_column = child_actions->tryFindInOutputs(child_filter->getFilterColumnName());
        if (!filter_column)
            return 0;
        child_filter_inputs = collectInputsNeededByNode(filter_column);
    }

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
            const ActionsDAG::Node * source_below = nullptr;
            if (const auto * surfaced = child_actions->tryFindInOutputs(name_above))
            {
                source_below = resolveAliases(surfaced);
                if (source_below->type != ActionsDAG::ActionType::INPUT)
                    continue;
                name_below = source_below->result_name;
            }
            else if (child_inputs_in_use.contains(name_above))
            {
                /// A column the child consumes but does not output cannot reach the parent at all.
                continue;
            }

            /// Removing only the parent-visible alias is not enough when the child also surfaces
            /// the same wide input under another name. That sibling would still cross the barrier,
            /// so pushing the function down would add work without reducing the carried volume.
            bool has_surfaced_sibling = false;
            if (source_below)
                for (const auto * output : child_actions->getOutputs())
                    if (output->result_name != name_above && resolveAliases(output) == source_below)
                    {
                        has_surfaced_sibling = true;
                        break;
                    }

            if (has_surfaced_sibling)
                continue;

            /// A `FilterStep` can pass an unmatched input column through even when its actions do
            /// not surface that column. Inspect the expression below the filter as well: it knows
            /// whether another name in the filter input header is an alias of the same source.
            /// Replacing only one of those names would leave the wide payload in the filter.
            if (child_filter)
            {
                const auto * filter_input_actions = findActionsBelowHeaderPreservingSteps(child_node->children.front());
                if (filter_input_actions)
                {
                    const auto * filter_input_source = filter_input_actions->tryFindInOutputs(name_below);
                    if (filter_input_source)
                    {
                        filter_input_source = resolveAliases(filter_input_source);
                        if (filter_input_source->type != ActionsDAG::ActionType::INPUT)
                            continue;

                        bool has_passthrough_sibling = false;
                        for (const auto * output : filter_input_actions->getOutputs())
                            if (output->result_name != name_below && resolveAliases(output) == filter_input_source)
                            {
                                has_passthrough_sibling = true;
                                break;
                            }

                        if (has_passthrough_sibling)
                            continue;
                    }
                }
            }
        }
        else if (const auto * sorting_input_actions = findActionsBelowHeaderPreservingSteps(child_node->children.front()))
        {
            /// A `SortingStep` has no `ActionsDAG` of its own. Its input can nevertheless expose
            /// the same wide source under multiple aliases, possibly through header-preserving
            /// steps such as `LimitStep`, so inspect the expression below them before deciding
            /// that removing one name reduces the sort payload.
            const auto * source_below = sorting_input_actions->tryFindInOutputs(name_below);
            if (!source_below)
                continue;

            source_below = resolveAliases(source_below);
            if (source_below->type != ActionsDAG::ActionType::INPUT)
                continue;

            bool has_surfaced_sibling = false;
            for (const auto * output : sorting_input_actions->getOutputs())
                if (output->result_name != name_below && resolveAliases(output) == source_below)
                {
                    has_surfaced_sibling = true;
                    break;
                }

            if (has_surfaced_sibling)
                continue;
        }

        if (columns_pinned_by_child.contains(name_below))
            continue;

        /// A filter must itself read the argument. Otherwise the function is evaluated before a
        /// potentially selective filter, adding CPU work while only saving the copied values of
        /// the wide column from rows that survive the filter.
        if (child_filter && !child_filter_inputs.contains(name_below))
            continue;

        /// The filter-specific rewrite changes the number of rows on which the function is
        /// evaluated. Do not use it for functions that scan the payload: saving copies of the
        /// surviving strings cannot in general repay scanning every input string.
        if (child_filter)
        {
            bool all_functions_are_cheap = true;
            for (const auto * function : functions)
                if (!isCheapToEvaluateBeforeFilter(*function->function_base, argument->result_type))
                {
                    all_functions_are_cheap = false;
                    break;
                }

            if (!all_functions_are_cheap)
                continue;
        }

        /// Do not compute a scalar before a filter if its predicate already computes the same
        /// scalar from the same argument. The filter is kept intact by this rewrite, so pushing
        /// the parent's calculation below it would evaluate the scalar twice for rejected rows.
        if (child_filter)
        {
            bool filter_computes_pushed_function = false;
            for (const auto & filter_node : child_actions->getNodes())
            {
                if (filter_node.type != ActionsDAG::ActionType::FUNCTION || !filter_node.function_base || filter_node.children.size() != 1)
                    continue;

                if (resolveAliases(filter_node.children.front())->result_name != name_below)
                    continue;

                for (const auto * function : functions)
                    if (filter_node.function_base->getName() == function->function_base->getName())
                    {
                        filter_computes_pushed_function = true;
                        break;
                    }

                if (filter_computes_pushed_function)
                    break;
            }

            if (filter_computes_pushed_function)
                continue;
        }

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
    NameSet pushed_result_names;
    for (const auto & pushed_function : pushed_functions)
        if (child_input_header.has(pushed_function.result_name) || child_output_header.has(pushed_function.result_name)
            || !pushed_result_names.insert(pushed_function.result_name).second)
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
    auto description = base_description.contains(marker)
        ? std::string{base_description}
        : fmt::format("{}{}", base_description, marker);
    pushed_node.step->setStepDescription(std::move(description), settings.max_step_description_length);

    child_node->step->updateInputHeader(pushed_node.step->getOutputHeader());
    parent_node->step->updateInputHeader(child_node->step->getOutputHeader());

    return 3;
}

}
