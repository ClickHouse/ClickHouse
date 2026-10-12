#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

namespace
{

using Node = ActionsDAG::Node;
using ActionType = ActionsDAG::ActionType;

constexpr size_t max_chain_length = 4;

const Node * skipAliases(const Node * node)
{
    while (node->type == ActionType::ALIAS)
        node = node->children.front();
    return node;
}

bool isAndFunction(const Node * node)
{
    return node->type == ActionType::FUNCTION && node->function_base && node->function_base->getName() == "and";
}

/// An AND atom that `FilterStep` never splits off into its own filter.
bool isTrivialAtom(const Node * node)
{
    node = skipAliases(node);
    return node->type != ActionType::FUNCTION && node->type != ActionType::ARRAY_JOIN;
}

/// What a value is computed by: a node of one of the DAGs, or a column of the stream under the chain.
struct Origin
{
    const Node * node = nullptr;
    /// The DAG of `node`: 0 is the join, `i + 1` is the chain step `i`.
    size_t level = 0;
    std::string_view bottom_column;

    static Origin ofNode(const Node * node, size_t level) { return {node, level, {}}; }
    static Origin ofBottomColumn(std::string_view name) { return {nullptr, 0, name}; }

    bool isSame(const Origin & other) const
    {
        return node ? node == other.node : (!other.node && bottom_column == other.bottom_column);
    }
};

struct ChainStep
{
    QueryPlan::Node * plan_node;
    ActionsDAG * dag;
    const FilterStep * filter;
    std::unordered_map<std::string_view, size_t> output_name_count;
};

/// A column of the output of a chain step that the key of the join can be computed from.
struct ProvidedColumn
{
    /// The chain step whose output gets the node `exported`, or none when the column is already in the output.
    std::optional<size_t> export_step;
    const Node * exported = nullptr;
    /// The chain steps that have to pass the column through, from the deepest to the join's child.
    size_t forward_steps = 0;
    String name;
    DataTypePtr type;

    bool isInJoinInput() const { return !export_step && forward_steps == 0; }
};

struct Match
{
    const Node * node;
    size_t level;
    const Node * provider;
};

/// One side of the join and the Expression/Filter steps directly under it.
class JoinSide
{
public:
    JoinSide(QueryPlan::Node & child, const Optimization::ExtraSettings & settings_) : settings(settings_)
    {
        QueryPlan::Node * node = &child;
        while (chain.size() < max_chain_length && node->children.size() == 1)
        {
            auto * expression = typeid_cast<ExpressionStep *>(node->step.get());
            auto * filter = typeid_cast<FilterStep *>(node->step.get());
            ActionsDAG * dag = expression ? &expression->getExpression() : (filter ? &filter->getExpression() : nullptr);
            if (!dag || dag->hasArrayJoin() || dag->hasStatefulFunctions())
                break;

            ChainStep step{node, dag, filter, {}};
            for (const auto & column : *node->step->getOutputHeader())
                ++step.output_name_count[column.name];
            chain.push_back(std::move(step));
            node = node->children.front();
        }

        /// Only a MergeTree read with its own multiple PREWHERE steps keeps each atom of the filters above it in its own filter.
        const auto * read = typeid_cast<const ReadFromMergeTree *>(node->step.get());
        atoms_run_as_own_filters = settings.merge_filters && read && read->getReaderSettings().enable_multiple_prewhere_read_steps;

        for (size_t i = 0; i < chain.size(); ++i)
            collectEagerNodes(i);
    }

    bool empty() const { return chain.empty(); }

    bool hasUniqueOutputNames() const
    {
        return std::ranges::all_of(chain.front().output_name_count, [](const auto & entry) { return entry.second == 1; });
    }

    bool hasOutput(size_t step, const String & name) const { return chain[step].output_name_count.contains(name); }

    /// What computes the column `name` read by the DAG at `level`.
    std::optional<Origin> inputOrigin(size_t level, const String & name)
    {
        if (level == chain.size())
        {
            const auto & header = *chain.back().plan_node->step->getInputHeaders().front();
            size_t count = 0;
            const String * column_name = nullptr;
            for (const auto & column : header)
            {
                if (column.name == name)
                {
                    ++count;
                    column_name = &column.name;
                }
            }
            if (count != 1)
                return {};
            return Origin::ofBottomColumn(*column_name);
        }

        const auto & step = chain[level];
        auto count_it = step.output_name_count.find(name);
        if (count_it == step.output_name_count.end() || count_it->second != 1)
            return {};
        if (step.filter && step.filter->removesFilterColumn() && step.filter->getFilterColumnName() == name)
            return {};

        const Node * output = nullptr;
        for (const auto * node : step.dag->getOutputs())
        {
            if (node->result_name != name)
                continue;
            if (output)
                return {};
            output = node;
        }
        if (output)
            return nodeOrigin(level + 1, output);

        for (const auto * input : step.dag->getInputs())
            if (input->result_name == name)
                return {};
        return inputOrigin(level + 1, name);
    }

    std::optional<Origin> nodeOrigin(size_t level, const Node * node)
    {
        node = skipAliases(node);
        if (node->type == ActionType::INPUT)
            return inputOrigin(level, node->result_name);
        return Origin::ofNode(node, level);
    }

    bool isSameComputation(const Origin & lhs, const Origin & rhs)
    {
        if (lhs.isSame(rhs))
            return true;
        if (!lhs.node || !rhs.node)
            return false;

        auto [it, inserted] = same_computation_cache.try_emplace({lhs.node, rhs.node}, false);
        if (!inserted)
            return it->second;

        checkStackSize();
        it->second = isSameComputationImpl(lhs, rhs);
        return it->second;
    }

    /// The eager node of a chain step that computes the same as `node` of the join's DAG.
    std::optional<Match> findMatch(const Node * node)
    {
        for (size_t step = 0; step < chain.size(); ++step)
            for (const auto * provider : eager_nodes[step])
                if (isSameComputation(Origin::ofNode(node, 0), Origin::ofNode(provider, step + 1)))
                    return Match{node, step + 1, provider};
        return {};
    }

    /// The column of the output of the join's child that holds `match`, exporting and forwarding it if needed.
    /// `planned` holds the names already planned for each step, so that two columns never get one name.
    std::optional<ProvidedColumn> provideColumn(const Match & match, std::map<std::pair<size_t, String>, const Node *> & planned)
    {
        size_t provider_step = match.level - 1;
        auto provider = Origin::ofNode(match.provider, match.level);

        ProvidedColumn result;
        std::optional<size_t> available_at;
        for (size_t step = 0; step <= provider_step && !available_at; ++step)
        {
            for (const auto & column : *chain[step].plan_node->step->getOutputHeader())
            {
                auto origin = inputOrigin(step, column.name);
                if (origin && origin->isSame(provider))
                {
                    available_at = step;
                    result.name = column.name;
                    result.type = column.type;
                    break;
                }
            }
        }

        size_t top_step = 0;
        if (available_at)
        {
            top_step = *available_at;
        }
        else
        {
            result.export_step = provider_step;
            result.exported = match.provider;
            result.name = match.provider->result_name;
            result.type = match.provider->result_type;
            top_step = provider_step;
            if (!canAddName(provider_step, result.name, match.provider, planned))
                return {};
            for (const auto * output : chain[provider_step].dag->getOutputs())
                if (output->result_name == result.name)
                    return {};
        }

        for (size_t step = 0; step < top_step; ++step)
        {
            if (!canAddName(step, result.name, match.provider, planned))
                return {};
            size_t inputs_with_name = 0;
            for (const auto * input : chain[step].dag->getInputs())
                inputs_with_name += input->result_name == result.name;
            if (inputs_with_name > 1)
                return {};
        }

        if (result.export_step)
            planned[{*result.export_step, result.name}] = match.provider;
        for (size_t step = 0; step < top_step; ++step)
            planned[{step, result.name}] = match.provider;
        result.forward_steps = top_step;
        return result;
    }

    /// Adds the column to the outputs of the chain steps. Returns the number of changed steps.
    size_t applyProvidedColumn(const ProvidedColumn & column)
    {
        size_t changed_steps = 0;
        if (column.export_step)
        {
            auto & outputs = chain[*column.export_step].dag->getOutputs();
            if (std::ranges::find(outputs, column.exported) == outputs.end())
                outputs.push_back(column.exported);
            changed_steps = *column.export_step + 1;
        }

        for (size_t step = 0; step < column.forward_steps; ++step)
        {
            auto & dag = *chain[step].dag;
            const Node * input = nullptr;
            for (const auto * node : dag.getInputs())
                if (node->result_name == column.name)
                    input = node;
            if (!input)
                input = &dag.addInput(column.name, column.type);

            auto & outputs = dag.getOutputs();
            if (std::ranges::find(outputs, input) == outputs.end())
                outputs.push_back(input);
        }
        return std::max(changed_steps, column.forward_steps);
    }

    void updateHeaders(size_t changed_steps)
    {
        for (size_t step = changed_steps; step > 0; --step)
        {
            auto & plan_node = *chain[step - 1].plan_node;
            plan_node.step->updateInputHeader(plan_node.children.front()->step->getOutputHeader());
        }
    }

private:
    bool canAddName(size_t step, const String & name, const Node * provider, const std::map<std::pair<size_t, String>, const Node *> & planned) const
    {
        if (auto it = planned.find({step, name}); it != planned.end())
            return it->second == provider;
        return !hasOutput(step, name);
    }

    bool isSameComputationImpl(const Origin & lhs, const Origin & rhs)
    {
        const Node & lhs_node = *lhs.node;
        const Node & rhs_node = *rhs.node;
        if (!ActionsDAG::isSameOperation(lhs_node, rhs_node))
            return false;
        if (lhs_node.type != ActionType::FUNCTION || rhs_node.type != ActionType::FUNCTION)
            return true;

        if (lhs_node.children.size() != rhs_node.children.size()
            || !allNodeFunctions(lhs_node, isInsensitiveToEvaluationCount) || !allNodeFunctions(rhs_node, isInsensitiveToEvaluationCount))
            return false;

        const bool name_insensitive = lhs_node.function_base->isNameInsensitive() && rhs_node.function_base->isNameInsensitive();
        /// Steps may be built with different settings, and only name-insensitive functions hash all the state they capture
        if (!name_insensitive && lhs_node.function_base != rhs_node.function_base)
            return false;
        for (size_t i = 0; i < lhs_node.children.size(); ++i)
        {
            const auto * lhs_child = lhs_node.children[i];
            const auto * rhs_child = rhs_node.children[i];
            if (WhichDataType(lhs_child->result_type).isFunction() || WhichDataType(rhs_child->result_type).isFunction())
                return false;
            if (!name_insensitive && lhs_child->result_name != rhs_child->result_name)
                return false;

            auto lhs_origin = nodeOrigin(lhs.level, lhs_child);
            auto rhs_origin = nodeOrigin(rhs.level, rhs_child);
            if (!lhs_origin || !rhs_origin || !isSameComputation(*lhs_origin, *rhs_origin))
                return false;
        }
        return true;
    }

    /// Function nodes the step evaluates on each of its rows: not below a lazily executed argument of a
    /// short-circuit function. A filter of `FilterStep` and the atoms of its AND chain are not collected,
    /// because the step may replace them with a constant.
    void collectEagerNodes(size_t step)
    {
        const auto & dag = *chain[step].dag;
        std::vector<const Node *> stack(dag.getOutputs().begin(), dag.getOutputs().end());
        std::unordered_set<const Node *> excluded;

        if (const auto * filter = chain[step].filter)
        {
            const auto * filter_output = &dag.findInOutputs(filter->getFilterColumnName());
            const auto * filter_node = skipAliases(filter_output);
            std::erase_if(stack, [&](const Node * output) { return output == filter_output || output == filter_node; });
            excluded.insert(filter_node);

            std::vector<const Node *> atoms;
            std::vector<const Node *> and_stack{filter_node};
            while (!and_stack.empty())
            {
                const auto * node = and_stack.back();
                and_stack.pop_back();
                if (isAndFunction(node))
                    and_stack.insert(and_stack.end(), node->children.rbegin(), node->children.rend());
                else
                    atoms.push_back(node);
            }
            for (const auto * atom : atoms)
                excluded.insert(skipAliases(atom));

            /// Eager atoms: those FilterStep splits off, if each atom runs as its own filter.
            if (atoms_run_as_own_filters && !dag.hasStatefulFunctions())
            {
                size_t last = atoms.size();
                for (size_t i = 0; i < atoms.size(); ++i)
                    if (!isTrivialAtom(atoms[i]))
                        last = i;

                bool trivial_before = false;
                for (size_t i = 0; i < atoms.size(); ++i)
                {
                    if (isTrivialAtom(atoms[i]))
                        trivial_before = true;
                    else if (i != last || !trivial_before)
                        stack.push_back(atoms[i]);
                }
            }
        }

        auto & result = eager_nodes.emplace_back();
        std::unordered_set<const Node *> visited;
        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();
            if (!visited.insert(node).second)
                continue;

            if (node->type == ActionType::ALIAS)
            {
                stack.push_back(node->children.front());
                continue;
            }
            if (node->type != ActionType::FUNCTION || node->column)
                continue;

            if (!excluded.contains(node))
                result.push_back(node);

            IFunctionBase::ShortCircuitSettings short_circuit_settings;
            /// A lazy argument never counts, whatever the setting: a read runs its PREWHERE with its own context.
            if (node->function_base->isShortCircuit(short_circuit_settings, node->children.size()))
            {
                for (size_t argument : short_circuit_settings.arguments_with_disabled_lazy_execution)
                    if (argument < node->children.size())
                        stack.push_back(node->children[argument]);
                continue;
            }
            stack.insert(stack.end(), node->children.begin(), node->children.end());
        }
    }

    const Optimization::ExtraSettings & settings;
    bool atoms_run_as_own_filters = false;
    std::vector<ChainStep> chain;
    std::vector<std::vector<const Node *>> eager_nodes;
    std::map<std::pair<const Node *, const Node *>, bool> same_computation_cache;
};

struct KeyOperand
{
    size_t predicate;
    bool is_lhs;
    const Node * node;
    std::vector<Match> matches;
    std::vector<ProvidedColumn> columns;
};

void collectMatches(JoinSide & side, const Node * node, std::vector<Match> & matches, std::unordered_set<const Node *> & visited)
{
    node = skipAliases(node);
    if (node->type != ActionType::FUNCTION || node->column || !visited.insert(node).second)
        return;

    if (auto match = side.findMatch(node))
    {
        matches.push_back(*match);
        return;
    }
    for (const auto * child : node->children)
        collectMatches(side, child, matches, visited);
}

/// Whether `copyWithReplacements` rebuilds `node` for these matches. Sets `sensitive` when it rebuilds a function sensitive
/// to how often it runs: another consumer of the original node, such as an ASOF inequality, would evaluate it once more.
bool isRebuilt(const Node * node, const std::unordered_set<const Node *> & matches,
    std::unordered_map<const Node *, bool> & rebuilt, bool & sensitive)
{
    if (matches.contains(node))
        return true;
    if (node->type == ActionType::ARRAY_JOIN)
        return false;
    if (auto it = rebuilt.find(node); it != rebuilt.end())
        return it->second;

    checkStackSize();
    bool result = false;
    for (const auto * child : node->children)
        result = isRebuilt(child, matches, rebuilt, sensitive) || result;
    if (result && !allNodeFunctions(*node, isInsensitiveToEvaluationCount))
        sensitive = true;
    rebuilt.emplace(node, result);
    return result;
}

[[maybe_unused]] bool hasInputsInHeaderOrder(const JoinStepLogical & join_step)
{
    const auto & inputs = join_step.getActionsDAG().getInputs();
    const size_t left_inputs = join_step.getInputHeaders().front()->columns();
    if (inputs.size() != left_inputs + join_step.getInputHeaders().back()->columns())
        return false;
    for (size_t i = 0; i < inputs.size(); ++i)
        if (JoinActionRef(inputs[i], join_step.getExpressionActions()).fromLeft() != (i < left_inputs))
            return false;
    return true;
}

/// The operands of the equi-join keys of one side that can be computed from columns of the chain under it.
std::vector<KeyOperand> planKeyOperands(JoinSide & side, const JoinOperator & join_operator, bool is_left, const Block & other_header)
{
    std::vector<KeyOperand> operands;
    std::map<std::pair<size_t, String>, const Node *> planned;

    for (size_t predicate = 0; predicate < join_operator.expression.size(); ++predicate)
    {
        auto [op, lhs, rhs] = join_operator.expression[predicate].asBinaryPredicate();
        if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
            continue;
        if (!((lhs.fromLeft() && rhs.fromRight()) || (lhs.fromRight() && rhs.fromLeft())))
            continue;

        const bool is_lhs = lhs.fromLeft() == is_left;
        const auto * operand_node = (is_lhs ? lhs : rhs).getNode();
        if (skipAliases(operand_node)->type == ActionType::INPUT)
            continue;

        KeyOperand operand{predicate, is_lhs, operand_node, {}, {}};
        std::unordered_set<const Node *> visited;
        collectMatches(side, operand_node, operand.matches, visited);
        if (operand.matches.empty())
            continue;

        std::unordered_set<const Node *> match_nodes;
        for (const auto & match : operand.matches)
            match_nodes.insert(match.node);
        std::unordered_map<const Node *, bool> rebuilt;
        bool sensitive = false;
        isRebuilt(operand_node, match_nodes, rebuilt, sensitive);
        if (sensitive)
            continue;

        auto new_planned = planned;
        for (const auto & match : operand.matches)
        {
            auto column = side.provideColumn(match, new_planned);
            if (!column || other_header.has(column->name))
                break;
            operand.columns.push_back(std::move(*column));
        }
        if (operand.columns.size() != operand.matches.size())
            continue;

        planned = std::move(new_planned);
        operands.push_back(std::move(operand));
    }

    return operands;
}

/// `node` rebuilt in `dag` to read `replacements` instead of the nodes they map; unchanged subtrees are shared
const Node * copyWithReplacements(ActionsDAG & dag, const Node * node,
    const std::unordered_map<const Node *, const Node *> & replacements, std::unordered_map<const Node *, const Node *> & copies)
{
    if (auto it = replacements.find(node); it != replacements.end())
        return it->second;
    if (auto it = copies.find(node); it != copies.end())
        return it->second;
    /// Rebuilding an ARRAY_JOIN would expand the rows twice when another key still reads the original one
    if (node->type == ActionType::ARRAY_JOIN)
        return node;

    checkStackSize();
    ActionsDAG::NodeRawConstPtrs children;
    bool changed = false;
    for (const auto * child : node->children)
    {
        children.push_back(copyWithReplacements(dag, child, replacements, copies));
        changed = changed || children.back() != child;
    }

    const Node * result = node;
    if (changed)
    {
        if (node->type == ActionType::ALIAS)
            result = &dag.addAlias(*children.front(), node->result_name);
        else if (node->type == ActionType::FUNCTION)
            result = &dag.addFunction(node->function_base, std::move(children), node->result_name);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot rebuild the node {} of a join key", node->result_name);
    }
    copies.emplace(node, result);
    return result;
}

/// The operand computed in the join's actions from the inputs that hold its matches.
JoinActionRef rewriteOperand(const KeyOperand & operand, const std::unordered_map<String, JoinActionRef> & provided_inputs,
    std::unordered_map<const Node *, const Node *> & replacements, std::unordered_map<const Node *, const Node *> & copies)
{
    std::vector<JoinActionRef> inputs;
    for (const auto & column : operand.columns)
        inputs.push_back(provided_inputs.at(column.name));

    if (operand.matches.front().node == skipAliases(operand.node))
        return inputs.front();

    return JoinActionRef::transform(inputs, [&](ActionsDAG & dag, ActionsDAG::NodeRawConstPtrs nodes)
    {
        /// A function can read the names of its arguments, so a replacement keeps the name of the node it replaces.
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            const auto * match = operand.matches[i].node;
            if (!replacements.contains(match))
                replacements[match] = nodes[i]->result_name == match->result_name ? nodes[i] : &dag.addAlias(*nodes[i], match->result_name);
        }
        return copyWithReplacements(dag, operand.node, replacements, copies);
    });
}

}

size_t tryReuseExpressionsInJoinKeys(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings & settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(parent_node->step.get());
    if (!join_step || parent_node->children.size() != 2)
        return 0;

    auto & join_operator = join_step->getJoinOperator();
    auto & expression_actions = join_step->getExpressionActions();

    /// The new operands of the changed predicates, by the position of the predicate
    std::unordered_map<size_t, std::pair<JoinActionRef, JoinActionRef>> new_operands;
    auto set_new_operand = [&](const KeyOperand & operand, JoinActionRef key)
    {
        auto & [new_lhs, new_rhs] = new_operands.try_emplace(operand.predicate, nullptr, nullptr).first->second;
        (operand.is_lhs ? new_lhs : new_rhs) = key;
    };

    std::unordered_map<const Node *, const Node *> replacements;
    std::unordered_map<const Node *, const Node *> copies;

    size_t update_depth = 0;
    for (size_t side_index = 0; side_index < 2; ++side_index)
    {
        auto & child = *parent_node->children[side_index];
        if (typeid_cast<const JoinStepLogicalLookup *>(child.step.get()))
            continue;

        JoinSide side(child, settings);
        if (side.empty() || !side.hasUniqueOutputNames())
            continue;

        const bool is_left = side_index == 0;
        auto operands = planKeyOperands(side, join_operator, is_left, *join_step->getInputHeaders()[1 - side_index]);
        if (operands.empty())
            continue;

        size_t changed_steps = 0;
        for (const auto & operand : operands)
            for (const auto & column : operand.columns)
                changed_steps = std::max(changed_steps, side.applyProvidedColumn(column));
        side.updateHeaders(changed_steps);

        std::unordered_map<String, JoinActionRef> provided_inputs;
        if (changed_steps > 0)
        {
            const size_t old_columns = join_step->getInputHeaders()[side_index]->columns();
            join_step->updateInputHeader(child.step->getOutputHeader(), side_index);
            const auto & new_header = *child.step->getOutputHeader();
            for (size_t position = old_columns; position < new_header.columns(); ++position)
            {
                const auto & column = new_header.getByPosition(position);
                provided_inputs.emplace(column.name,
                    expression_actions.addInputOfSide(column.name, column.type, is_left ? JoinTableSide::Left : JoinTableSide::Right));
            }
        }
        for (const auto & operand : operands)
            for (const auto & column : operand.columns)
                if (column.isInJoinInput())
                    provided_inputs.try_emplace(column.name, expression_actions.findNode(column.name, /* is_input= */ true));

        for (const auto & operand : operands)
            set_new_operand(operand, rewriteOperand(operand, provided_inputs, replacements, copies));

        update_depth = std::max(update_depth, 1 + changed_steps);
    }

    if (new_operands.empty())
        return 0;

    for (auto & [predicate, operands] : new_operands)
    {
        auto & expression = join_operator.expression[predicate];
        auto [op, lhs, rhs] = expression.asBinaryPredicate();
        auto function_base = expression.getNode()->function_base;
        expression = JoinActionRef::transform(
            {operands.first ? operands.first : lhs, operands.second ? operands.second : rhs},
            [&](ActionsDAG & dag, ActionsDAG::NodeRawConstPtrs arguments) { return &dag.addFunction(function_base, std::move(arguments), {}); });
    }

    chassert(hasInputsInHeaderOrder(*join_step));
    return update_depth;
}

}
