#include <Interpreters/ActionsDAG.h>

#include <Analyzer/FunctionNode.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/materialize.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/indexHint.h>
#include <Interpreters/Context.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/SetSerialization.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Core/SortDescription.h>
#include <Planner/PlannerActionsVisitor.h>

#include <algorithm>
#include <stack>
#include <string>
#include <unordered_map>
#include <base/sort.h>
#include <Common/JSONBuilder.h>
#include <Common/Logger.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeSet.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int THERE_IS_NO_COLUMN;
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

namespace
{

std::pair<ColumnsWithTypeAndName, bool> getFunctionArguments(const ActionsDAG::NodeRawConstPtrs & children)
{
    size_t num_arguments = children.size();

    bool all_const = true;
    ColumnsWithTypeAndName arguments(num_arguments);

    for (size_t i = 0; i < num_arguments; ++i)
    {
        const auto & child = *children[i];

        ColumnWithTypeAndName argument;
        argument.column = child.column;
        argument.type = child.result_type;
        argument.name = child.result_name;

        if (!argument.column || !isColumnConst(*argument.column))
            all_const = false;

        arguments[i] = std::move(argument);
    }
    return { std::move(arguments), all_const };
}

bool isConstantFromScalarSubquery(const ActionsDAG::Node * node)
{
    std::stack<const ActionsDAG::Node *> stack;
    stack.push(node);
    while (!stack.empty())
    {
        const auto * arg = stack.top();
        stack.pop();

        if (arg->column && isColumnConst(*arg->column))
            continue;

        while (arg->type == ActionsDAG::ActionType::ALIAS)
            arg = arg->children.at(0);

        if (arg->type != ActionsDAG::ActionType::FUNCTION)
            return false;

        if (arg->function_base->getName() == "__scalarSubqueryResult")
            continue;

        if (arg->children.empty() || !arg->function_base->isSuitableForConstantFolding())
            return false;

        for (const auto * child : arg->children)
            stack.push(child);
    }

    return true;
}

}

bool ActionsDAG::Node::isDeterministic() const
{
    bool deterministic_if_func = type != ActionType::FUNCTION || function_base->isDeterministic();
    bool deterministic_if_const = type != ActionType::COLUMN || is_deterministic_constant;
    return deterministic_if_func && deterministic_if_const;
}

void ActionsDAG::Node::toTree(JSONBuilder::JSONMap & map) const
{
    map.add("Node Type", magic_enum::enum_name(type));

    if (result_type)
        map.add("Result Type", result_type->getName());

    if (!result_name.empty())
        map.add("Result Name", result_name);

    if (column)
        map.add("Column", column->getName());

    if (function_base)
        map.add("Function", function_base->getName());

    if (type == ActionType::FUNCTION)
        map.add("Compiled", is_function_compiled);
}

UInt64 ActionsDAG::Node::getHash() const
{
    SipHash hash_state;
    updateHash(hash_state);
    return hash_state.get64();
}

void ActionsDAG::Node::updateHash(SipHash & hash_state) const
{
    hash_state.update(type);

    if (!result_name.empty())
        hash_state.update(result_name);

    if (result_type)
        hash_state.update(result_type->getName());

    if (function_base)
        hash_state.update(function_base->getName());

    if (function)
        hash_state.update(function->getName());

    hash_state.update(is_function_compiled);
    hash_state.update(is_deterministic_constant);

    if (column)
    {
        hash_state.update(column->getName());

        /// We must also hash the actual constant value, not just the column type name.
        /// Otherwise, two different constants with the same type and the same expression-based
        /// result_name (e.g. from CTE constant folding) would produce identical hashes,
        /// leading to query condition cache collisions and incorrect results.
        if (isColumnConst(*column))
            column->updateHashWithValue(0, hash_state);
    }

    for (const auto & child : children)
        child->updateHash(hash_state);
}

UInt64 ActionsDAG::getHash() const
{
    SipHash hash;
    updateHash(hash);
    return hash.get64();
}

void ActionsDAG::updateHash(SipHash & hash_state) const
{
    struct Frame
    {
        const ActionsDAG::Node * const node;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    for (const auto & node : outputs)
        stack.push({.node = node});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        if (frame.next_child == frame.node->children.size())
        {
            frame.node->updateHash(hash_state);
            stack.pop();
        }
        else
        {
            stack.push({.node = frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
    }
}

ActionsDAG::ActionsDAG(const NamesAndTypesList & inputs_)
{
    for (const auto & input : inputs_)
        outputs.push_back(&addInput(input.name, input.type));
}

ActionsDAG::ActionsDAG(const ColumnsWithTypeAndName & inputs_, bool duplicate_const_columns)
{
    for (const auto & input : inputs_)
    {
        if (input.column && isColumnConst(*input.column) && duplicate_const_columns)
        {
            addInput(input);

            /// Here we also add column.
            /// It will allow to remove input which is actually constant (after projection).
            /// Also, some transforms from query pipeline may randomly materialize constants,
            ///   without any respect to header structure. So, it is a way to drop materialized column and use
            ///   constant value from header.
            /// We cannot remove such input right now cause inputs positions are important in some cases.
            outputs.push_back(&addColumn(input));
        }
        else
            outputs.push_back(&addInput(input.name, input.type));
    }
}

ActionsDAG::Node ActionsDAG::createAlias(const Node & child, std::string alias)
{
    Node node;
    node.type = ActionType::ALIAS;
    node.result_type = child.result_type;
    node.result_name = std::move(alias);
    node.column = child.column;
    node.children.emplace_back(&child);

    return node;
}

ActionsDAG::Node & ActionsDAG::addNode(Node node)
{
    auto & res = nodes.emplace_back(std::move(node));

    // This should only be a temporary fix to avoid regression in 25.10
    // https://github.com/ClickHouse/ClickHouse/issues/90363#issue-3642139014
    if (res.type != ActionType::PLACEHOLDER)
    {
        const auto valid_column = !res.column || (res.column->isConst() || typeid_cast<const ColumnSet *>(res.column.get()) != nullptr);
        if (!valid_column)
            res.column = nullptr;
    }

    if (res.type == ActionType::INPUT)
        inputs.emplace_back(&res);

    return res;
}

const ActionsDAG::Node & ActionsDAG::addInput(std::string name, DataTypePtr type)
{
    Node node;
    node.type = ActionType::INPUT;
    node.result_type = std::move(type);
    node.result_name = std::move(name);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addInput(ColumnWithTypeAndName column)
{
    Node node;
    node.type = ActionType::INPUT;
    node.result_type = std::move(column.type);
    node.result_name = std::move(column.name);
    node.column = std::move(column.column);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addColumn(ColumnWithTypeAndName column, bool is_deterministic_constant)
{
    if (!column.column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add column {} because it is nullptr", column.name);

    Node node;
    node.type = ActionType::COLUMN;
    node.result_type = std::move(column.type);
    node.result_name = std::move(column.name);
    node.column = std::move(column.column);
    node.is_deterministic_constant = is_deterministic_constant;

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addAlias(const Node & child, std::string alias)
{
    return addNode(createAlias(child, std::move(alias)));
}

const ActionsDAG::Node & ActionsDAG::addArrayJoin(const Node & child, std::string result_name)
{
    const auto & array_type = getArrayJoinDataType(child.result_type);
    if (!array_type)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "ARRAY JOIN requires array argument");

    if (result_name.empty())
        result_name = "arrayJoin(" + child.result_name + ")";

    Node node;
    node.type = ActionType::ARRAY_JOIN;
    node.result_type = array_type->getNestedType();
    node.result_name = std::move(result_name);
    node.children.emplace_back(&child);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addFunction(
    const FunctionOverloadResolverPtr & function,
    NodeRawConstPtrs children,
    std::string result_name)
{
    auto [arguments, all_const] = getFunctionArguments(children);

    auto constant_args = function->getArgumentsThatAreAlwaysConstant();
    for (size_t pos : constant_args)
    {
        if (pos >= children.size())
            continue;

        if (arguments[pos].column && isColumnConst(*arguments[pos].column))
            continue;

        if (isConstantFromScalarSubquery(children[pos]))
            arguments[pos].column = arguments[pos].type->createColumnConstWithDefaultValue(0);
    }

    auto function_base = function->build(arguments);
    return addFunctionImpl(
        function_base,
        std::move(children),
        std::move(arguments),
        std::move(result_name),
        function_base->getResultType(),
        all_const);
}

const ActionsDAG::Node & ActionsDAG::addFunction(
    const FunctionNode & function,
    NodeRawConstPtrs children,
    std::string result_name)
{
    auto [arguments, all_const] = getFunctionArguments(children);

    return addFunctionImpl(
        function.getFunction(),
        std::move(children),
        std::move(arguments),
        std::move(result_name),
        function.getResultType(),
        all_const);
}

const ActionsDAG::Node & ActionsDAG::addFunction(
    const FunctionBasePtr & function_base,
    NodeRawConstPtrs children,
    std::string result_name)
{
    auto [arguments, all_const] = getFunctionArguments(children);

    return addFunctionImpl(
        function_base,
        std::move(children),
        std::move(arguments),
        std::move(result_name),
        function_base->getResultType(),
        all_const);
}

const ActionsDAG::Node & ActionsDAG::addCast(const Node & node_to_cast, const DataTypePtr & cast_type, std::string result_name, ContextPtr context)
{
    Field cast_type_constant_value(cast_type->getName());

    ColumnWithTypeAndName column;
    column.name = calculateConstantActionNodeName(cast_type_constant_value);
    column.column = DataTypeString().createColumnConst(0, cast_type_constant_value);
    column.type = std::make_shared<DataTypeString>();

    const auto * cast_type_constant_node = &addColumn(column);
    ActionsDAG::NodeRawConstPtrs children = {&node_to_cast, cast_type_constant_node};
    auto func_base_cast = createInternalCast(ColumnWithTypeAndName{node_to_cast.result_type, node_to_cast.result_name}, cast_type, CastType::nonAccurate, {}, context);

    return addFunction(func_base_cast, std::move(children), result_name);
}

const ActionsDAG::Node & ActionsDAG::addFunctionImpl(
    const FunctionBasePtr & function_base,
    NodeRawConstPtrs children,
    ColumnsWithTypeAndName arguments,
    std::string result_name,
    DataTypePtr result_type,
    bool all_const)
{
    size_t num_arguments = children.size();

    Node node;
    node.type = ActionType::FUNCTION;
    node.children = std::move(children);

    node.function_base = function_base;
    node.result_type = result_type;
    node.function = node.function_base->prepare(arguments);

    /// If all arguments are constants, and function is suitable to be executed in 'prepare' stage - execute function.
    if (node.function_base->isSuitableForConstantFolding())
    {
        ColumnPtr column;

        if (all_const)
        {
            size_t num_rows = arguments.empty() ? 0 : arguments.front().column->size();
            column = node.function->execute(arguments, node.result_type, num_rows, true);
            if (column->getDataType() != node.result_type->getColumnType())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected return type from {}. Expected {}. Got {}",
                    node.function->getName(),
                    node.result_type->getColumnType(),
                    column->getDataType());
        }
        else
        {
            column = node.function_base->getConstantResultForNonConstArguments(arguments, node.result_type);
        }

        /// If the result is not a constant, just in case, we will consider the result as unknown.
        if (column && isColumnConst(*column))
        {
            /// All constant (literal) columns in block are added with size 1.
            /// But if there was no columns in block before executing a function, the result has size 0.
            /// Change the size to 1.

            if (column->empty())
                column = column->cloneResized(1);

            node.column = std::move(column);
        }
    }

    if (result_name.empty())
    {
        result_name = function_base->getName() + "(";
        for (size_t i = 0; i < num_arguments; ++i)
        {
            if (i)
                result_name += ", ";
            result_name += node.children[i]->result_name;
        }
        result_name += ")";
    }

    node.result_name = std::move(result_name);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addPlaceholder(std::string name, DataTypePtr type)
{
    Node node;
    node.type = ActionType::PLACEHOLDER;
    node.result_type = std::move(type);
    node.result_name = std::move(name);
    node.column = node.result_type->createColumn();

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::findInOutputs(const std::string & name) const
{
    if (const auto * node = tryFindInOutputs(name))
        return *node;

    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier: '{}'", name);
}

const ActionsDAG::Node * ActionsDAG::tryFindInOutputs(const std::string & name) const
{
    for (const auto & node : outputs)
        if (node->result_name == name)
            return node;

    return nullptr;
}

ActionsDAG::NodeRawConstPtrs ActionsDAG::findInOutputs(const Names & names) const
{
    NodeRawConstPtrs required_nodes;
    required_nodes.reserve(names.size());

    std::unordered_map<std::string_view, const Node *> names_map;
    for (const auto * node : outputs)
        names_map[node->result_name] = node;

    for (const auto & name : names)
    {
        auto it = names_map.find(name);
        if (it == names_map.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", name, dumpDAG());

        required_nodes.push_back(it->second);
    }

    return required_nodes;
}

ActionsDAG::SplitPossibleOutputNamesResult ActionsDAG::splitPossibleOutputNames(NameMultiSet possible_output_names) const
{
    SplitPossibleOutputNamesResult result;
    for (const auto * output : outputs)
    {
        if (auto it = possible_output_names.find(output->result_name); it != possible_output_names.end())
        {
            auto extracted_node = possible_output_names.extract(it);
            result.output_names.insert(std::move(extracted_node.value()));
        }
    }
    result.not_output_names.reserve(possible_output_names.size());
    result.not_output_names.assign(std::make_move_iterator(possible_output_names.begin()), std::make_move_iterator(possible_output_names.end()));
    return result;
}

void ActionsDAG::addOrReplaceInOutputs(const Node & node)
{
    bool replaced = false;
    for (auto & output_node : outputs)
    {
        if (output_node->result_name == node.result_name)
        {
            output_node = &node;
            replaced = true;
        }
    }

    if (!replaced)
        outputs.push_back(&node);
}

ActionsDAG::NodeRawConstPtrs ActionsDAG::getNodesPointers() const
{
    NodeRawConstPtrs result;
    result.reserve(nodes.size());
    for (const auto & node : nodes)
        result.push_back(&node);
    return result;
}

NamesAndTypesList ActionsDAG::getRequiredColumns() const
{
    NamesAndTypesList result;
    for (const auto & input_node : inputs)
        result.emplace_back(input_node->result_name, input_node->result_type);

    return result;
}

Names ActionsDAG::getRequiredColumnsNames() const
{
    Names result;
    result.reserve(inputs.size());

    for (const auto & input_node : inputs)
        result.emplace_back(input_node->result_name);

    return result;
}

ColumnsWithTypeAndName ActionsDAG::getResultColumns() const
{
    ColumnsWithTypeAndName result;
    result.reserve(outputs.size());

    for (const auto & node : outputs)
        result.emplace_back(node->column, node->result_type, node->result_name);

    return result;
}

NamesAndTypesList ActionsDAG::getNamesAndTypesList() const
{
    NamesAndTypesList result;
    for (const auto & node : outputs)
        result.emplace_back(node->result_name, node->result_type);

    return result;
}

Names ActionsDAG::getNames() const
{
    Names names;
    names.reserve(outputs.size());

    for (const auto & node : outputs)
        names.emplace_back(node->result_name);

    return names;
}

std::string ActionsDAG::dumpNames() const
{
    WriteBufferFromOwnString out;
    for (auto it = nodes.begin(); it != nodes.end(); ++it)
    {
        if (it != nodes.begin())
            out << ", ";
        out << it->result_name;
    }
    return out.str();
}

bool ActionsDAG::removeUnusedActions(const NameSet & required_names, bool allow_remove_inputs, bool allow_constant_folding)
{
    NodeRawConstPtrs required_nodes;
    required_nodes.reserve(required_names.size());

    NameSet added;
    for (const auto & node : outputs)
    {
        if (required_names.contains(node->result_name) && !added.contains(node->result_name))
        {
            required_nodes.push_back(node);
            added.insert(node->result_name);
        }
    }

    if (added.size() < required_names.size())
    {
        for (const auto & name : required_names)
            if (!added.contains(name))
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                                "Unknown column: {}, there are only columns {}", name, dumpNames());
    }

    outputs.swap(required_nodes);

    if (removeUnusedActions(allow_remove_inputs, allow_constant_folding))
        return true;

    if (required_nodes.size() != outputs.size())
        return true;

    for (auto old_it = required_nodes.begin(), new_it = outputs.begin(); new_it != outputs.end(); ++old_it, ++new_it)
        if (*old_it != *new_it)
            return true;

    return false;
}

bool ActionsDAG::removeUnusedActions(const Names & required_names, bool allow_remove_inputs, bool allow_constant_folding)
{
    auto required_nodes = findInOutputs(required_names);
    outputs.swap(required_nodes);
    if (removeUnusedActions(allow_remove_inputs, allow_constant_folding))
        return true;

    if (required_nodes.size() != outputs.size())
        return true;

    for (auto old_it = required_nodes.begin(), new_it = outputs.begin(); new_it != outputs.end(); ++old_it, ++new_it)
        if (*old_it != *new_it)
            return true;

    return false;
}

bool ActionsDAG::removeUnusedActions(bool allow_remove_inputs, bool allow_constant_folding)
{
    std::unordered_set<const Node *> used_inputs;
    if (!allow_remove_inputs)
    {
        for (const auto * input : inputs)
            used_inputs.insert(input);
    }
    return removeUnusedActions(used_inputs, allow_constant_folding);
}

bool ActionsDAG::removeUnusedActions(const std::unordered_set<const Node *> & used_inputs, bool allow_constant_folding)
{
    NodeRawConstPtrs roots;
    roots.reserve(outputs.size() + used_inputs.size());
    roots = outputs;

    for (auto & node : nodes)
    {
        /// We cannot remove arrayJoin because it changes the number of rows.
        if (node.type == ActionType::ARRAY_JOIN)
            roots.push_back(&node);

        if (node.type == ActionType::INPUT && used_inputs.contains(&node))
            roots.push_back(&node);
    }

    std::unordered_set<const Node *> required_nodes;
    std::unordered_set<const Node *> non_deterministic_nodes;

    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;

    enum class VisitStage { NonDeterministic, Required };

    for (auto stage : {VisitStage::NonDeterministic, VisitStage::Required})
    {
        required_nodes.clear();

        for (const auto * root : roots)
        {
            if (!required_nodes.contains(root))
            {
                required_nodes.insert(root);
                stack.push({.node = root});
            }

            while (!stack.empty())
            {
                auto & frame = stack.top();
                auto * node = const_cast<Node *>(frame.node);

                while (frame.next_child_to_visit < node->children.size())
                {
                    const auto * child = node->children[frame.next_child_to_visit];
                    ++frame.next_child_to_visit;

                    if (!required_nodes.contains(child))
                    {
                        required_nodes.insert(child);
                        stack.push({.node = child});
                        break;
                    }
                }

                if (stack.top().node != node)
                    continue;

                stack.pop();

                if (stage == VisitStage::Required)
                    continue;

                if (!node->isDeterministic())
                    non_deterministic_nodes.insert(node);
                else
                {
                    for (const auto * child : node->children)
                    {
                        if (non_deterministic_nodes.contains(child))
                        {
                            non_deterministic_nodes.insert(node);
                            break;
                        }
                    }
                }

                /// Constant folding.
                if (allow_constant_folding && !node->children.empty()
                    && node->column && isColumnConst(*node->column))
                {
                    node->type = ActionsDAG::ActionType::COLUMN;
                    node->children.clear();
                    node->is_deterministic_constant = !non_deterministic_nodes.contains(node);
                }
            }
        }
    }

    const auto erased_count = std::erase_if(nodes, [&](const Node & node) { return !required_nodes.contains(&node); });
    std::erase_if(inputs, [&](const Node * node) { return !required_nodes.contains(node); });

    return erased_count != 0;
}


void ActionsDAG::removeAliasesForFilter(const std::string & filter_name)
{
    const auto & filter_node = findInOutputs(filter_name);
    std::stack<Node *> stack;
    stack.push(const_cast<Node *>(&filter_node));

    std::unordered_set<const Node *> visited;
    visited.insert(stack.top());

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();
        for (auto & child : node->children)
        {
            while (child->type == ActionType::ALIAS)
                child = child->children.front();

            if (!visited.contains(child))
            {
                stack.push(const_cast<Node *>(child));
                visited.insert(child);
            }
        }
    }
}

ActionsDAG ActionsDAG::cloneSubDAG(const NodeRawConstPtrs & outputs, bool remove_aliases)
{
    std::unordered_map<const Node *, const Node *> copy_map;
    return cloneSubDAG(outputs, copy_map, remove_aliases);
}

ActionsDAG ActionsDAG::cloneSubDAG(const NodeRawConstPtrs & outputs, NodeMapping & copy_map, bool remove_aliases)
{
    ActionsDAG actions;

    struct Frame
    {
        const Node * node = nullptr;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;

    for (const auto * output : outputs)
    {
        if (copy_map.contains(output))
            continue;

        stack.push(Frame{output});
        while (!stack.empty())
        {
            auto & frame = stack.top();
            const auto & children = frame.node->children;
            while (frame.next_child < children.size() && copy_map.contains(children[frame.next_child]))
                ++frame.next_child;

            if (frame.next_child < children.size())
            {
                stack.push(Frame{children[frame.next_child]});
                continue;
            }

            auto & copy_node = copy_map[frame.node];

            if (remove_aliases && frame.node->type == ActionType::ALIAS)
                copy_node = copy_map[frame.node->children.front()];
            else
                copy_node = &actions.nodes.emplace_back(*frame.node);

            if (frame.node->type == ActionType::INPUT)
                actions.inputs.push_back(copy_node);

            stack.pop();
        }
    }

    for (auto & node : actions.nodes)
        for (auto & child : node.children)
            child = copy_map[child];

    for (const auto * output : outputs)
        actions.outputs.push_back(copy_map[output]);

    return actions;
}

static ColumnWithTypeAndName executeActionForPartialResult(
    const ActionsDAG::Node * node,
    ColumnsWithTypeAndName arguments,
    size_t input_rows_count,
    bool only_constant_arguments)
{
    ColumnWithTypeAndName res_column;
    res_column.type = node->result_type;
    res_column.name = node->result_name;

    switch (node->type)
    {
        case ActionsDAG::ActionType::FUNCTION:
        {
            try
            {
                if (only_constant_arguments)
                    res_column.column = node->function->execute(arguments, res_column.type, input_rows_count, true);
                else
                    res_column.column = node->function_base->getConstantResultForNonConstArguments(arguments, res_column.type);
            }
            catch (Exception & e)
            {
                std::string arguments_description;
                for (const auto & arg : arguments)
                {
                    if (!arguments_description.empty())
                        arguments_description += ", ";
                    arguments_description += arg.dumpStructure();
                }
                e.addMessage("while executing function {} on arguments {}", node->function->getName(), arguments_description);
                throw;
            }
            break;
        }

        case ActionsDAG::ActionType::ARRAY_JOIN:
        {
            auto key = arguments.at(0);
            key.column = key.column->convertToFullColumnIfConst();

            const auto * array = getArrayJoinColumnRawPtr(key.column);
            if (!array)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                                "ARRAY JOIN of not array nor map: {}", node->result_name);

            ColumnPtr data;
            if (input_rows_count < array->size())
                data = array->getDataInRange(0, input_rows_count);
            else
                data = array->getDataPtr();

            res_column.column = data;
            break;
        }

        case ActionsDAG::ActionType::COLUMN:
        {
            auto column = node->column;
            if (input_rows_count != column->size())
                column = column->cloneResized(input_rows_count);

            res_column.column = column;
            break;
        }

        case ActionsDAG::ActionType::ALIAS:
        {
            res_column.column = arguments.at(0).column;
            break;
        }

        case ActionsDAG::ActionType::INPUT:
        {
            break;
        }

        case ActionsDAG::ActionType::PLACEHOLDER:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to execute PLACEHOLDER action");
        }
    }

    return res_column;
}

Block ActionsDAG::updateHeader(const Block & header) const
{
    IntermediateExecutionResult node_to_column;
    std::set<size_t> pos_to_remove;

    {
        using inline_vector = absl::InlinedVector<size_t, 7>; // 64B, holding max 7 size_t elements inlined
        absl::flat_hash_map<std::string_view, inline_vector> input_positions;

        /// We insert from last to first in the inlinedVector so it's easier to pop_back matches later
        for (size_t pos = inputs.size(); pos != 0; pos--)
            input_positions[inputs[pos - 1]->result_name].emplace_back(pos - 1);

        for (size_t pos = 0; pos < header.columns(); ++pos)
        {
            const auto & col = header.getByPosition(pos);
            auto it = input_positions.find(col.name);
            if (it != input_positions.end() && !it->second.empty())
            {
                pos_to_remove.insert(pos);

                auto & v = it->second;
                node_to_column[inputs[v.back()]] = col;
                v.pop_back();
            }
        }
    }

    ColumnsWithTypeAndName result_columns;
    try
    {
        result_columns = evaluatePartialResult(node_to_column, outputs, /* input_rows_count= */ 0, { .throw_on_error = true });
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK)
            e.addMessage("in block {}", header.dumpStructure());

        throw;
    }

    Block res;
    res.reserve(result_columns.size());
    for (auto & col : result_columns)
        res.insert(std::move(col));

    res.reserve(header.columns() - pos_to_remove.size());
    for (size_t i = 0; i < header.columns(); i++)
    {
        if (!pos_to_remove.contains(i))
            res.insert(header.data[i]);
    }

    return res;
}

ColumnsWithTypeAndName ActionsDAG::evaluatePartialResult(
    IntermediateExecutionResult & node_to_column,
    const NodeRawConstPtrs & outputs,
    size_t input_rows_count,
    PartialEvaluationParameters params
)
{
    chassert(input_rows_count <= 1); /// evaluatePartialResult() should be used only to evaluate headers or constants

    ColumnsWithTypeAndName result_columns;
    result_columns.reserve(outputs.size());

    struct Frame
    {
        const Node * node = nullptr;
        size_t next_child = 0;
    };

    {
        for (const auto * output_node : outputs)
        {
            if (!node_to_column.contains(output_node))
            {
                std::stack<Frame> stack;
                stack.push({.node = output_node});

                while (!stack.empty())
                {
                    auto & frame = stack.top();
                    const auto * node = frame.node;

                    while (frame.next_child < node->children.size())
                    {
                        const auto * child = node->children[frame.next_child];
                        ++frame.next_child;
                        if (!node_to_column.contains(child))
                        {
                            stack.push({.node = child});
                            break;
                        }
                    }

                    if (stack.top().node != node)
                        continue;

                    stack.pop();

                    if (node->type == ActionsDAG::ActionType::PLACEHOLDER)
                    {
                        /// Maybe move to executeActionForPartialResult
                        node_to_column[node] = ColumnWithTypeAndName(
                            node->column->cloneResized(input_rows_count),
                            node->result_type,
                            node->result_name);
                        continue;
                    }

                    ColumnsWithTypeAndName arguments(node->children.size());
                    bool has_all_arguments = true;
                    for (size_t i = 0; i < arguments.size(); ++i)
                    {
                        const auto * child = node->children[i];
                        if (auto it = node_to_column.find(child); it != node_to_column.end())
                            arguments[i] = it->second;
                        else
                            arguments[i] = ColumnWithTypeAndName{nullptr, child->result_type, child->result_name};

                        if (!arguments[i].column)
                            has_all_arguments = false;
                        if (!has_all_arguments && params.throw_on_error)
                            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Not found column {}", child->result_name);
                    }

                    if (node->type == ActionsDAG::ActionType::INPUT && params.throw_on_error)
                        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                                        "Not found column {}",
                                        node->result_name);

                    if (node->type != ActionsDAG::ActionType::INPUT)
                    {
                        if (node->type == ActionType::FUNCTION && params.skip_materialize && node->function_base->getName() == "materialize")
                        {
                            node_to_column[node] = arguments.at(0);
                        }

                        if (has_all_arguments)
                            node_to_column[node] = executeActionForPartialResult(node, std::move(arguments), input_rows_count, true);
                        else if (params.allow_unknown_function_arguments)
                            node_to_column[node] = executeActionForPartialResult(node, std::move(arguments), input_rows_count, false);
                    }
                }
            }

            auto it = node_to_column.find(output_node);
            if (it != node_to_column.end())
                result_columns.push_back(node_to_column[output_node]);
            else
                result_columns.emplace_back(nullptr, output_node->result_type, output_node->result_name);
        }
    }

    return result_columns;
}

NameSet ActionsDAG::foldActionsByProjection(
    const NameSet & required_columns, const Block & projection_block_for_keys, const String & predicate_column_name, bool add_missing_keys)
{
    std::unordered_set<const Node *> visited_nodes;
    std::unordered_set<std::string_view> visited_output_nodes_names;
    std::stack<Node *> stack;

    /// Record all needed output nodes to start folding.
    for (const auto & output_node : outputs)
    {
        if (required_columns.contains(output_node->result_name) || output_node->result_name == predicate_column_name)
        {
            visited_nodes.insert(output_node);
            visited_output_nodes_names.insert(output_node->result_name);
            stack.push(const_cast<Node *>(output_node));
        }
    }

    /// If some required columns are not in any output node, try searching from all projection key
    /// columns. If still missing, return empty set which means current projection fails to match
    /// (missing columns).
    if (add_missing_keys)
    {
        for (const auto & column : required_columns)
        {
            if (!visited_output_nodes_names.contains(column))
            {
                if (const ColumnWithTypeAndName * column_with_type_name = projection_block_for_keys.findByName(column))
                {
                    const auto * node = &addInput(*column_with_type_name);
                    visited_nodes.insert(node);
                    outputs.push_back(node);
                    visited_output_nodes_names.insert(column);
                }
                else
                {
                    // Missing column
                    return {};
                }
            }
        }
    }

    /// Traverse the DAG from root to leaf. Substitute any matched node with columns in projection_block_for_keys.
    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (const ColumnWithTypeAndName * column_with_type_name = projection_block_for_keys.findByName(node->result_name))
        {
            if (node->type != ActionsDAG::ActionType::INPUT)
            {
                /// Projection folding.
                node->type = ActionsDAG::ActionType::INPUT;
                node->result_type = column_with_type_name->type;
                node->result_name = column_with_type_name->name;
                node->children.clear();
                inputs.push_back(node);
            }
        }

        for (const auto * child : node->children)
        {
            if (!visited_nodes.contains(child))
            {
                stack.push(const_cast<Node *>(child));
                visited_nodes.insert(child);
            }
        }
    }

    /// Clean up unused nodes after folding.
    std::erase_if(inputs, [&](const Node * node) { return !visited_nodes.contains(node); });
    std::erase_if(outputs, [&](const Node * node) { return !visited_output_nodes_names.contains(node->result_name); });
    nodes.remove_if([&](const Node & node) { return !visited_nodes.contains(&node); });

    /// Calculate the required columns after folding.
    NameSet next_required_columns;
    for (const auto & input_node : inputs)
        next_required_columns.insert(input_node->result_name);

    return next_required_columns;
}


ActionsDAG ActionsDAG::foldActionsByProjection(const std::unordered_map<const Node *, const Node *> & new_inputs, const NodeRawConstPtrs & required_outputs)
{
    ActionsDAG dag;
    std::unordered_map<const Node *, const Node *> inputs_mapping;
    std::unordered_map<const Node *, const Node *> mapping;
    struct Frame
    {
        const Node * node;
        size_t next_child = 0;
    };

    std::vector<Frame> stack;
    for (const auto * output : required_outputs)
    {
        if (mapping.contains(output))
            continue;

        stack.push_back({.node = output});
        while (!stack.empty())
        {
            auto & frame = stack.back();

            if (frame.next_child == 0)
            {
                auto it = new_inputs.find(frame.node);
                if (it != new_inputs.end())
                {
                    const auto & [new_input, rename] = *it;

                    auto & node = mapping[frame.node];

                    if (!node)
                    {
                        /// It is possible to have a few aliases on the same column.
                        /// We may want to replace all the aliases,
                        /// in this case they should have a single input as a child.
                        auto & mapped_input = inputs_mapping[rename];

                        if (!mapped_input)
                        {
                            bool should_rename = new_input->result_name != rename->result_name;
                            const auto & input_name = should_rename ? rename->result_name : new_input->result_name;
                            mapped_input = &dag.addInput(input_name, new_input->result_type);
                            if (should_rename)
                                mapped_input = &dag.addAlias(*mapped_input, new_input->result_name);
                        }

                        node = mapped_input;
                    }

                    stack.pop_back();
                    continue;
                }
            }

            const auto & children = frame.node->children;

            while (frame.next_child < children.size() && !mapping.emplace(children[frame.next_child], nullptr).second)
                ++frame.next_child;

            if (frame.next_child < children.size())
            {
                const auto * child = children[frame.next_child];
                ++frame.next_child;
                stack.push_back({.node = child});
                continue;
            }

            if (frame.node->type == ActionType::INPUT)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cannot fold actions for projection. Node {} requires input {} which does not belong to projection",
                    stack.front().node->result_name, frame.node->result_name);

            auto & node = dag.nodes.emplace_back(*frame.node);
            for (auto & child : node.children)
                child = mapping[child];

            mapping[frame.node] = &node;
            stack.pop_back();
        }
    }

    for (const auto * output : required_outputs)
    {
        /// Keep the names for outputs.
        /// Add an alias if the mapped node has a different result name.
        const auto * mapped_output = mapping[output];
        if (output->result_name != mapped_output->result_name)
            mapped_output = &dag.addAlias(*mapped_output, output->result_name);
        dag.outputs.push_back(mapped_output);
    }

    return dag;
}

void ActionsDAG::addAliases(const NamesWithAliases & aliases)
{
    std::unordered_map<std::string_view, size_t> names_map;
    size_t output_nodes_size = outputs.size();

    for (size_t i = 0; i < output_nodes_size; ++i)
        names_map[outputs[i]->result_name] = i;

    size_t aliases_size = aliases.size();

    NodeRawConstPtrs required_nodes;
    required_nodes.reserve(aliases_size);

    for (const auto & item : aliases)
    {
        auto it = names_map.find(item.first);
        if (it == names_map.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", item.first, dumpNames());

        required_nodes.push_back(outputs[it->second]);
    }

    for (size_t i = 0; i < aliases_size; ++i)
    {
        const auto & item = aliases[i];
        const auto * child = required_nodes[i];

        if (!item.second.empty() && item.first != item.second)
        {
            Node node;
            node.type = ActionType::ALIAS;
            node.result_type = child->result_type;
            node.result_name = item.second;
            node.column = child->column;
            node.children.emplace_back(child);

            child = &addNode(std::move(node));
        }

        auto it = names_map.find(child->result_name);
        if (it == names_map.end())
        {
            names_map[child->result_name] = outputs.size();
            outputs.push_back(child);
        }
        else
            outputs[it->second] = child;
    }
}

void ActionsDAG::project(const NamesWithAliases & projection)
{
    std::unordered_map<std::string_view, const Node *> names_map;
    for (const auto * output_node : outputs)
        names_map.emplace(output_node->result_name, output_node);

    outputs.clear();

    size_t projection_size = projection.size();
    outputs.reserve(projection_size);

    for (const auto & item : projection)
    {
        auto it = names_map.find(item.first);
        if (it == names_map.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", item.first, dumpNames());

        outputs.push_back(it->second);
    }

    for (size_t i = 0; i < projection_size; ++i)
    {
        const auto & item = projection[i];
        auto & child = outputs[i];

        if (!item.second.empty() && item.first != item.second)
        {
            Node node;
            node.type = ActionType::ALIAS;
            node.result_type = child->result_type;
            node.result_name = item.second;
            node.column = child->column;
            node.children.emplace_back(child);

            child = &addNode(std::move(node));
        }
    }

    removeUnusedActions();
}

void ActionsDAG::appendInputsForUnusedColumns(const Block & sample_block)
{
    std::unordered_map<std::string_view, std::list<size_t>> names_map;
    size_t num_columns = sample_block.columns();
    for (size_t pos = 0; pos < num_columns; ++pos)
        names_map[sample_block.getByPosition(pos).name].push_back(pos);

    for (const auto * input : inputs)
    {
        auto & positions = names_map[input->result_name];
        if (positions.empty())
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                            "Not found column {} in block {}", input->result_name, sample_block.dumpStructure());

        positions.pop_front();
    }

    for (const auto & [_, positions] : names_map)
    {
        for (auto pos : positions)
        {
            const auto & col = sample_block.getByPosition(pos);
            addInput(col.name, col.type);
        }
    }
}

bool ActionsDAG::tryRestoreColumn(const std::string & column_name)
{
    for (const auto * output_node : outputs)
        if (output_node->result_name == column_name)
            return true;

    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it)
    {
        auto & node = *it;
        if (node.result_name == column_name)
        {
            outputs.push_back(&node);
            return true;
        }
    }

    return false;
}

bool ActionsDAG::removeUnusedResult(const std::string & column_name)
{
    /// Find column in output nodes and remove.
    const Node * col;
    {
        auto it = outputs.begin();
        for (; it != outputs.end(); ++it)
            if ((*it)->result_name == column_name)
                break;

        if (it == outputs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found result {} in ActionsDAG\n{}", column_name, dumpDAG());

        col = *it;
        outputs.erase(it);
    }

    /// Check if column is in input.
    auto it = inputs.begin();
    for (; it != inputs.end(); ++it)
        if (*it == col)
            break;

    /// Check column has no dependent.
    for (const auto & node : nodes)
        for (const auto * child : node.children)
            if (col == child)
                return false;

    /// Do not remove input if it was mentioned in output nodes several times.
    for (const auto * output_node : outputs)
        if (col == output_node)
            return false;

    /// Remove from nodes and inputs.
    for (auto jt = nodes.begin(); jt != nodes.end(); ++jt)
    {
        if (&(*jt) == col)
        {
            nodes.erase(jt);
            break;
        }
    }

    if (it != inputs.end())
        inputs.erase(it);
    return true;
}

void ActionsDAG::removeFromOutputs(const std::string & node_name)
{
    auto it = std::find_if(
        outputs.begin(),
        outputs.end(),
        [&node_name](const Node * node)
        {
            return node->result_name == node_name;
        });

    if (it == outputs.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found node with {} in the outputs of ActionsDAG\n{}", node_name, dumpDAG());
    outputs.erase(it);

    removeUnusedActions(/*allow_remove_inputs=*/false);
}

ActionsDAG ActionsDAG::clone() const
{
    std::unordered_map<const Node *, const Node *> old_to_new_nodes;
    return clone(old_to_new_nodes);
}

ActionsDAG ActionsDAG::clone(std::unordered_map<const Node *, const Node *> & old_to_new_nodes) const
{
    ActionsDAG actions;

    for (const auto & node : nodes)
    {
        auto & copy_node = actions.nodes.emplace_back(node);
        old_to_new_nodes[&node] = &copy_node;
    }

    for (auto & node : actions.nodes)
        for (auto & child : node.children)
            child = old_to_new_nodes[child];

    for (const auto & output_node : outputs)
        actions.outputs.push_back(old_to_new_nodes[output_node]);

    for (const auto & input_node : inputs)
        actions.inputs.push_back(old_to_new_nodes[input_node]);

    return actions;
}

#if USE_EMBEDDED_COMPILER
void ActionsDAG::compileExpressions(size_t min_count_to_compile_expression, const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes)
{
    compileFunctions(min_count_to_compile_expression, lazy_executed_nodes);
    removeUnusedActions(/*allow_remove_inputs = */ false);
}
#endif

std::string ActionsDAG::dumpDAG() const
{
    std::unordered_map<const Node *, size_t> map;
    for (const auto & node : nodes)
    {
        size_t idx = map.size();
        map[&node] = idx;
    }

    WriteBufferFromOwnString out;
    for (const auto & node : nodes)
    {
        out << map[&node] << " : ";
        switch (node.type)
        {
            case ActionsDAG::ActionType::COLUMN:
                out << "COLUMN ";
                break;

            case ActionsDAG::ActionType::ALIAS:
                out << "ALIAS ";
                break;

            case ActionsDAG::ActionType::FUNCTION:
                out << "FUNCTION ";
                break;

            case ActionsDAG::ActionType::ARRAY_JOIN:
                out << "ARRAY JOIN ";
                break;

            case ActionsDAG::ActionType::INPUT:
                out << "INPUT ";
                break;

            case ActionsDAG::ActionType::PLACEHOLDER:
                out << "PLACEHOLDER ";
                break;
        }

        out << "(";
        for (size_t i = 0; i < node.children.size(); ++i)
        {
            if (i)
                out << ", ";
            out << map[node.children[i]];
        }
        out << ")";

        out << " " << (node.column ? node.column->getName() : "(no column)");
        out << " " << (node.result_type ? node.result_type->getName() : "(no type)");
        out << " " << (!node.result_name.empty() ? node.result_name : "(no name)");

        if (node.function_base)
            out << " [" << node.function_base->getName() << "]";

        if (node.is_function_compiled)
            out << " [compiled]";

        out << "\n";
    }

    out << "Output nodes:";
    for (const auto * node : outputs)
        out << ' ' << map[node];
    out << '\n';

    return out.str();
}

bool ActionsDAG::hasCorrelatedColumns() const noexcept
{
    for (const auto & node : nodes)
        if (node.type == ActionType::PLACEHOLDER)
            return true;

    return false;
}

bool ActionsDAG::hasArrayJoin() const noexcept
{
    for (const auto & node : nodes)
        if (node.type == ActionType::ARRAY_JOIN)
            return true;

    return false;
}

bool ActionsDAG::hasStatefulFunctions() const
{
    for (const auto & node : nodes)
        if (node.type == ActionType::FUNCTION && node.function_base->isStateful())
            return true;

    return false;
}

bool ActionsDAG::trivial() const noexcept
{
    for (const auto & node : nodes)
        if (node.type == ActionType::FUNCTION || node.type == ActionType::ARRAY_JOIN)
            return false;

    return true;
}

void ActionsDAG::assertDeterministic() const
{
    for (const auto & node : nodes)
        if (!node.isDeterministic())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expression must be deterministic but it contains non-deterministic part `{}`", node.result_name);
}

bool ActionsDAG::hasNonDeterministic() const
{
    for (const auto & node : nodes)
        if (!node.isDeterministic())
            return true;
    return false;
}

void ActionsDAG::decorrelate() noexcept
{
    for (auto & node : nodes)
    {
        if (node.type == ActionType::PLACEHOLDER)
        {
            node.type = ActionType::INPUT;
            node.column = nullptr;
            inputs.emplace_back(&node);
        }
    }
}

void ActionsDAG::addMaterializingOutputActions(bool materialize_sparse)
{
    for (auto & output_node : outputs)
        output_node = &materializeNode(*output_node, materialize_sparse);
}

const ActionsDAG::Node & ActionsDAG::materializeNodeWithoutRename(const Node & node, bool materialize_sparse)
{
    FunctionPtr func_materialize;
    if (materialize_sparse)
        func_materialize = std::make_shared<FunctionMaterialize<true>>();
    else
        func_materialize = std::make_shared<FunctionMaterialize<false>>();

    FunctionOverloadResolverPtr func_builder_materialize = std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(func_materialize));

    return addFunction(func_builder_materialize, {&node}, {});
}

const ActionsDAG::Node & ActionsDAG::materializeNode(const Node & node, bool materialize_sparse)
{
    const auto & func = materializeNodeWithoutRename(node, materialize_sparse);
    return addAlias(func, node.result_name);
}

ActionsDAG ActionsDAG::makeConvertingActions(
    const ColumnsWithTypeAndName & source,
    const ColumnsWithTypeAndName & result,
    MatchColumnsMode mode,
    ContextPtr context,
    bool ignore_constant_values,
    bool add_cast_columns,
    NameToNameMap * new_names,
    NameSet * columns_contain_compiled_function)
{
    size_t num_input_columns = source.size();
    size_t num_result_columns = result.size();

    if (mode == MatchColumnsMode::Position && num_input_columns != num_result_columns)
        throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH, "Number of columns doesn't match (source: {} and result: {})", num_input_columns, num_result_columns);

    if (add_cast_columns && mode != MatchColumnsMode::Name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Converting with add_cast_columns supported only for MatchColumnsMode::Name");

    ActionsDAG actions_dag(source);
    NodeRawConstPtrs projection(num_result_columns);

    FunctionOverloadResolverPtr func_builder_materialize = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMaterialize<false>>());

    std::unordered_map<std::string_view, std::list<size_t>> inputs;
    if (mode == MatchColumnsMode::Name)
    {
        size_t input_nodes_size = actions_dag.inputs.size();
        for (size_t pos = 0; pos < input_nodes_size; ++pos)
            inputs[actions_dag.inputs[pos]->result_name].push_back(pos);
    }

    for (size_t result_col_num = 0; result_col_num < num_result_columns; ++result_col_num)
    {
        const auto & res_elem = result[result_col_num];
        const Node * src_node = nullptr;
        const Node * dst_node = nullptr;

        switch (mode)
        {
            case MatchColumnsMode::Position:
            {
                src_node = dst_node = actions_dag.inputs[result_col_num];
                break;
            }

            case MatchColumnsMode::Name:
            {
                auto & input = inputs[res_elem.name];
                if (input.empty())
                {
                    const auto * res_const = typeid_cast<const ColumnConst *>(res_elem.column.get());
                    if (ignore_constant_values && res_const)
                        src_node = dst_node = &actions_dag.addColumn(res_elem);
                    else
                        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                                        "Cannot find column `{}` in source stream, there are only columns: [{}]",
                                        res_elem.name, Block(source).dumpNames());
                }
                else
                {
                    src_node = dst_node = actions_dag.inputs[input.front()];
                    input.pop_front();
                }
                break;
            }
        }

        /// Check constants.
        if (const auto * res_const = typeid_cast<const ColumnConst *>(res_elem.column.get()))
        {
            if (const auto * src_const = typeid_cast<const ColumnConst *>(dst_node->column.get()))
            {
                if (ignore_constant_values)
                    dst_node = &actions_dag.addColumn(res_elem);
                else if (res_const->getField() != src_const->getField())
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Cannot convert column `{}` because it is constant but values of constants are different in source and result",
                        res_elem.name);
            }
            else if (columns_contain_compiled_function && columns_contain_compiled_function->contains(res_elem.name))
            {
                /// It may happen when JIT compilation is enabled that source column is constant and destination column is not constant.
                /// e.g. expression "and(equals(materialize(null::Nullable(UInt64)), null::Nullable(UInt64)), equals(null::Nullable(UInt64), null::Nullable(UInt64)))"
                /// compiled expression is "and(equals(input: Nullable(UInt64), null), null). Partial evaluation of the compiled expression isn't able to infer that the result column is constant.
                /// It causes inconsistency between pipeline header(source column is not constant) and output header of ExpressionStep(destination column is constant).
                /// So we need to convert non-constant column to constant column under this condition.
                dst_node = &actions_dag.addColumn(res_elem);
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot convert column `{}` because it is non constant in source stream but must be constant in result",
                    res_elem.name);
        }

        /// Add CAST function to convert into result type if needed.
        if (!res_elem.type->equals(*dst_node->result_type))
        {
            ColumnWithTypeAndName column;
            column.name = res_elem.type->getName();
            column.column = DataTypeString().createColumnConst(0, column.name);
            column.type = std::make_shared<DataTypeString>();

            const auto * right_arg = &actions_dag.addColumn(std::move(column));
            const auto * left_arg = dst_node;

            CastDiagnostic diagnostic = {dst_node->result_name, res_elem.name};
            ColumnWithTypeAndName left_column{nullptr, dst_node->result_type, {}};
            auto func_base_cast = createInternalCast(std::move(left_column), res_elem.type, CastType::nonAccurate, std::move(diagnostic), context);

            NodeRawConstPtrs children = { left_arg, right_arg };
            dst_node = &actions_dag.addFunction(func_base_cast, std::move(children), {});
        }

        if (dst_node->column && isColumnConst(*dst_node->column) && !(res_elem.column && isColumnConst(*res_elem.column)))
        {
            NodeRawConstPtrs children = {dst_node};
            dst_node = &actions_dag.addFunction(func_builder_materialize, std::move(children), {});
        }

        if (dst_node->result_name != res_elem.name)
        {
            if (add_cast_columns)
            {
                if (inputs.contains(dst_node->result_name))
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot convert column `{}` to `{}` because other column have same name",
                                    res_elem.name, dst_node->result_name);
                if (new_names)
                    new_names->emplace(res_elem.name, dst_node->result_name);

                /// Leave current column on same place, add converted to back
                projection[result_col_num] = src_node;
                projection.push_back(dst_node);
            }
            else
            {
                dst_node = &actions_dag.addAlias(*dst_node, res_elem.name);
                projection[result_col_num] = dst_node;
            }
        }
        else
        {
            projection[result_col_num] = dst_node;
        }
    }

    actions_dag.outputs.swap(projection);
    actions_dag.removeUnusedActions(false);

    return actions_dag;
}

ActionsDAG ActionsDAG::makeAddingColumnActions(ColumnWithTypeAndName column)
{
    ActionsDAG adding_column_action;
    FunctionOverloadResolverPtr func_builder_materialize
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMaterialize<true>>());

    auto column_name = column.name;
    const auto * column_node = &adding_column_action.addColumn(std::move(column));
    NodeRawConstPtrs inputs = {column_node};
    const auto & function_node = adding_column_action.addFunction(func_builder_materialize, std::move(inputs), {});
    const auto & alias_node = adding_column_action.addAlias(function_node, std::move(column_name));

    adding_column_action.outputs.push_back(&alias_node);
    return adding_column_action;
}

ActionsDAG ActionsDAG::merge(ActionsDAG && first, ActionsDAG && second)
{
    first.mergeInplace(std::move(second));

    /// Some actions could become unused. Do not drop inputs to preserve the header.
    first.removeUnusedActions(false);

    return std::move(first);
}

void ActionsDAG::mergeInplace(ActionsDAG && second)
{
    std::unordered_map<const Node *, const Node *> inputs_map;
    mergeInplace(std::move(second), inputs_map, false);
}

void ActionsDAG::mergeInplace(ActionsDAG && second, std::unordered_map<const Node *, const Node *> & inputs_map, bool remove_dangling_inputs)
{
    auto & first = *this;
    /// first: x (1), x (2), y ==> x (2), z, x (3)
    /// second: x (1), x (2), x (3) ==> x (3), x (2), x (1)
    /// merge: x (1), x (2), x (3), y =(first)=> x (2), z, x (4), x (3) =(second)=> x (3), x (4), x (2), z

    /// Will store merged result in `first`.

    /// This map contains nodes which should be removed from `first` outputs, cause they are used as inputs for `second`.
    /// The second element is the number of removes (cause one node may be repeated several times in result).
    std::unordered_map<const Node *, size_t> removed_first_result;
    /// Map inputs of `second` to nodes of `first`.

    /// Update inputs list.
    {
        /// Outputs may have multiple columns with same name. They also may be used by `second`. Order is important.
        std::unordered_map<std::string_view, std::list<const Node *>> first_result;
        for (const auto & output_node : first.outputs)
            first_result[output_node->result_name].push_back(output_node);

        for (const auto & input_node : second.inputs)
        {
            auto it = first_result.find(input_node->result_name);
            if (it == first_result.end() || it->second.empty())
            {
                first.inputs.push_back(input_node);
            }
            else
            {
                inputs_map[input_node] = it->second.front();
                removed_first_result[it->second.front()] += 1;
                it->second.pop_front();
            }
        }
    }

    /// Replace inputs from `second` to nodes from `first` result.
    for (auto & node : second.nodes)
    {
        for (auto & child : node.children)
        {
            if (child->type == ActionType::INPUT)
            {
                auto it = inputs_map.find(child);
                if (it != inputs_map.end())
                    child = it->second;
            }
        }
    }

    for (auto & output_node : second.outputs)
    {
        if (output_node->type == ActionType::INPUT)
        {
            auto it = inputs_map.find(output_node);
            if (it != inputs_map.end())
                output_node = it->second;
        }
    }

    {
        /// Add not removed result from first actions.
        for (const auto * output_node : first.outputs)
        {
            auto it = removed_first_result.find(output_node);
            if (it != removed_first_result.end() && it->second > 0)
                --it->second;
            else
                second.outputs.push_back(output_node);
        }

        first.outputs.swap(second.outputs);
    }

    if (remove_dangling_inputs)
        std::erase_if(second.nodes, [&inputs_map](const auto & node)
        {
            auto mapit = inputs_map.find(&node);
            return mapit != inputs_map.end() && mapit->second != &node;
        });

    first.nodes.splice(first.nodes.end(), std::move(second.nodes));
}

void ActionsDAG::mergeNodes(ActionsDAG && second, NodeRawConstPtrs * out_outputs)
{
    std::unordered_map<std::string, const ActionsDAG::Node *> node_name_to_node;
    for (auto & node : nodes)
        node_name_to_node.emplace(node.result_name, &node);

    struct Frame
    {
        ActionsDAG::Node * node = nullptr;
        bool visited_children = false;
    };

    std::unordered_map<const ActionsDAG::Node *, ActionsDAG::Node *> const_node_to_node;
    for (auto & node : second.nodes)
        const_node_to_node.emplace(&node, &node);

    std::vector<Frame> nodes_to_process;
    nodes_to_process.reserve(second.getOutputs().size());
    for (auto & node : second.getOutputs())
        nodes_to_process.push_back({const_node_to_node.at(node), false /*visited_children*/});

    std::unordered_set<const ActionsDAG::Node *> nodes_to_move_from_second_dag;

    while (!nodes_to_process.empty())
    {
        auto & node_to_process = nodes_to_process.back();
        auto * node = node_to_process.node;

        auto node_it = node_name_to_node.find(node->result_name);
        if (node_it != node_name_to_node.end())
        {
            nodes_to_process.pop_back();
            continue;
        }

        if (!node_to_process.visited_children)
        {
            node_to_process.visited_children = true;

            for (auto & child : node->children)
                nodes_to_process.push_back({const_node_to_node.at(child), false /*visited_children*/});

            /// If node has children process them first
            if (!node->children.empty())
                continue;
        }

        for (auto & child : node->children)
            child = node_name_to_node.at(child->result_name);

        node_name_to_node.emplace(node->result_name, node);
        nodes_to_move_from_second_dag.insert(node);

        nodes_to_process.pop_back();
    }

    if (out_outputs)
    {
        for (auto & node : second.getOutputs())
            out_outputs->push_back(node_name_to_node.at(node->result_name));
    }

    if (nodes_to_move_from_second_dag.empty())
        return;

    auto second_nodes_end = second.nodes.end();
    for (auto second_node_it = second.nodes.begin(); second_node_it != second_nodes_end;)
    {
        if (!nodes_to_move_from_second_dag.contains(&(*second_node_it)))
        {
            ++second_node_it;
            continue;
        }

        auto node_to_move_it = second_node_it;
        ++second_node_it;
        nodes.splice(nodes.end(), second.nodes, node_to_move_it);

        if (node_to_move_it->type == ActionType::INPUT)
            inputs.push_back(&(*node_to_move_it));
    }
}

void ActionsDAG::unite(ActionsDAG && second)
{
    nodes.splice(nodes.end(), std::move(second.nodes));
    inputs.append_range(second.inputs);
    outputs.append_range(second.outputs);
}

ActionsDAG::SplitResult ActionsDAG::split(std::unordered_set<const Node *> split_nodes, bool create_split_nodes_mapping, bool avoid_duplicate_inputs) const
{
    /// Split DAG into two parts.
    /// (first_nodes, first_outputs) is a part which will have split_list in result.
    /// (second_nodes, second_outputs) is a part which will have same outputs as current actions.
    Nodes first_nodes;
    NodeRawConstPtrs first_outputs;

    Nodes second_nodes;
    NodeRawConstPtrs second_outputs;

    /// List of nodes from current actions which are not inputs, but will be in second part.
    NodeRawConstPtrs new_inputs;

    /// Avoid new inputs to have the same name as existing inputs.
    /// It's allowed for DAG but may break Block invariant 'columns with identical name must have identical structure'.
    std::unordered_set<std::string_view> duplicate_inputs;
    size_t duplicate_counter = 0;
    if (avoid_duplicate_inputs)
        for (const auto * input : inputs)
            duplicate_inputs.insert(input->result_name);

    struct Frame
    {
        const Node * node = nullptr;
        size_t next_child_to_visit = 0;
    };

    struct Data
    {
        bool needed_by_split_node = false;
        bool visited = false;
        bool used_in_result = false;

        /// Copies of node in one of the DAGs.
        /// For COLUMN and INPUT both copies may exist.
        Node * to_second = nullptr;
        Node * to_first = nullptr;
    };

    std::stack<Frame> stack;
    std::unordered_map<const Node *, Data> data;

    for (const auto & output_node : outputs)
        data[output_node].used_in_result = true;

    /// DFS. Decide if node is needed by split.
    for (const auto & node : nodes)
    {
        if (!split_nodes.contains(&node))
            continue;

        auto & cur_data = data[&node];
        if (cur_data.needed_by_split_node)
            continue;

        cur_data.needed_by_split_node = true;
        stack.push({.node = &node});

        while (!stack.empty())
        {
            const auto * cur_node = stack.top().node;
            stack.pop();

            for (const auto * child : cur_node->children)
            {
                auto & child_data = data[child];
                if (!child_data.needed_by_split_node)
                {
                    child_data.needed_by_split_node = true;
                    stack.push({.node = child});
                }
            }
        }
    }

    /// DFS. Move nodes to one of the DAGs.
    for (const auto & node : nodes)
    {
        if (!data[&node].visited)
            stack.push({.node = &node});

        while (!stack.empty())
        {
            /// Copying top frame is important, because we might access it after executing pop().
            auto cur = stack.top();
            auto & cur_data = data[cur.node];

            /// At first, visit all children.
            while (cur.next_child_to_visit < cur.node->children.size())
            {
                const auto * child = cur.node->children[cur.next_child_to_visit];
                auto & child_data = data[child];

                if (!child_data.visited)
                {
                    stack.push({.node = child});
                    break;
                }

                ++cur.next_child_to_visit;
            }

            /// Make a copy part.
            if (cur.next_child_to_visit == cur.node->children.size())
            {
                cur_data.visited = true;
                stack.pop();

                if (!cur_data.needed_by_split_node)
                {
                    auto & copy = second_nodes.emplace_back(*cur.node);
                    cur_data.to_second = &copy;

                    /// Replace children to newly created nodes.
                    for (auto & child : copy.children)
                    {
                        auto & child_data = data[child];

                        /// If children is not created, it may be from split part.
                        if (!child_data.to_second)
                        {
                            if (child->type == ActionType::COLUMN) /// Just create new node for COLUMN action.
                            {
                                child_data.to_second = &second_nodes.emplace_back(*child);
                            }
                            else
                            {
                                /// Node from first part is added as new input.
                                Node input_node;
                                input_node.type = ActionType::INPUT;
                                input_node.result_type = child->result_type;
                                input_node.result_name = child->result_name;
                                child_data.to_second = &second_nodes.emplace_back(std::move(input_node));

                                if (child->type != ActionType::INPUT)
                                    new_inputs.push_back(child);
                            }
                        }

                        child = child_data.to_second;
                    }
                }
                else
                {
                    auto & copy = first_nodes.emplace_back(*cur.node);
                    cur_data.to_first = &copy;

                    /// Replace children to newly created nodes.
                    for (auto & child : copy.children)
                    {
                        child = data[child].to_first;
                        assert(child != nullptr);
                    }

                    if (cur_data.used_in_result)
                    {
                        /// If this node is needed in result, add it as input.
                        Node input_node;
                        input_node.type = ActionType::INPUT;
                        input_node.result_type = cur.node->result_type;
                        input_node.result_name = cur.node->result_name;
                        cur_data.to_second = &second_nodes.emplace_back(std::move(input_node));

                        if (cur.node->type != ActionType::INPUT)
                            new_inputs.push_back(cur.node);
                    }
                }
            }
        }
    }

    for (const auto * output_node : outputs)
        second_outputs.push_back(data[output_node].to_second);

    NodeRawConstPtrs second_inputs;
    NodeRawConstPtrs first_inputs;

    for (const auto * input_node : inputs)
    {
        const auto & cur = data[input_node];
        if (cur.to_first)
        {
            first_inputs.push_back(cur.to_first);

            if (cur.to_second)
                first_outputs.push_back(cur.to_first);
        }
    }

    for (const auto * input : new_inputs)
    {
        auto & cur = data[input];

        if (avoid_duplicate_inputs)
        {
            bool is_name_updated = false;
            while (!duplicate_inputs.insert(cur.to_first->result_name).second)
            {
                is_name_updated = true;
                cur.to_first->result_name = fmt::format("{}_{}", input->result_name, duplicate_counter);
                ++duplicate_counter;
            }

            if (is_name_updated)
            {
                Node input_node;
                input_node.type = ActionType::INPUT;
                input_node.result_type = cur.to_first->result_type;
                input_node.result_name = cur.to_first->result_name;

                auto * new_input = &second_nodes.emplace_back(std::move(input_node));
                cur.to_second->type = ActionType::ALIAS;
                cur.to_second->children = {new_input};
                cur.to_second = new_input;
            }
        }

        second_inputs.push_back(cur.to_second);
        first_outputs.push_back(cur.to_first);
    }

    for (const auto * input_node : inputs)
    {
        const auto & cur = data[input_node];
        if (cur.to_second)
            second_inputs.push_back(cur.to_second);
    }

    ActionsDAG first_actions;
    first_actions.nodes.swap(first_nodes);
    first_actions.outputs.swap(first_outputs);
    first_actions.inputs.swap(first_inputs);

    ActionsDAG second_actions;
    second_actions.nodes.swap(second_nodes);
    second_actions.outputs.swap(second_outputs);
    second_actions.inputs.swap(second_inputs);

    std::unordered_map<const Node *, const Node *> split_nodes_mapping;
    if (create_split_nodes_mapping)
    {
        for (const auto * node : split_nodes)
            split_nodes_mapping[node] = data[node].to_first;
    }

    return {std::move(first_actions), std::move(second_actions), std::move(split_nodes_mapping)};
}

ActionsDAG::SplitResult ActionsDAG::splitActionsBeforeArrayJoin(const Names & array_joined_columns) const
{
    std::unordered_set<std::string_view> array_joined_columns_set(array_joined_columns.begin(), array_joined_columns.end());
    struct Frame
    {
        const Node * node = nullptr;
        size_t next_child_to_visit = 0;
    };

    std::unordered_set<const Node *> split_nodes;
    std::unordered_set<const Node *> visited_nodes;

    std::stack<Frame> stack;

    /// DFS. Decide if node depends on ARRAY JOIN.
    for (const auto & node : nodes)
    {
        if (visited_nodes.contains(&node))
            continue;

        visited_nodes.insert(&node);
        stack.push({.node = &node});

        while (!stack.empty())
        {
            auto & cur = stack.top();

            /// At first, visit all children. We depend on ARRAY JOIN if any child does.
            while (cur.next_child_to_visit < cur.node->children.size())
            {
                const auto * child = cur.node->children[cur.next_child_to_visit];

                if (!visited_nodes.contains(child))
                {
                    visited_nodes.insert(child);
                    stack.push({.node = child});
                    break;
                }

                ++cur.next_child_to_visit;
            }

            if (cur.next_child_to_visit == cur.node->children.size())
            {
                bool depend_on_array_join = false;
                if (cur.node->type == ActionType::INPUT && array_joined_columns_set.contains(cur.node->result_name))
                    depend_on_array_join = true;

                for (const auto * child : cur.node->children)
                {
                    if (!split_nodes.contains(child))
                        depend_on_array_join = true;
                }

                if (!depend_on_array_join)
                    split_nodes.insert(cur.node);

                stack.pop();
            }
        }
    }

    auto res = split(split_nodes);
    return res;
}

ActionsDAG::NodeRawConstPtrs ActionsDAG::getParents(const Node * target) const
{
    NodeRawConstPtrs parents;
    for (const auto & node : getNodes())
    {
        for (const auto & child : node.children)
        {
            if (child == target)
            {
                parents.push_back(&node);
                break;
            }
        }
    }
    return parents;
}

ActionsDAG::SplitResult ActionsDAG::splitActionsBySortingDescription(const NameSet & sort_columns) const
{
    std::unordered_set<const Node *> split_nodes;
    for (const auto & sort_column : sort_columns)
        if (const auto * node = tryFindInOutputs(sort_column))
        {
            split_nodes.insert(node);
            /// Sorting can materialize const columns, so if we have const expression used in sorting,
            /// we should also add all it's parents, otherwise, we can break the header
            /// (function can expect const column, but will get materialized).
            if (node->column && isColumnConst(*node->column))
            {
                auto parents = getParents(node);
                split_nodes.insert(parents.begin(), parents.end());
            }
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Sorting column {} wasn't found in the ActionsDAG's outputs. DAG:\n{}",
                sort_column,
                dumpDAG());

    auto res = split(split_nodes);
    return res;
}

bool ActionsDAG::isFilterAlwaysFalseForDefaultValueInputs(const std::string & filter_name, const Block & input_stream_header) const
{
    const auto * filter_node = tryFindInOutputs(filter_name);
    if (!filter_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Outputs for ActionsDAG does not contain filter column name {}. DAG:\n{}",
                        filter_name,
                        dumpDAG());

    std::unordered_map<std::string, ColumnWithTypeAndName> input_node_name_to_default_input_column;

    for (const auto * input : inputs)
    {
        if (!input_stream_header.has(input->result_name))
            continue;

        if (input->column)
            continue;

        auto constant_column = input->result_type->createColumnConst(1, input->result_type->getDefault());
        auto constant_column_with_type_and_name = ColumnWithTypeAndName{constant_column, input->result_type, input->result_name};
        input_node_name_to_default_input_column.emplace(input->result_name, std::move(constant_column_with_type_and_name));
    }

    std::optional<ActionsDAG> filter_with_default_value_inputs;

    try
    {
        filter_with_default_value_inputs = buildFilterActionsDAG({filter_node}, input_node_name_to_default_input_column);
    }
    catch (const Exception &)
    {
        /** It is possible that duing DAG construction, some functions cannot be executed for constant default value inputs
          * and exception will be thrown.
          */
        return false;
    }

    if (!filter_with_default_value_inputs)
        return false;

    const auto * filter_with_default_value_inputs_filter_node = filter_with_default_value_inputs->getOutputs()[0];
    if (!filter_with_default_value_inputs_filter_node->column || !isColumnConst(*filter_with_default_value_inputs_filter_node->column))
        return false;

    const auto & constant_type = filter_with_default_value_inputs_filter_node->result_type;
    if (!constant_type->canBeUsedInBooleanContext())
        return false;

    Field value;
    filter_with_default_value_inputs_filter_node->column->get(0, value);

    if (value.isNull())
        return true;

    auto predicate_value = value.safeGet<UInt8>();
    return predicate_value == 0;
}

ActionsDAG::SplitResult ActionsDAG::splitActionsForFilter(const std::string & column_name) const
{
    const auto * node = tryFindInOutputs(column_name);
    if (!node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Outputs for ActionsDAG does not contain filter column name {}. DAG:\n{}",
                        column_name,
                        dumpDAG());

    std::unordered_set<const Node *> split_nodes = {node};
    auto res = split(split_nodes);
    return res;
}

namespace
{

struct ConjunctionNodes
{
    ActionsDAG::NodeRawConstPtrs allowed;
    ActionsDAG::NodeRawConstPtrs rejected;
};

/// Take a node which result is a predicate.
/// Assuming predicate is a conjunction (probably, trivial).
/// Find separate conjunctions nodes. Split nodes into allowed and rejected sets.
/// Allowed predicate is a predicate which can be calculated using only nodes from the allowed_nodes set.
ConjunctionNodes getConjunctionNodes(ActionsDAG::Node * predicate, std::unordered_set<const ActionsDAG::Node *> allowed_nodes, bool allow_non_deterministic_functions)
{
    ConjunctionNodes conjunction;
    std::unordered_set<const ActionsDAG::Node *> allowed;
    std::unordered_set<const ActionsDAG::Node *> rejected;

    /// Parts of predicate in case predicate is conjunction (or just predicate itself).
    std::unordered_set<const ActionsDAG::Node *> predicates;
    {
        std::stack<const ActionsDAG::Node *> stack;
        std::unordered_set<const ActionsDAG::Node *> visited_nodes;
        stack.push(predicate);
        visited_nodes.insert(predicate);
        while (!stack.empty())
        {
            const auto * node = stack.top();
            stack.pop();
            bool is_conjunction = node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "and";
            if (is_conjunction)
            {
                for (const auto & child : node->children)
                {
                    if (!visited_nodes.contains(child))
                    {
                        visited_nodes.insert(child);
                        stack.push(child);
                    }
                }
            }
            else
                predicates.insert(node);
        }
    }

    struct Frame
    {
        const ActionsDAG::Node * node = nullptr;
        size_t next_child_to_visit = 0;
        size_t num_allowed_children = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const ActionsDAG::Node *> visited_nodes;

    stack.push({.node = predicate});
    visited_nodes.insert(predicate);
    while (!stack.empty())
    {
        auto & cur = stack.top();

        /// At first, visit all children.
        while (cur.next_child_to_visit < cur.node->children.size())
        {
            const auto * child = cur.node->children[cur.next_child_to_visit];

            if (!visited_nodes.contains(child))
            {
                visited_nodes.insert(child);
                stack.push({.node = child});
                break;
            }

            if (allowed_nodes.contains(child))
                ++cur.num_allowed_children;
            ++cur.next_child_to_visit;
        }

        if (cur.next_child_to_visit == cur.node->children.size())
        {
            if (cur.num_allowed_children == cur.node->children.size())
            {
                bool is_deprecated_function = !allow_non_deterministic_functions
                    && cur.node->type == ActionsDAG::ActionType::FUNCTION
                    && !cur.node->function_base->isDeterministicInScopeOfQuery();

                if (cur.node->type != ActionsDAG::ActionType::ARRAY_JOIN
                    && cur.node->type != ActionsDAG::ActionType::INPUT
                    && !is_deprecated_function)
                    allowed_nodes.emplace(cur.node);
            }

            if (predicates.contains(cur.node))
            {
                if (allowed_nodes.contains(cur.node))
                {
                    if (allowed.insert(cur.node).second)
                        conjunction.allowed.push_back(cur.node);

                }
                else
                {
                    if (rejected.insert(cur.node).second)
                        conjunction.rejected.push_back(cur.node);
                }
            }

            stack.pop();
        }
    }

    return conjunction;
}

ColumnsWithTypeAndName prepareFunctionArguments(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    ColumnsWithTypeAndName arguments;
    arguments.reserve(nodes.size());

    for (const auto * child : nodes)
    {
        ColumnWithTypeAndName argument;
        argument.column = child->column;
        argument.type = child->result_type;
        argument.name = child->result_name;

        arguments.emplace_back(std::move(argument));
    }

    return arguments;
}

}

std::optional<ActionsDAG::ActionsForFilterPushDown> ActionsDAG::createActionsForConjunction(NodeRawConstPtrs conjunction, const ColumnsWithTypeAndName & all_inputs)
{
    if (conjunction.empty())
        return {};

    ActionsDAG actions;
    bool remove_filter = true;

    FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> nodes_mapping;
    std::unordered_map<std::string, std::list<const Node *>> required_inputs;

    struct Frame
    {
        const ActionsDAG::Node * node = nullptr;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;

    /// DFS. Clone actions.
    for (const auto * predicate : conjunction)
    {
        if (nodes_mapping.contains(predicate))
            continue;

        stack.push({.node = predicate});
        while (!stack.empty())
        {
            auto & cur = stack.top();
            /// At first, visit all children.
            while (cur.next_child_to_visit < cur.node->children.size())
            {
                const auto * child = cur.node->children[cur.next_child_to_visit];

                if (!nodes_mapping.contains(child))
                {
                    stack.push({.node = child});
                    break;
                }

                ++cur.next_child_to_visit;
            }

            if (cur.next_child_to_visit == cur.node->children.size())
            {
                auto & node = actions.nodes.emplace_back(*cur.node);
                nodes_mapping[cur.node] = &node;

                for (auto & child : node.children)
                    child = nodes_mapping[child];

                if (node.type == ActionType::INPUT)
                    required_inputs[node.result_name].push_back(&node);

                stack.pop();
            }
        }
    }

    const Node * result_predicate = nodes_mapping[*conjunction.begin()];

    if (conjunction.size() > 1)
    {
        NodeRawConstPtrs args;
        args.reserve(conjunction.size());
        for (const auto * predicate : conjunction)
            args.emplace_back(nodes_mapping[predicate]);

        result_predicate = &actions.addFunction(func_builder_and, std::move(args), {});
    }

    size_t filter_pos = 0;
    bool has_input_name_collision = false;

    for (const auto & col : all_inputs)
    {
        const Node * input;
        auto & list = required_inputs[col.name];
        if (list.empty())
            input = &actions.addInput(col);
        else
        {
            input = list.front();
            list.pop_front();
            actions.inputs.push_back(input);
        }

        /// We should not add result_predicate into the outputs for the second time.
        /// If the predicate is an input, do not remove it.
        if (input == result_predicate)
        {
            remove_filter = false;
            filter_pos = actions.outputs.size();
        }
        /// Predicate name has a collision with another node. Need to rename it.
        else if (input->result_name == result_predicate->result_name)
            has_input_name_collision = true;

        actions.outputs.push_back(input);
    }

    if (has_input_name_collision)
    {
        for (size_t idx = 0;; ++idx)
        {
            std::string rename = fmt::format("_filter_{}_{}", result_predicate->result_name, idx);
            if (required_inputs.contains(rename))
                continue;

            result_predicate = &actions.addAlias(*result_predicate, std::move(rename));
            break;
        }
    }

    if (remove_filter)
        actions.outputs.insert(actions.outputs.begin(), result_predicate);

    if (result_predicate->type == ActionType::COLUMN)
    {
        /// If result is a column, it means that predicate is constant. Let's not push it further.
        return {};
    }

    return ActionsForFilterPushDown{std::move(actions), filter_pos, remove_filter, false};
}

std::optional<ActionsDAG::ActionsForFilterPushDown> ActionsDAG::splitActionsForFilterPushDown(
    const std::string & filter_name,
    bool removes_filter,
    const Names & available_inputs,
    const ColumnsWithTypeAndName & all_inputs,
    bool allow_non_deterministic_functions)
{
    Node * predicate = const_cast<Node *>(tryFindInOutputs(filter_name));
    if (!predicate)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
            filter_name,
            dumpDAG());

    /// If condition is constant let's do nothing.
    /// It means there is nothing to push down or optimization was already applied.
    if (predicate->type == ActionType::COLUMN)
        return {};

    std::unordered_set<const Node *> allowed_nodes;

    /// Get input nodes from available_inputs names.
    {
        std::unordered_map<std::string_view, std::list<const Node *>> inputs_map;
        for (const auto & input_node : inputs)
            inputs_map[input_node->result_name].emplace_back(input_node);

        for (const auto & name : available_inputs)
        {
            auto & inputs_list = inputs_map[name];
            if (inputs_list.empty())
                continue;

            allowed_nodes.emplace(inputs_list.front());
            inputs_list.pop_front();
        }
    }

    auto conjunction = getConjunctionNodes(predicate, allowed_nodes, allow_non_deterministic_functions);

    if (conjunction.allowed.empty())
        return {};

    chassert(predicate->result_type);

    auto actions = createActionsForConjunction(conjunction.allowed, all_inputs);
    if (!actions)
        return {};

    /// Now, when actions are created, update the current DAG.
    actions->is_filter_const_after_push_down = removeUnusedConjunctions(std::move(conjunction.rejected), predicate, removes_filter);

    return actions;
}

ActionsDAG::ActionsForJOINFilterPushDown ActionsDAG::splitActionsForJOINFilterPushDown(
    const std::string & filter_name,
    bool removes_filter,
    const Names & left_stream_available_columns_to_push_down,
    const Block & left_stream_header,
    const Names & right_stream_available_columns_to_push_down,
    const Block & right_stream_header,
    const Names & equivalent_columns_to_push_down,
    const std::unordered_map<std::string, ColumnWithTypeAndName> & equivalent_left_stream_column_to_right_stream_column,
    const std::unordered_map<std::string, ColumnWithTypeAndName> & equivalent_right_stream_column_to_left_stream_column)
{
    Node * predicate = const_cast<Node *>(tryFindInOutputs(filter_name));
    if (!predicate)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
            filter_name,
            dumpDAG());

    /// If condition is constant let's do nothing.
    /// It means there is nothing to push down or optimization was already applied.
    if (predicate->type == ActionType::COLUMN)
        return {};

    auto get_input_nodes = [this](const Names & inputs_names)
    {
        std::unordered_set<const Node *> allowed_nodes;

        std::unordered_map<std::string_view, std::list<const Node *>> inputs_map;
        for (const auto & input_node : inputs)
            inputs_map[input_node->result_name].emplace_back(input_node);

        for (const auto & name : inputs_names)
        {
            auto & inputs_list = inputs_map[name];
            if (inputs_list.empty())
                continue;

            allowed_nodes.emplace(inputs_list.front());
            inputs_list.pop_front();
        }

        return allowed_nodes;
    };

    auto left_stream_allowed_nodes = get_input_nodes(left_stream_available_columns_to_push_down);
    auto right_stream_allowed_nodes = get_input_nodes(right_stream_available_columns_to_push_down);
    auto both_streams_allowed_nodes = get_input_nodes(equivalent_columns_to_push_down);

    auto left_stream_push_down_conjunctions = getConjunctionNodes(predicate, left_stream_allowed_nodes, false);
    auto right_stream_push_down_conjunctions = getConjunctionNodes(predicate, right_stream_allowed_nodes, false);
    auto both_streams_push_down_conjunctions = getConjunctionNodes(predicate, both_streams_allowed_nodes, false);

    NodeRawConstPtrs left_stream_allowed_conjunctions = std::move(left_stream_push_down_conjunctions.allowed);
    NodeRawConstPtrs right_stream_allowed_conjunctions = std::move(right_stream_push_down_conjunctions.allowed);

    std::unordered_set<const Node *> left_stream_allowed_conjunctions_set(left_stream_allowed_conjunctions.begin(), left_stream_allowed_conjunctions.end());
    std::unordered_set<const Node *> right_stream_allowed_conjunctions_set(right_stream_allowed_conjunctions.begin(), right_stream_allowed_conjunctions.end());

    for (const auto * both_streams_push_down_allowed_conjunction_node : both_streams_push_down_conjunctions.allowed)
    {
        if (!left_stream_allowed_conjunctions_set.contains(both_streams_push_down_allowed_conjunction_node))
            left_stream_allowed_conjunctions.push_back(both_streams_push_down_allowed_conjunction_node);

        if (!right_stream_allowed_conjunctions_set.contains(both_streams_push_down_allowed_conjunction_node))
            right_stream_allowed_conjunctions.push_back(both_streams_push_down_allowed_conjunction_node);
    }

    std::unordered_set<const Node *> rejected_conjunctions_set;
    rejected_conjunctions_set.insert(left_stream_push_down_conjunctions.rejected.begin(), left_stream_push_down_conjunctions.rejected.end());
    rejected_conjunctions_set.insert(right_stream_push_down_conjunctions.rejected.begin(), right_stream_push_down_conjunctions.rejected.end());
    rejected_conjunctions_set.insert(both_streams_push_down_conjunctions.rejected.begin(), both_streams_push_down_conjunctions.rejected.end());

    for (const auto & left_stream_allowed_conjunction : left_stream_allowed_conjunctions)
        rejected_conjunctions_set.erase(left_stream_allowed_conjunction);

    for (const auto & right_stream_allowed_conjunction : right_stream_allowed_conjunctions)
        rejected_conjunctions_set.erase(right_stream_allowed_conjunction);

    NodeRawConstPtrs rejected_conjunctions(rejected_conjunctions_set.begin(), rejected_conjunctions_set.end());

    auto left_stream_filter_to_push_down = createActionsForConjunction(left_stream_allowed_conjunctions, left_stream_header.getColumnsWithTypeAndName());
    auto right_stream_filter_to_push_down = createActionsForConjunction(right_stream_allowed_conjunctions, right_stream_header.getColumnsWithTypeAndName());

    auto replace_equivalent_columns_in_filter = [](
        const ActionsDAG & filter,
        size_t filter_pos,
        const Block & stream_header,
        const std::unordered_map<std::string, ColumnWithTypeAndName> & columns_to_replace)
    {
        auto updated_filter = *ActionsDAG::buildFilterActionsDAG({filter.getOutputs()[filter_pos]}, columns_to_replace);
        chassert(updated_filter.getOutputs().size() == 1);

        /** If result filter to left or right stream has column that is one of the stream inputs, we need distinguish filter column from
          * actual input column. It is necessary because after filter step, filter column became constant column with value 1, and
          * not all JOIN algorithms properly work with constants.
          *
          * Example: SELECT key FROM ( SELECT key FROM t1 ) AS t1 JOIN ( SELECT key FROM t1 ) AS t2 ON t1.key = t2.key WHERE key;
          */
        const auto * stream_filter_node = updated_filter.getOutputs()[0];
        if (stream_header.has(stream_filter_node->result_name))
        {
            const auto & alias_node = updated_filter.addAlias(*stream_filter_node, "__filter" + stream_filter_node->result_name);
            updated_filter.getOutputs()[0] = &alias_node;
        }

        std::unordered_map<std::string, std::list<const Node *>> updated_filter_inputs;

        for (const auto & input : updated_filter.getInputs())
            updated_filter_inputs[input->result_name].push_back(input);

        for (const auto & input : filter.getInputs())
        {
            if (updated_filter_inputs.contains(input->result_name))
                continue;

            const Node * updated_filter_input_node = nullptr;

            auto it = columns_to_replace.find(input->result_name);
            if (it != columns_to_replace.end())
                updated_filter_input_node = &updated_filter.addInput(it->second);
            else
                updated_filter_input_node = &updated_filter.addInput({input->column, input->result_type, input->result_name});

            updated_filter_inputs[input->result_name].push_back(updated_filter_input_node);
        }

        for (const auto & input_column : stream_header.getColumnsWithTypeAndName())
        {
            const Node * input;
            auto & list = updated_filter_inputs[input_column.name];
            if (list.empty())
            {
                input = &updated_filter.addInput(input_column);
            }
            else
            {
                input = list.front();
                list.pop_front();
            }

            if (input != updated_filter.getOutputs()[0])
                updated_filter.outputs.push_back(input);
        }

        return updated_filter;
    };

    if (left_stream_filter_to_push_down)
        left_stream_filter_to_push_down->dag = replace_equivalent_columns_in_filter(left_stream_filter_to_push_down->dag,
            left_stream_filter_to_push_down->filter_pos,
            left_stream_header,
            equivalent_right_stream_column_to_left_stream_column);

    if (right_stream_filter_to_push_down)
        right_stream_filter_to_push_down->dag = replace_equivalent_columns_in_filter(right_stream_filter_to_push_down->dag,
            right_stream_filter_to_push_down->filter_pos,
            right_stream_header,
            equivalent_left_stream_column_to_right_stream_column);

    /*
     * We should check the presence of a split filter column name in stream columns to avoid removing the required column.
     *
     * Example:
     * A filter expression is `a AND b = c`, but `b` and `c` belong to another side of the join and not in allowed columns to push down,
     * so the final split filter is just `a`.
     * In this case `a` can be in stream columns but not `and(a, equals(b, c))`.
     */

    bool left_stream_filter_removes_filter = true;
    bool right_stream_filter_removes_filter = true;
    std::optional<ActionsDAG> left_stream_filter_to_push_down_dag;
    std::optional<ActionsDAG> right_stream_filter_to_push_down_dag;

    if (left_stream_filter_to_push_down)
    {
        const auto & left_stream_filter_column_name = left_stream_filter_to_push_down->dag.getOutputs()[0]->result_name;
        left_stream_filter_removes_filter = !left_stream_header.has(left_stream_filter_column_name);
        left_stream_filter_to_push_down_dag = std::move(left_stream_filter_to_push_down->dag);
    }

    if (right_stream_filter_to_push_down)
    {
        const auto & right_stream_filter_column_name = right_stream_filter_to_push_down->dag.getOutputs()[0]->result_name;
        right_stream_filter_removes_filter = !right_stream_header.has(right_stream_filter_column_name);
        right_stream_filter_to_push_down_dag = std::move(right_stream_filter_to_push_down->dag);
    }

    ActionsDAG::ActionsForJOINFilterPushDown result{
        .left_stream_filter_to_push_down = std::move(left_stream_filter_to_push_down_dag),
        .left_stream_filter_removes_filter = left_stream_filter_removes_filter,
        .right_stream_filter_to_push_down = std::move(right_stream_filter_to_push_down_dag),
        .right_stream_filter_removes_filter = right_stream_filter_removes_filter,
        .is_filter_const_after_all_push_downs = false};

    if (!result.left_stream_filter_to_push_down && !result.right_stream_filter_to_push_down)
        return result;

    /// Now, when actions are created, update the current DAG.
    result.is_filter_const_after_all_push_downs = removeUnusedConjunctions(std::move(rejected_conjunctions), predicate, removes_filter);

    return result;
}

bool ActionsDAG::removeUnusedConjunctions(NodeRawConstPtrs rejected_conjunctions, Node * predicate, bool removes_filter)
{
    bool is_filter_const = false;
    if (rejected_conjunctions.empty())
    {
        /// The whole predicate was split.
        if (removes_filter)
        {
            /// If filter column is not needed, remove it from output nodes.
            std::erase_if(outputs, [&](const Node * node) { return node == predicate; });
        }
        else
        {
            /// Replace predicate result to constant 1.
            Node node;
            node.type = ActionType::COLUMN;
            node.result_name = predicate->result_name;
            node.result_type = predicate->result_type;
            node.column = node.result_type->createColumnConst(0, 1);

            if (predicate->type != ActionType::INPUT)
                *predicate = std::move(node);
            else
            {
                /// Special case. We cannot replace input to constant inplace.
                /// Because we cannot affect inputs list for actions.
                /// So we just add a new constant and update outputs.
                const auto * new_predicate = &addNode(node);
                for (auto & output_node : outputs)
                    if (output_node == predicate)
                        output_node = new_predicate;
            }

            is_filter_const = true;
        }
    }
    else
    {
        /// Predicate is conjunction, where both allowed and rejected sets are not empty.

        NodeRawConstPtrs new_children = std::move(rejected_conjunctions);

        if (new_children.size() == 1)
        {
            /// Rejected set has only one predicate.
            /// Fix the result type and add an alias.
            auto & child = new_children.front();

            if (!removes_filter)
            {
                /// Preserve the original type if the column is needed in the result.
                if (isFloat(removeLowCardinalityAndNullable(child->result_type)))
                {
                    /// For floating point types, it's not enough to cast to just UInt8.
                    /// Because counstants like 0.1 will be casted to 0, which is inconsistent with e.g. "1 and 0.1"
                    DataTypePtr cast_type = DataTypeFactory::instance().get("Bool");
                    if (isNullableOrLowCardinalityNullable(child->result_type))
                        cast_type = std::make_shared<DataTypeNullable>(std::move(cast_type));
                    child = &addCast(*child, cast_type, {}, nullptr);
                }

                if (!child->result_type->equals(*predicate->result_type))
                    child = &addCast(*child, predicate->result_type, {}, nullptr);
            }

            Node node;
            node.type = ActionType::ALIAS;
            node.result_name = predicate->result_name;
            node.result_type = predicate->result_type;
            node.children.swap(new_children);
            *predicate = std::move(node);
        }
        else
        {
            /// Predicate is function AND, which still have more then one argument
            /// or it has one argument of the wrong type.
            /// Update children and rebuild it.
            predicate->children.swap(new_children);
            auto arguments = prepareFunctionArguments(predicate->children);

            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

            predicate->function_base = func_builder_and->build(arguments);
            predicate->function = predicate->function_base->prepare(arguments);
        }
    }

    std::unordered_set<const Node *> used_inputs;
    for (const auto * input : inputs)
        used_inputs.insert(input);

    removeUnusedActions(used_inputs);

    return is_filter_const;
}

static bool isColumnSortingPreserved(const ActionsDAG::Node * start_node, const String & sorted_column)
{
    /// only function node can several children
    /// but we support monotonicity check only for functions with one argument
    /// so, currently we consider just first child - it covers majority of cases
    /// TODO: if one parameter is variable and other are constant then we can try to check monotonicity as well

    /// first find the column
    const ActionsDAG::Node * node = start_node;
    bool found = false;
    while (node)
    {
        /// if column found
        if (node->type == ActionsDAG::ActionType::INPUT && node->result_name == sorted_column)
        {
            found = true;
            break;
        }

        if (node->children.empty())
            break; /// column not found

        node = node->children.front();
    }
    if (!found)
        return false;

    /// if column found, check if sorting is preserved
    const Field field{};
    node = start_node;
    while (node)
    {
        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            auto func = node->function_base;
            if (func)
            {
                if (!func->hasInformationAboutMonotonicity())
                    return false;

                const auto & types = func->getArgumentTypes();
                if (types.empty())
                    return false;

                const auto monotonicity = func->getMonotonicityForRange(*types.front(), field, field);
                if (!monotonicity.is_always_monotonic)
                    return false;
            }
        }

        if (node->children.empty())
            break;

        node = node->children.front();
    }
    return true;
}

bool ActionsDAG::isSortingPreserved(
    const Block & input_header, const SortDescription & sort_description, const String & ignore_output_column) const
{
    if (sort_description.empty())
        return true;

    if (hasArrayJoin())
        return false;

    const Block & output_header = updateHeader(input_header);
    for (const auto & desc : sort_description)
    {
        /// header contains column with the same name
        if (output_header.findByName(desc.column_name))
        {
            /// find the corresponding node in output
            const auto * output_node = tryFindInOutputs(desc.column_name);
            if (!output_node)
            {
                /// sorted column name in header but NOT in expression output -> no expression is applied to it -> sorting preserved
                continue;
            }
        }

        /// check if any output node is related to the sorted column and sorting order is preserved
        bool preserved = false;
        for (const auto * output_node : outputs)
        {
            if (output_node->result_name == ignore_output_column)
                continue;

            if (isColumnSortingPreserved(output_node, desc.column_name))
            {
                preserved = true;
                break;
            }
        }
        if (!preserved)
            return false;
    }

    return true;
}

std::optional<ActionsDAG> ActionsDAG::buildFilterActionsDAG(
    const NodeRawConstPtrs & filter_nodes,
    const std::unordered_map<std::string, ColumnWithTypeAndName> & node_name_to_input_node_column,
    bool single_output_condition_node)
{
    if (filter_nodes.empty())
        return {};

    struct Frame
    {
        const ActionsDAG::Node * node = nullptr;
        bool visited_children = false;
    };

    ActionsDAG result_dag;
    std::unordered_map<std::string, const ActionsDAG::Node *> result_inputs;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> node_to_result_node;

    size_t filter_nodes_size = filter_nodes.size();

    std::vector<Frame> nodes_to_process;
    nodes_to_process.reserve(filter_nodes_size);

    for (const auto & node : filter_nodes)
        nodes_to_process.push_back({node, false /*visited_children*/});

    while (!nodes_to_process.empty())
    {
        auto & node_to_process = nodes_to_process.back();
        const auto * node = node_to_process.node;

        /// Already visited node
        if (node_to_result_node.contains(node))
        {
            nodes_to_process.pop_back();
            continue;
        }

        const ActionsDAG::Node * result_node = nullptr;

        auto input_node_it = node_name_to_input_node_column.find(node->result_name);
        if (input_node_it != node_name_to_input_node_column.end())
        {
            auto & result_input = result_inputs[input_node_it->second.name];
            if (!result_input)
                result_input = &result_dag.addInput(input_node_it->second);

            node_to_result_node.emplace(node, result_input);
            nodes_to_process.pop_back();
            continue;
        }

        if (!node_to_process.visited_children)
        {
            node_to_process.visited_children = true;

            for (const auto & child : node->children)
                nodes_to_process.push_back({child, false /*visited_children*/});

            /// If node has children process them first
            if (!node->children.empty())
                continue;
        }

        auto node_type = node->type;

        switch (node_type)
        {
            case ActionsDAG::ActionType::INPUT:
            {
                auto & result_input = result_inputs[node->result_name];
                if (!result_input)
                    result_input = &result_dag.addInput({node->column, node->result_type, node->result_name});
                result_node = result_input;
                break;
            }
            case ActionsDAG::ActionType::COLUMN:
            {
                result_node = &result_dag.addColumn({node->column, node->result_type, node->result_name}, node->is_deterministic_constant);
                break;
            }
            case ActionsDAG::ActionType::ALIAS:
            {
                const auto * child = node->children.front();
                result_node = &result_dag.addAlias(*(node_to_result_node.find(child)->second), node->result_name);
                break;
            }
            case ActionsDAG::ActionType::ARRAY_JOIN:
            {
                const auto * child = node->children.front();
                result_node = &result_dag.addArrayJoin(*(node_to_result_node.find(child)->second), {});
                break;
            }
            case ActionsDAG::ActionType::FUNCTION:
            {
                NodeRawConstPtrs function_children;
                function_children.reserve(node->children.size());

                FunctionOverloadResolverPtr function_overload_resolver;

                String result_name;
                if (node->function_base->getName() == "indexHint")
                {
                    ActionsDAG::NodeRawConstPtrs children;
                    if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node->function_base.get()))
                    {
                        if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                        {
                            ActionsDAG index_hint_filter_dag;
                            const auto & index_hint_args = index_hint->getActions().getOutputs();

                            if (!index_hint_args.empty())
                                index_hint_filter_dag = *buildFilterActionsDAG(index_hint_args,
                                    node_name_to_input_node_column,
                                    false /*single_output_condition_node*/);

                            auto index_hint_function_clone = std::make_shared<FunctionIndexHint>();
                            index_hint_function_clone->setActions(std::move(index_hint_filter_dag));
                            function_overload_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(index_hint_function_clone));
                            /// Keep the unique name like "indexHint(foo)" instead of replacing it
                            /// with "indexHint()". Otherwise index analysis (which does look at
                            /// indexHint arguments that we're hiding here) will get confused by the
                            /// multiple substantially different nodes with the same result name.
                            result_name = node->result_name;
                        }
                    }
                }

                for (const auto & child : node->children)
                    function_children.push_back(node_to_result_node.find(child)->second);

                auto [arguments, all_const] = getFunctionArguments(function_children);
                auto function_base = function_overload_resolver ? function_overload_resolver->build(arguments) : node->function_base;

                result_node = &result_dag.addFunctionImpl(
                    function_base,
                    std::move(function_children),
                    std::move(arguments),
                    result_name,
                    node->result_type,
                    all_const);
                break;
            }
            case ActionsDAG::ActionType::PLACEHOLDER:
            {
                /// TODO: check if it's correct
                result_node = &result_dag.addPlaceholder(node->result_name, node->result_type);
                break;
            }
        }

        node_to_result_node.emplace(node, result_node);
        nodes_to_process.pop_back();
    }

    auto & result_dag_outputs = result_dag.getOutputs();
    result_dag_outputs.reserve(filter_nodes_size);

    for (const auto & node : filter_nodes)
        result_dag_outputs.push_back(node_to_result_node.find(node)->second);

    if (result_dag_outputs.size() > 1 && single_output_condition_node)
    {
        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        result_dag_outputs = { &result_dag.addFunction(func_builder_and, result_dag_outputs, {}) };
    }

    return result_dag;
}

ActionsDAG::NodeRawConstPtrs ActionsDAG::extractConjunctionAtoms(const Node * predicate)
{
    NodeRawConstPtrs atoms;

    std::stack<const ActionsDAG::Node *> stack;
    stack.push(predicate);

    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();
        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & name = node->function_base->getName();
            if (name == "and")
            {
                for (const auto * arg : node->children)
                    stack.push(arg);

                continue;
            }
        }

        atoms.push_back(node);
    }

    return atoms;
}

ActionsDAG::NodeRawConstPtrs ActionsDAG::filterNodesByAllowedInputs(
    NodeRawConstPtrs nodes,
    const std::unordered_set<const Node *> & allowed_inputs)
{
    size_t result_size = 0;

    std::unordered_map<const ActionsDAG::Node *, bool> can_compute;
    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
        bool can_compute_all_childern = true;
    };

    std::stack<Frame> stack;

    for (const auto * node : nodes)
    {
        if (!can_compute.contains(node))
            stack.push({node});

        while (!stack.empty())
        {
            auto & frame = stack.top();
            bool need_visit_child = false;
            while (frame.next_child_to_visit < frame.node->children.size())
            {
                auto it = can_compute.find(frame.node->children[frame.next_child_to_visit]);
                if (it == can_compute.end())
                {
                    stack.push({frame.node->children[frame.next_child_to_visit]});
                    need_visit_child = true;
                    break;
                }

                frame.can_compute_all_childern &= it->second;
                ++frame.next_child_to_visit;
            }

            if (need_visit_child)
                continue;

            if (frame.node->type == ActionsDAG::ActionType::INPUT)
                can_compute[frame.node] = allowed_inputs.contains(frame.node);
            else
                can_compute[frame.node] = frame.can_compute_all_childern;

            stack.pop();
        }

        if (can_compute.at(node))
        {
            nodes[result_size] = node;
            ++result_size;
        }
    }

    nodes.resize(result_size);
    return nodes;
}

ActionsDAG ActionsDAG::restrictFilterDAGToInputs(const ActionsDAG::Node * filter_node, const NameSet & available_inputs) const
{
    ActionsDAG actions;
    std::unordered_map<const Node *, const Node *> copy_map;
    std::unordered_map<const ActionsDAG::Node *, bool> can_compute;

    /// Phase 1: Traverse the DAG and determine which nodes can be computed
    {
        struct Frame
        {
            const ActionsDAG::Node * node;
            size_t next_child_to_visit = 0;
            bool can_compute_all_children = true;
        };

        std::stack<Frame> stack;
        stack.push({filter_node});
        while (!stack.empty())
        {
            auto & frame = stack.top();
            bool need_visit_child = false;

            while (frame.next_child_to_visit < frame.node->children.size())
            {
                const auto * child = frame.node->children[frame.next_child_to_visit];
                auto it = can_compute.find(child);
                if (it == can_compute.end())
                {
                    stack.push({child});
                    need_visit_child = true;
                    break;
                }

                /// If child is not computable, attempt to substitute it with a constant.
                /// Currently, only "and" function is supported.
                if (!it->second)
                {
                    if (frame.node->type == ActionsDAG::ActionType::FUNCTION)
                    {
                        const auto & name = frame.node->function_base->getName();

                        /// Replace non-computable child in "and" with constant true.
                        if (name == "and")
                        {
                            ColumnWithTypeAndName column;
                            column.name = child->result_name;
                            column.type = child->result_type;
                            column.column = child->result_type->createColumnConst(0, 1);
                            copy_map[child] = &actions.addColumn(std::move(column));

                            /// Mark as now computable (since we substituted it)
                            it->second = true;
                        }
                    }
                }

                frame.can_compute_all_children &= it->second;
                ++frame.next_child_to_visit;
            }

            if (need_visit_child)
                continue;

            if (frame.node->type == ActionsDAG::ActionType::INPUT)
                can_compute[frame.node] = available_inputs.contains(frame.node->result_name);
            else
                can_compute[frame.node] = frame.can_compute_all_children;

            stack.pop();
        }
    }

    /// Phase 2: Reconstruct the DAG using copy_map
    struct Frame
    {
        const Node * node = nullptr;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    if (!copy_map.contains(filter_node))
    {
        stack.push(Frame{filter_node});
        while (!stack.empty())
        {
            auto & frame = stack.top();
            const auto & children = frame.node->children;

            while (frame.next_child < children.size() && copy_map.contains(children[frame.next_child]))
                ++frame.next_child;

            if (frame.next_child < children.size())
            {
                stack.push(Frame{children[frame.next_child]});
                continue;
            }

            auto & copy_node = copy_map[frame.node];
            copy_node = &actions.nodes.emplace_back(*frame.node);

            if (frame.node->type == ActionType::INPUT)
                actions.inputs.push_back(copy_node);

            stack.pop();
        }
    }

    for (auto & node : actions.nodes)
        for (auto & child : node.children)
            child = copy_map[child];

    actions.outputs.push_back(copy_map[filter_node]);

    return actions;
}

FindOriginalNodeForOutputName::FindOriginalNodeForOutputName(const ActionsDAG & actions_)
{
    const auto & actions_outputs = actions_.getOutputs();
    for (const auto * output_node : actions_outputs)
    {
        /// find input node which refers to the output node
        /// consider only aliases on the path
        const auto * node = output_node;
        while (node)
        {
            if (node->type == ActionsDAG::ActionType::ALIAS)
            {
                node = node->children.front();
            }
            /// materiailze() function can occur when dealing with views
            /// TODO: not sure if it should be done here, looks too generic place
            else if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "materialize")
            {
                chassert(node->children.size() == 1);
                node = node->children.front();
            }
            else
                break;
        }
        if (node && node->type == ActionsDAG::ActionType::INPUT)
            index.emplace(output_node->result_name, node);
    }
}

const ActionsDAG::Node * FindOriginalNodeForOutputName::find(const String & output_name)
{
    const auto it = index.find(output_name);
    if (it == index.end())
        return nullptr;

    return it->second;
}


static void serializeCapture(const LambdaCapture & capture, WriteBuffer & out)
{
    writeStringBinary(capture.return_name, out);
    encodeDataType(capture.return_type, out);

    writeVarUInt(capture.captured_names.size(), out);
    for (const auto & name : capture.captured_names)
        writeStringBinary(name, out);

    writeVarUInt(capture.captured_types.size(), out);
    for (const auto & type : capture.captured_types)
        encodeDataType(type, out);

    writeVarUInt(capture.lambda_arguments.size(), out);
    for (const auto & item : capture.lambda_arguments)
    {
        writeStringBinary(item.name, out);
        encodeDataType(item.type, out);
    }
}

static void deserializeCapture(LambdaCapture & capture, ReadBuffer & in)
{
    readStringBinary(capture.return_name, in);
    capture.return_type = decodeDataType(in);

    UInt64 num_names;
    readVarUInt(num_names, in);
    capture.captured_names.resize(num_names);
    for (auto & name : capture.captured_names)
        readStringBinary(name, in);

    UInt64 num_types;
    readVarUInt(num_types, in);
    capture.captured_types.resize(num_types);
    for (auto & type : capture.captured_types)
        type = decodeDataType(in);

    UInt64 num_args;
    readVarUInt(num_args, in);
    capture.lambda_arguments.clear();
    for (size_t i = 0; i < num_args; ++i)
    {
        NameAndTypePair name_and_type;
        readStringBinary(name_and_type.name, in);
        name_and_type.type = decodeDataType(in);
        capture.lambda_arguments.push_back(std::move(name_and_type));
    }
}

static void serializeConstant(const IDataType & type, const IColumn & value, WriteBuffer & out, SerializedSetsRegistry & registry)
{
    if (WhichDataType(type).isSet())
    {
        /// Apparently, it is important whether the set is inside constant column or not.
        /// If the set was made from tuple, we can apply constant folding.
        /// Also, constants affect function return type (e.g. for LowCardinality).
        /// However, we cannot always use constant columns because the sets from subquery are not ready yet.
        /// So, here we keep this information in the flag.
        ///
        /// Technically, this information can be restored from the set type.
        /// For now, only sets from subqueries are not constants.
        /// But we cannot get the set type before we deserialize it (this is done after the main plan).
        /// Also, we cannot serialize sets before the main plan, because they are registered lazily on serialization.
        bool is_constant = false;

        const IColumn * maybe_set = &value;
        if (const auto * column_const = typeid_cast<const ColumnConst *>(maybe_set))
        {
            is_constant = true;
            maybe_set = &column_const->getDataColumn();
        }

        const auto * column_set = typeid_cast<const ColumnSet *>(maybe_set);
        if (!column_set)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "ColumnSet is expected for DataTypeSet. Got {}", value.getName());

        UInt8 flags = 0;
        if (is_constant)
            flags |= 1;

        writeBinary(flags, out);

        auto hash = column_set->getData()->getHash();
        writeBinary(hash, out);
        registry.sets.emplace(hash, column_set->getData());
        return;
    }

    if (WhichDataType(type).isFunction())
    {
        const IColumn * maybe_function = &value;
        if (const auto * column_const = typeid_cast<const ColumnConst *>(maybe_function))
            maybe_function = &column_const->getDataColumn();

        const auto * column_function = typeid_cast<const ColumnFunction *>(maybe_function);
        if (!column_function)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "ColumnSet is expected for DataTypeSet. Got {}", value.getName());

        auto function = column_function->getFunction();
        const auto * function_expression = typeid_cast<const FunctionExpression *>(function.get());
        if (!function_expression)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected FunctionExpression for ColumnFunction. Got {}", function->getName());

        serializeCapture(function_expression->getCapture(), out);
        function_expression->getAcionsDAG().serialize(out, registry);

        const auto & captured_columns = column_function->getCapturedColumns();
        writeVarUInt(captured_columns.size(), out);
        for (const auto & captured_column : captured_columns)
        {
            encodeDataType(captured_column.type, out);
            serializeConstant(*captured_column.type, *captured_column.column, out, registry);
        }

        return;
    }

    const auto * const_column = typeid_cast<const ColumnConst *>(&value);
    if (!const_column)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot serialize non-constant column {} as constant", value.getName());

    const auto & data = const_column->getDataColumn();
    type.getDefaultSerialization()->serializeBinary(data, 0, out, FormatSettings{});
}

static MutableColumnPtr deserializeConstant(
    const IDataType & type,
    ReadBuffer & in,
    DeserializedSetsRegistry & registry,
    const ContextPtr & context)
{
    if (WhichDataType(type).isSet())
    {
        UInt8 flags;
        readBinary(flags, in);

        bool is_constant = flags & 1;

        FutureSet::Hash hash;
        readBinary(hash, in);

        auto column_set = ColumnSet::create(1, nullptr);

        if (!is_constant)
        {
            registry.sets[hash].push_back(column_set.get());
            return column_set;
        }

        auto column_const = ColumnConst::create(std::move(column_set), 0);
        /// After move, get the pointer from ColumnConst
        const auto * set_column = typeid_cast<const ColumnSet *>(column_const->getDataColumnPtr().get());
        registry.sets[hash].push_back(const_cast<ColumnSet *>(set_column));
        return column_const;
    }

    if (WhichDataType(type).isFunction())
    {
        LambdaCapture capture;
        deserializeCapture(capture, in);
        auto capture_dag = ActionsDAG::deserialize(in, registry, context);

        UInt64 num_captured_columns;
        readVarUInt(num_captured_columns, in);
        ColumnsWithTypeAndName captured_columns(num_captured_columns);

        for (auto & captured_column : captured_columns)
        {
            captured_column.type = decodeDataType(in);
            captured_column.column = deserializeConstant(*captured_column.type, in, registry, context);
        }

        auto function_expression = std::make_shared<FunctionExpression>(
            std::make_shared<LambdaCapture>(std::move(capture)),
            std::make_shared<ExpressionActions>(
                std::move(capture_dag),
                ExpressionActionsSettings(context, CompileExpressions::yes)));

        return ColumnFunction::create(1, std::move(function_expression), std::move(captured_columns));
    }

    auto column = type.createColumn();
    type.getDefaultSerialization()->deserializeBinary(*column, in, FormatSettings{});
    return ColumnConst::create(std::move(column), 0);
}

std::unordered_map<const ActionsDAG::Node *, size_t> ActionsDAG::getNodeToIdMap() const
{
    std::unordered_map<const Node *, size_t> node_to_id;
    for (const auto & node : nodes)
        node_to_id.emplace(&node, node_to_id.size());

    if (nodes.size() != node_to_id.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate nodes in ActionsDAG");

    return node_to_id;
}

std::vector<const ActionsDAG::Node *> ActionsDAG::getIdToNode() const
{
    return std::ranges::to<std::vector>(nodes | std::views::transform([](const auto & node) { return &node; }));
}

/// Reorder DAG nodes so that the whole subgraph of the inputs is listed before the node itself
static void addChildrenBeforeNode(std::vector<const ActionsDAG::Node *> & reordered_nodes, std::unordered_set<const ActionsDAG::Node *> & already_added_nodes, const ActionsDAG::Node * node)
{
    if (already_added_nodes.contains(node))
        return;

    for (const auto * child : node->children)
        addChildrenBeforeNode(reordered_nodes, already_added_nodes, child);

    reordered_nodes.push_back(node);
    already_added_nodes.insert(node);
};

void ActionsDAG::serialize(WriteBuffer & out, SerializedSetsRegistry & registry) const
{
    size_t nodes_size = nodes.size();
    writeVarUInt(nodes_size, out);

    /// Reorder nodes so that children are serialized before parents. Otherwise deserialization will be more complicated.
    std::vector<const Node *> reordered_nodes;
    {
        std::unordered_set<const Node *> already_added_nodes;
        for (const auto & node : nodes)
            addChildrenBeforeNode(reordered_nodes, already_added_nodes, &node);
    }

    std::unordered_map<const Node *, size_t> node_to_id;
    for (const auto * node : reordered_nodes)
        node_to_id.emplace(node, node_to_id.size());

    if (nodes.size() != node_to_id.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate nodes in ActionsDAG");

    for (size_t node_id = 0; node_id < reordered_nodes.size(); ++node_id)
    {
        const auto & node = *reordered_nodes[node_id];
        writeIntBinary(static_cast<UInt8>(node.type), out);
        writeStringBinary(node.result_name, out);
        encodeDataType(node.result_type, out);

        writeVarUInt(node.children.size(), out);
        for (const auto * child : node.children)
        {
            auto child_id = node_to_id.at(child);
            chassert(child_id < node_id, fmt::format("Node {} references child node {} that has not been serialized yet", node_id, child_id));
            writeVarUInt(child_id, out);
        }

        /// Serialize column if it is present
        const bool has_column = (node.type != ActionType::INPUT && node.column != nullptr);
        UInt8 column_flags = 0;
        if (has_column)
        {
            column_flags |= 1;
            if (node.is_deterministic_constant)
                column_flags |= 2;
        }

        const auto * function_capture = typeid_cast<const FunctionCapture *>(node.function_base.get());
        if (function_capture)
            column_flags |= 4;

        writeIntBinary(column_flags, out);

        if (has_column)
            serializeConstant(*node.result_type, *node.column, out, registry);

        if (node.type == ActionType::INPUT)
        {
            /// nothing to serialize
        }
        else if (node.type == ActionType::COLUMN)
        {
            /// nothing to serialize, column is already serialized
        }
        else if (node.type == ActionType::ALIAS)
        {
            /// nothing to serialize
        }
        else if (node.type == ActionType::FUNCTION)
        {
            writeStringBinary(node.function_base->getName(), out);
            if (function_capture)
            {
                serializeCapture(function_capture->getCapture(), out);
                function_capture->getAcionsDAG().serialize(out, registry);
            }
        }
        else if (node.type == ActionType::ARRAY_JOIN)
        {
            /// nothing to serialize
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown node type {}", static_cast<size_t>(node.type));
        }
    }

    writeVarUInt(inputs.size(), out);
    for (const auto * input : inputs)
        writeVarUInt(node_to_id.at(input), out);

    writeVarUInt(outputs.size(), out);
    for (const auto * output : outputs)
        writeVarUInt(node_to_id.at(output), out);
}

ActionsDAG ActionsDAG::deserialize(ReadBuffer & in, DeserializedSetsRegistry & registry, const ContextPtr & context)
{
    size_t nodes_size;
    readVarUInt(nodes_size, in);

    std::list<Node> nodes;
    std::unordered_map<size_t, Node *> id_to_node;
    for (size_t i = 0; i < nodes_size; ++i)
        id_to_node.emplace(i, &nodes.emplace_back(Node{}));

    for (size_t i = 0; i < nodes_size; ++i)
    {
        Node & node = *id_to_node.at(i);

        UInt8 action_type{0};
        readIntBinary(action_type, in);
        if (action_type > static_cast<UInt8>(ActionType::FUNCTION))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown action type {}", size_t(action_type));
        node.type = static_cast<ActionType>(action_type);

        readStringBinary(node.result_name, in);
        node.result_type = decodeDataType(in);

        size_t children_size;
        readVarUInt(children_size, in);
        for (size_t j = 0; j < children_size; ++j)
        {
            size_t child_id;
            readVarUInt(child_id, in);
            node.children.push_back(id_to_node.at(child_id));
        }

        UInt8 column_flags = 0;
        readIntBinary(column_flags, in);

        /// Deserialize column if it is present
        if (column_flags & 1)
        {
            if ((column_flags & 2) == 0)
                node.is_deterministic_constant = false;

            node.column = deserializeConstant(*node.result_type, in, registry, context);
        }

        if (node.type == ActionType::INPUT)
        {
            /// nothing to deserialize
            if (!node.children.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Deserialized input can't have children");
        }
        else if (node.type == ActionType::COLUMN)
        {
            /// Column is already deserialized
            if (!node.children.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Deserialized column can't have children");
        }
        else if (node.type == ActionType::ALIAS)
        {
            /// nothing to deserialize
            if (node.children.size() != 1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Deserialized alias must have one children");
        }
        else if (node.type == ActionType::FUNCTION)
        {
            String function_name;
            readStringBinary(function_name, in);

            ColumnsWithTypeAndName arguments;
            arguments.reserve(node.children.size());
            for (const auto * child : node.children)
            {
                ColumnWithTypeAndName argument;
                argument.column = child->column;
                argument.type = child->result_type;
                argument.name = child->result_name;

                arguments.emplace_back(std::move(argument));
            }

            if (column_flags & 4)
            {
                LambdaCapture capture;
                deserializeCapture(capture, in);
                auto capture_dag = ActionsDAG::deserialize(in, registry, context);

                node.function_base = std::make_shared<FunctionCapture>(
                    std::make_shared<ExpressionActions>(
                        std::move(capture_dag),
                        ExpressionActionsSettings(context, CompileExpressions::yes)),
                    std::make_shared<LambdaCapture>(std::move(capture)),
                    node.result_type,
                    function_name);

                node.function = node.function_base->prepare(arguments);
                node.is_function_compiled = false;
            }
            else
            {
                auto function = FunctionFactory::instance().get(function_name, context);

                node.function_base = function->build(arguments);
                node.function = node.function_base->prepare(arguments);
                node.is_function_compiled = false;

                auto lhs_type = node.result_type;
                if (const auto * lhs_tuple = typeid_cast<const DataTypeTuple *>(lhs_type.get()))
                    lhs_type = std::make_shared<DataTypeTuple>(lhs_tuple->getElements());

                auto rhs_type = node.function_base->getResultType();
                if (const auto * rhs_tuple = typeid_cast<const DataTypeTuple *>(rhs_type.get()))
                    rhs_type = std::make_shared<DataTypeTuple>(rhs_tuple->getElements());

                if (!lhs_type->equals(*rhs_type))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Deserialized function {} has invalid type. Expected {}, deserialized {}.",
                        function_name,
                        rhs_type->getName(),
                        lhs_type->getName());
            }
        }
        else if (node.type == ActionType::ARRAY_JOIN)
        {
            /// nothing to deserialize
            if (node.children.size() != 1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Deserialized array join must have one children");
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown node type {}", static_cast<size_t>(node.type));
        }
    }

    size_t inputs_size;
    readVarUInt(inputs_size, in);
    std::vector<const Node *> inputs;
    std::unordered_set<const Node *> inputs_set;
    inputs.reserve(inputs_size);
    for (size_t i = 0; i < inputs_size; ++i)
    {
        size_t input_id;
        readVarUInt(input_id, in);
        const auto * input = id_to_node.at(input_id);

        if (input->type != ActionType::INPUT)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Deserialized input {} has type {}",
                input->result_name, magic_enum::enum_name(input->type));

        if (!inputs_set.emplace(input).second)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Duplicate input {}", input->result_name);

        inputs.push_back(input);
    }

    size_t outputs_size;
    readVarUInt(outputs_size, in);
    std::vector<const Node *> outputs;
    outputs.reserve(outputs_size);
    for (size_t i = 0; i < outputs_size; ++i)
    {
        size_t output_id;
        readVarUInt(output_id, in);
        outputs.push_back(id_to_node.at(output_id));
    }

    for (const auto & node : nodes)
        if (node.type == ActionType::INPUT && !inputs_set.contains(&node))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Deserialized input {} is not in the inputs list",
                node.result_name);

    ActionsDAG dag;
    dag.nodes = std::move(nodes);
    dag.inputs = std::move(inputs);
    dag.outputs = std::move(outputs);

    return dag;
}

Names getRequiredOutputNamesInOrder(NameMultiSet required_outputs, const ActionsDAG & actions_dag)
{
    Names new_required_outputs;
    new_required_outputs.reserve(required_outputs.size());
    for (const auto & output: actions_dag.getOutputs())
    {
        if (auto it = required_outputs.find(output->result_name); it != required_outputs.end())
        {
            auto node = required_outputs.extract(it);
            new_required_outputs.push_back(std::move(node.value()));
        }
    }
    return new_required_outputs;
}

bool hasDuplicatedNames(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    NameSet unique_names;
    for (const auto * node: nodes)
    {
        auto [_, inserted] = unique_names.insert(node->result_name);
        if (!inserted)
            return true;
    }
    return false;
}

bool hasDuplicatedNamesInInputOrOutputs(const ActionsDAG & actions_dag)
{
    return hasDuplicatedNames(actions_dag.getInputs()) || hasDuplicatedNames(actions_dag.getOutputs());
}

}
