#include <Interpreters/ActionsDAG.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/materialize.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/CastOverloadResolver.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Core/SortDescription.h>

#include <stack>
#include <base/sort.h>
#include <Common/JSONBuilder.h>

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
    else if (function_builder)
        map.add("Function", function_builder->getName());

    if (type == ActionType::FUNCTION)
        map.add("Compiled", is_function_compiled);
}


ActionsDAG::ActionsDAG(const NamesAndTypesList & inputs_)
{
    for (const auto & input : inputs_)
        outputs.push_back(&addInput(input.name, input.type));
}

ActionsDAG::ActionsDAG(const ColumnsWithTypeAndName & inputs_)
{
    for (const auto & input : inputs_)
    {
        if (input.column && isColumnConst(*input.column))
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

ActionsDAG::Node & ActionsDAG::addNode(Node node)
{
    auto & res = nodes.emplace_back(std::move(node));

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

const ActionsDAG::Node & ActionsDAG::addColumn(ColumnWithTypeAndName column)
{
    if (!column.column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add column {} because it is nullptr", column.name);

    Node node;
    node.type = ActionType::COLUMN;
    node.result_type = std::move(column.type);
    node.result_name = std::move(column.name);
    node.column = std::move(column.column);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addAlias(const Node & child, std::string alias)
{
    Node node;
    node.type = ActionType::ALIAS;
    node.result_type = child.result_type;
    node.result_name = std::move(alias);
    node.column = child.column;
    node.children.emplace_back(&child);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addArrayJoin(const Node & child, std::string result_name)
{
    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(child.result_type.get());
    if (!array_type)
        throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);

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
    size_t num_arguments = children.size();

    Node node;
    node.type = ActionType::FUNCTION;
    node.function_builder = function;
    node.children = std::move(children);

    bool all_const = true;
    ColumnsWithTypeAndName arguments(num_arguments);

    for (size_t i = 0; i < num_arguments; ++i)
    {
        const auto & child = *node.children[i];

        ColumnWithTypeAndName argument;
        argument.column = child.column;
        argument.type = child.result_type;
        argument.name = child.result_name;

        if (!argument.column || !isColumnConst(*argument.column))
            all_const = false;

        arguments[i] = std::move(argument);
    }

    node.function_base = function->build(arguments);
    node.result_type = node.function_base->getResultType();
    node.function = node.function_base->prepare(arguments);
    node.is_deterministic = node.function_base->isDeterministic();

    /// If all arguments are constants, and function is suitable to be executed in 'prepare' stage - execute function.
    if (node.function_base->isSuitableForConstantFolding())
    {
        ColumnPtr column;

        if (all_const)
        {
            size_t num_rows = arguments.empty() ? 0 : arguments.front().column->size();
            column = node.function->execute(arguments, node.result_type, num_rows, true);
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
        result_name = function->getName() + "(";
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

void ActionsDAG::addOrReplaceInOutputs(const Node & node)
{
    for (auto & output_node : outputs)
    {
        if (output_node->result_name == node.result_name)
        {
            output_node = &node;
            return;
        }
    }

    outputs.push_back(&node);
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

void ActionsDAG::removeUnusedActions(const NameSet & required_names, bool allow_remove_inputs, bool allow_constant_folding)
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
    removeUnusedActions(allow_remove_inputs, allow_constant_folding);
}

void ActionsDAG::removeUnusedActions(const Names & required_names, bool allow_remove_inputs, bool allow_constant_folding)
{
    NodeRawConstPtrs required_nodes;
    required_nodes.reserve(required_names.size());

    std::unordered_map<std::string_view, const Node *> names_map;
    for (const auto * node : outputs)
        names_map[node->result_name] = node;

    for (const auto & name : required_names)
    {
        auto it = names_map.find(name);
        if (it == names_map.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", name, dumpDAG());

        required_nodes.push_back(it->second);
    }

    outputs.swap(required_nodes);
    removeUnusedActions(allow_remove_inputs, allow_constant_folding);
}

void ActionsDAG::removeUnusedActions(bool allow_remove_inputs, bool allow_constant_folding)
{
    std::unordered_set<const Node *> visited_nodes;
    std::stack<Node *> stack;

    for (const auto * node : outputs)
    {
        visited_nodes.insert(node);
        stack.push(const_cast<Node *>(node));
    }

    for (auto & node : nodes)
    {
        /// We cannot remove arrayJoin because it changes the number of rows.
        bool is_array_join = node.type == ActionType::ARRAY_JOIN;

        if (is_array_join && !visited_nodes.contains(&node))
        {
            visited_nodes.insert(&node);
            stack.push(&node);
        }

        if (node.type == ActionType::INPUT && !allow_remove_inputs)
            visited_nodes.insert(&node);
    }

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        /// Constant folding.
        if (allow_constant_folding && !node->children.empty() && node->column && isColumnConst(*node->column))
        {
            node->type = ActionsDAG::ActionType::COLUMN;

            for (const auto & child : node->children)
            {
                if (!child->is_deterministic)
                {
                    node->is_deterministic = false;
                    break;
                }
            }

            node->children.clear();
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

    nodes.remove_if([&](const Node & node) { return !visited_nodes.contains(&node); });
    std::erase_if(inputs, [&](const Node * node) { return !visited_nodes.contains(node); });
}

static ColumnWithTypeAndName executeActionForHeader(const ActionsDAG::Node * node, ColumnsWithTypeAndName arguments)
{
    ColumnWithTypeAndName res_column;
    res_column.type = node->result_type;
    res_column.name = node->result_name;

    switch (node->type)
    {
        case ActionsDAG::ActionType::FUNCTION:
        {
            res_column.column = node->function->execute(arguments, res_column.type, 0, true);
            break;
        }

        case ActionsDAG::ActionType::ARRAY_JOIN:
        {
            auto key = arguments.at(0);
            key.column = key.column->convertToFullColumnIfConst();

            const ColumnArray * array = typeid_cast<const ColumnArray *>(key.column.get());
            if (!array)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                                "ARRAY JOIN of not array: {}", node->result_name);

            res_column.column = array->getDataPtr()->cloneEmpty();
            break;
        }

        case ActionsDAG::ActionType::COLUMN:
        {
            res_column.column = node->column->cloneResized(0);
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
    }

    return res_column;
}

Block ActionsDAG::updateHeader(Block header) const
{
    std::unordered_map<const Node *, ColumnWithTypeAndName> node_to_column;
    std::set<size_t> pos_to_remove;

    {
        std::unordered_map<std::string_view, std::list<size_t>> input_positions;

        for (size_t pos = 0; pos < inputs.size(); ++pos)
            input_positions[inputs[pos]->result_name].emplace_back(pos);

        for (size_t pos = 0; pos < header.columns(); ++pos)
        {
            const auto & col = header.getByPosition(pos);
            auto it = input_positions.find(col.name);
            if (it != input_positions.end() && !it->second.empty())
            {
                auto & list = it->second;
                pos_to_remove.insert(pos);
                node_to_column[inputs[list.front()]] = col;
                list.pop_front();
            }
        }
    }

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
                        if (!node_to_column.contains(child))
                        {
                            stack.push({.node = child});
                            break;
                        }

                        ++frame.next_child;
                    }

                    if (frame.next_child < node->children.size())
                        continue;

                    stack.pop();

                    ColumnsWithTypeAndName arguments(node->children.size());
                    for (size_t i = 0; i < arguments.size(); ++i)
                    {
                        arguments[i] = node_to_column[node->children[i]];
                        if (!arguments[i].column)
                            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                                            "Not found column {} in block", node->children[i]->result_name);
                    }

                    node_to_column[node] = executeActionForHeader(node, std::move(arguments));
                }
            }

            if (node_to_column[output_node].column)
                result_columns.push_back(node_to_column[output_node]);
        }
    }

    if (isInputProjected())
        header.clear();
    else
        header.erase(pos_to_remove);

    Block res;

    for (auto & col : result_columns)
        res.insert(std::move(col));

    for (auto && item : header)
        res.insert(std::move(item));

    return res;
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
        if (required_columns.find(output_node->result_name) != required_columns.end() || output_node->result_name == predicate_column_name)
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
            if (visited_output_nodes_names.find(column) == visited_output_nodes_names.end())
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

void ActionsDAG::reorderAggregationKeysForProjection(const std::unordered_map<std::string_view, size_t> & key_names_pos_map)
{
    ::sort(outputs.begin(), outputs.end(), [&key_names_pos_map](const Node * lhs, const Node * rhs)
    {
        return key_names_pos_map.find(lhs->result_name)->second < key_names_pos_map.find(rhs->result_name)->second;
    });
}

void ActionsDAG::addAggregatesViaProjection(const Block & aggregates)
{
    for (const auto & aggregate : aggregates)
        outputs.push_back(&addInput(aggregate));
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
    projectInput();
    projected_output = true;
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

    if (it == inputs.end())
        return false;

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
        if (&(*jt) == *it)
        {
            nodes.erase(jt);
            break;
        }
    }

    inputs.erase(it);
    return true;
}

ActionsDAGPtr ActionsDAG::clone() const
{
    auto actions = std::make_shared<ActionsDAG>();
    actions->project_input = project_input;
    actions->projected_output = projected_output;

    std::unordered_map<const Node *, Node *> copy_map;

    for (const auto & node : nodes)
    {
        auto & copy_node = actions->nodes.emplace_back(node);
        copy_map[&node] = &copy_node;
    }

    for (auto & node : actions->nodes)
        for (auto & child : node.children)
            child = copy_map[child];

    for (const auto & output_node : outputs)
        actions->outputs.push_back(copy_map[output_node]);

    for (const auto & input_node : inputs)
        actions->inputs.push_back(copy_map[input_node]);

    return actions;
}

#if USE_EMBEDDED_COMPILER
void ActionsDAG::compileExpressions(size_t min_count_to_compile_expression, const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes)
{
    compileFunctions(min_count_to_compile_expression, lazy_executed_nodes);
    removeUnusedActions();
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

bool ActionsDAG::hasArrayJoin() const
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

bool ActionsDAG::trivial() const
{
    for (const auto & node : nodes)
        if (node.type == ActionType::FUNCTION || node.type == ActionType::ARRAY_JOIN)
            return false;

    return true;
}

void ActionsDAG::assertDeterministic() const
{
    for (const auto & node : nodes)
        if (!node.is_deterministic)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expression must be deterministic but it contains non-deterministic part `{}`", node.result_name);
}

void ActionsDAG::addMaterializingOutputActions()
{
    for (auto & output_node : outputs)
        output_node = &materializeNode(*output_node);
}

const ActionsDAG::Node & ActionsDAG::materializeNode(const Node & node)
{
    FunctionOverloadResolverPtr func_builder_materialize
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMaterialize>());

    const auto & name = node.result_name;
    const auto * func = &addFunction(func_builder_materialize, {&node}, {});
    return addAlias(*func, name);
}

ActionsDAGPtr ActionsDAG::makeConvertingActions(
    const ColumnsWithTypeAndName & source,
    const ColumnsWithTypeAndName & result,
    MatchColumnsMode mode,
    bool ignore_constant_values,
    bool add_casted_columns,
    NameToNameMap * new_names)
{
    size_t num_input_columns = source.size();
    size_t num_result_columns = result.size();

    if (mode == MatchColumnsMode::Position && num_input_columns != num_result_columns)
        throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

    if (add_casted_columns && mode != MatchColumnsMode::Name)
        throw Exception("Converting with add_casted_columns supported only for MatchColumnsMode::Name", ErrorCodes::LOGICAL_ERROR);

    auto actions_dag = std::make_shared<ActionsDAG>(source);
    NodeRawConstPtrs projection(num_result_columns);

    FunctionOverloadResolverPtr func_builder_materialize = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMaterialize>());

    std::map<std::string_view, std::list<size_t>> inputs;
    if (mode == MatchColumnsMode::Name)
    {
        size_t input_nodes_size = actions_dag->inputs.size();
        for (size_t pos = 0; pos < input_nodes_size; ++pos)
            inputs[actions_dag->inputs[pos]->result_name].push_back(pos);
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
                src_node = dst_node = actions_dag->inputs[result_col_num];
                break;
            }

            case MatchColumnsMode::Name:
            {
                auto & input = inputs[res_elem.name];
                if (input.empty())
                {
                    const auto * res_const = typeid_cast<const ColumnConst *>(res_elem.column.get());
                    if (ignore_constant_values && res_const)
                        src_node = dst_node = &actions_dag->addColumn(res_elem);
                    else
                        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                                        "Cannot find column `{}` in source stream, there are only columns: [{}]",
                                        res_elem.name, Block(source).dumpNames());
                }
                else
                {
                    src_node = dst_node = actions_dag->inputs[input.front()];
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
                    dst_node = &actions_dag->addColumn(res_elem);
                else if (res_const->getField() != src_const->getField())
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Cannot convert column `{}` because it is constant but values of constants are different in source and result",
                        res_elem.name);
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

            const auto * right_arg = &actions_dag->addColumn(std::move(column));
            const auto * left_arg = dst_node;

            FunctionCastBase::Diagnostic diagnostic = {dst_node->result_name, res_elem.name};
            FunctionOverloadResolverPtr func_builder_cast
                = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

            NodeRawConstPtrs children = { left_arg, right_arg };
            dst_node = &actions_dag->addFunction(func_builder_cast, std::move(children), {});
        }

        if (dst_node->column && isColumnConst(*dst_node->column) && !(res_elem.column && isColumnConst(*res_elem.column)))
        {
            NodeRawConstPtrs children = {dst_node};
            dst_node = &actions_dag->addFunction(func_builder_materialize, std::move(children), {});
        }

        if (dst_node->result_name != res_elem.name)
        {
            if (add_casted_columns)
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
                dst_node = &actions_dag->addAlias(*dst_node, res_elem.name);
                projection[result_col_num] = dst_node;
            }
        }
        else
        {
            projection[result_col_num] = dst_node;
        }
    }

    actions_dag->outputs.swap(projection);
    actions_dag->removeUnusedActions();
    actions_dag->projectInput();

    return actions_dag;
}

ActionsDAGPtr ActionsDAG::makeAddingColumnActions(ColumnWithTypeAndName column)
{
    auto adding_column_action = std::make_shared<ActionsDAG>();
    FunctionOverloadResolverPtr func_builder_materialize
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMaterialize>());

    auto column_name = column.name;
    const auto * column_node = &adding_column_action->addColumn(std::move(column));
    NodeRawConstPtrs inputs = {column_node};
    const auto & function_node = adding_column_action->addFunction(func_builder_materialize, std::move(inputs), {});
    const auto & alias_node = adding_column_action->addAlias(function_node, std::move(column_name));

    adding_column_action->outputs.push_back(&alias_node);
    return adding_column_action;
}

ActionsDAGPtr ActionsDAG::merge(ActionsDAG && first, ActionsDAG && second)
{
    /// first: x (1), x (2), y ==> x (2), z, x (3)
    /// second: x (1), x (2), x (3) ==> x (3), x (2), x (1)
    /// merge: x (1), x (2), x (3), y =(first)=> x (2), z, x (4), x (3) =(second)=> x (3), x (4), x (2), z

    /// Will store merged result in `first`.

    /// This map contains nodes which should be removed from `first` outputs, cause they are used as inputs for `second`.
    /// The second element is the number of removes (cause one node may be repeated several times in result).
    std::unordered_map<const Node *, size_t> removed_first_result;
    /// Map inputs of `second` to nodes of `first`.
    std::unordered_map<const Node *, const Node *> inputs_map;

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
                if (first.project_input)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Cannot find column {} in ActionsDAG result", input_node->result_name);

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

    /// Update output nodes.
    if (second.project_input)
    {
        first.outputs.swap(second.outputs);
        first.project_input = true;
    }
    else
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

    first.nodes.splice(first.nodes.end(), std::move(second.nodes));

    first.projected_output = second.projected_output;

    /// Drop unused inputs and, probably, some actions.
    first.removeUnusedActions();

    return std::make_shared<ActionsDAG>(std::move(first));
}

ActionsDAG::SplitResult ActionsDAG::split(std::unordered_set<const Node *> split_nodes) const
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
            auto & cur_node = stack.top().node;
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
            auto & cur = stack.top();
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

                                new_inputs.push_back(child);
                            }
                        }

                        child = child_data.to_second;
                    }

                    /// Input from second DAG should also be in the first.
                    if (copy.type == ActionType::INPUT)
                    {
                        auto & input_copy = first_nodes.emplace_back(*cur.node);
                        assert(cur_data.to_first == nullptr);
                        cur_data.to_first = &input_copy;
                        new_inputs.push_back(cur.node);
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
                        input_node.result_type = node.result_type;
                        input_node.result_name = node.result_name;
                        cur_data.to_second = &second_nodes.emplace_back(std::move(input_node));

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
        first_inputs.push_back(cur.to_first);
    }

    for (const auto * input : new_inputs)
    {
        const auto & cur = data[input];
        second_inputs.push_back(cur.to_second);
        first_outputs.push_back(cur.to_first);
    }

    auto first_actions = std::make_shared<ActionsDAG>();
    first_actions->nodes.swap(first_nodes);
    first_actions->outputs.swap(first_outputs);
    first_actions->inputs.swap(first_inputs);

    auto second_actions = std::make_shared<ActionsDAG>();
    second_actions->nodes.swap(second_nodes);
    second_actions->outputs.swap(second_outputs);
    second_actions->inputs.swap(second_inputs);

    return {std::move(first_actions), std::move(second_actions)};
}

ActionsDAG::SplitResult ActionsDAG::splitActionsBeforeArrayJoin(const NameSet & array_joined_columns) const
{
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
                if (cur.node->type == ActionType::INPUT && array_joined_columns.contains(cur.node->result_name))
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
    res.second->project_input = project_input;
    return res;
}

ActionsDAG::SplitResult ActionsDAG::splitActionsBySortingDescription(const NameSet & sort_columns) const
{
    std::unordered_set<const Node *> split_nodes;
    for (const auto & sort_column : sort_columns)
        if (const auto * node = tryFindInOutputs(sort_column))
            split_nodes.insert(node);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Sorting column {} wasn't found in the ActionsDAG's outputs. DAG:\n{}",
                sort_column,
                dumpDAG());

    auto res = split(split_nodes);
    res.second->project_input = project_input;
    return res;
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
    res.second->project_input = project_input;
    return res;
}

namespace
{

struct ConjunctionNodes
{
    ActionsDAG::NodeRawConstPtrs allowed;
    ActionsDAG::NodeRawConstPtrs rejected;
};

/// Take a node which result is predicate.
/// Assuming predicate is a conjunction (probably, trivial).
/// Find separate conjunctions nodes. Split nodes into allowed and rejected sets.
/// Allowed predicate is a predicate which can be calculated using only nodes from allowed_nodes set.
ConjunctionNodes getConjunctionNodes(ActionsDAG::Node * predicate, std::unordered_set<const ActionsDAG::Node *> allowed_nodes)
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
                if (cur.node->type != ActionsDAG::ActionType::ARRAY_JOIN && cur.node->type != ActionsDAG::ActionType::INPUT)
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

    // std::cerr << "Allowed " << conjunction.allowed.size() << std::endl;
    // for (const auto & node : conjunction.allowed)
    //     std::cerr << node->result_name << std::endl;
    // std::cerr << "Rejected " << conjunction.rejected.size() << std::endl;
    // for (const auto & node : conjunction.rejected)
    //     std::cerr << node->result_name << std::endl;

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

/// Create actions which calculate conjunction of selected nodes.
/// Assume conjunction nodes are predicates (and may be used as arguments of function AND).
///
/// Result actions add single column with conjunction result (it is always first in outputs).
/// No other columns are added or removed.
ActionsDAGPtr ActionsDAG::cloneActionsForConjunction(NodeRawConstPtrs conjunction, const ColumnsWithTypeAndName & all_inputs)
{
    if (conjunction.empty())
        return nullptr;

    auto actions = std::make_shared<ActionsDAG>();

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
                auto & node = actions->nodes.emplace_back(*cur.node);
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

        result_predicate = &actions->addFunction(func_builder_and, std::move(args), {});
    }

    actions->outputs.push_back(result_predicate);

    for (const auto & col : all_inputs)
    {
        const Node * input;
        auto & list = required_inputs[col.name];
        if (list.empty())
            input = &actions->addInput(col);
        else
        {
            input = list.front();
            list.pop_front();
            actions->inputs.push_back(input);
        }

        /// We should not add result_predicate into the outputs for the second time.
        if (input->result_name != result_predicate->result_name)
            actions->outputs.push_back(input);
    }

    return actions;
}

ActionsDAGPtr ActionsDAG::cloneActionsForFilterPushDown(
    const std::string & filter_name,
    bool can_remove_filter,
    const Names & available_inputs,
    const ColumnsWithTypeAndName & all_inputs)
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
        return nullptr;

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

    auto conjunction = getConjunctionNodes(predicate, allowed_nodes);
    auto actions = cloneActionsForConjunction(conjunction.allowed, all_inputs);
    if (!actions)
        return nullptr;

    /// Now, when actions are created, update current DAG.

    if (conjunction.rejected.empty())
    {
        /// The whole predicate was split.
        if (can_remove_filter)
        {
            /// If filter column is not needed, remove it from output nodes.
            std::erase_if(outputs, [&](const Node * node) { return node == predicate; });

            /// At the very end of this method we'll call removeUnusedActions() with allow_remove_inputs=false,
            /// so we need to manually remove predicate if it is an input node.
            if (predicate->type == ActionType::INPUT)
            {
                std::erase_if(inputs, [&](const Node * node) { return node == predicate; });
                nodes.remove_if([&](const Node & node) { return &node == predicate; });
            }
        }
        else
        {
            /// Replace predicate result to constant 1.
            Node node;
            node.type = ActionType::COLUMN;
            node.result_name = std::move(predicate->result_name);
            node.result_type = std::move(predicate->result_type);
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
        }
    }
    else
    {
        /// Predicate is conjunction, where both allowed and rejected sets are not empty.
        /// Replace this node to conjunction of rejected predicates.

        NodeRawConstPtrs new_children = std::move(conjunction.rejected);

        if (new_children.size() == 1)
        {
            /// Rejected set has only one predicate.
            if (new_children.front()->result_type->equals(*predicate->result_type))
            {
                /// If it's type is same, just add alias.
                Node node;
                node.type = ActionType::ALIAS;
                node.result_name = predicate->result_name;
                node.result_type = predicate->result_type;
                node.children.swap(new_children);
                *predicate = std::move(node);
            }
            else
            {
                /// If type is different, cast column.
                /// This case is possible, cause AND can use any numeric type as argument.
                Node node;
                node.type = ActionType::COLUMN;
                node.result_name = predicate->result_type->getName();
                node.column = DataTypeString().createColumnConst(0, node.result_name);
                node.result_type = std::make_shared<DataTypeString>();

                const auto * right_arg = &nodes.emplace_back(std::move(node));
                const auto * left_arg = new_children.front();

                predicate->children = {left_arg, right_arg};
                auto arguments = prepareFunctionArguments(predicate->children);

                FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl();

                predicate->function_builder = func_builder_cast;
                predicate->function_base = predicate->function_builder->build(arguments);
                predicate->function = predicate->function_base->prepare(arguments);
            }
        }
        else
        {
            /// Predicate is function AND, which still have more then one argument.
            /// Just update children and rebuild it.
            predicate->children.swap(new_children);
            auto arguments = prepareFunctionArguments(predicate->children);

            predicate->function_base = predicate->function_builder->build(arguments);
            predicate->function = predicate->function_base->prepare(arguments);
        }
    }

    removeUnusedActions(false);
    return actions;
}

static bool chainPreservesSorting(const ActionsDAG::Node* chain)
{
    const Field field{};
    const ActionsDAG::Node* node = chain;
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

                /// TODO: we support monotonicity check only for functions with one parameter but ...
                ///       if one parameter is variable and other are constant then we can try to check monotonicity as well
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

static const ActionsDAG::Node* findColumn(const ActionsDAG::Node * start_node, const String & sorted_column)
{
    const ActionsDAG::Node * current = start_node;
    while (current)
    {
        /// if column found
        if (current->type == ActionsDAG::ActionType::INPUT && current->result_name == sorted_column)
            return current;

        if (current->children.empty())
            break;  /// column not found

        current = current->children.front();
    }
    return nullptr;
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
        bool found = false;
        for (const auto * output_node : outputs)
        {
            if (output_node->result_name == ignore_output_column)
                continue;

            if (findColumn(output_node, desc.column_name))
            {
                if (!chainPreservesSorting(output_node))
                    return false;

                found = true;
            }
        }
        if (!found)
            return false;
    }

    return true;
}

}
