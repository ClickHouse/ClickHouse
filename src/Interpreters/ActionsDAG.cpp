#include <Interpreters/ActionsDAG.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/materialize.h>
#include <Functions/FunctionsLogical.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <stack>
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
}

const char * ActionsDAG::typeToString(ActionsDAG::ActionType type)
{
    switch (type)
    {
        case ActionType::INPUT:
            return "Input";
        case ActionType::COLUMN:
            return "Column";
        case ActionType::ALIAS:
            return "Alias";
        case ActionType::ARRAY_JOIN:
            return "ArrayJoin";
        case ActionType::FUNCTION:
            return "Function";
    }

    __builtin_unreachable();
}

void ActionsDAG::Node::toTree(JSONBuilder::JSONMap & map) const
{
    map.add("Node Type", ActionsDAG::typeToString(type));

    if (result_type)
        map.add("Result Type", result_type->getName());

    if (!result_name.empty())
        map.add("Result Type", ActionsDAG::typeToString(type));

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
        index.push_back(&addInput(input.name, input.type));
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
            index.push_back(&addColumn(input));
        }
        else
            index.push_back(&addInput(input.name, input.type));
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

const ActionsDAG::Node & ActionsDAG::findInIndex(const std::string & name) const
{
    if (const auto * node = tryFindInIndex(name))
        return *node;

    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier: '{}'", name);
}

const ActionsDAG::Node * ActionsDAG::tryFindInIndex(const std::string & name) const
{
    for (const auto & node : index)
        if (node->result_name == name)
            return node;

    return nullptr;
}

void ActionsDAG::addOrReplaceInIndex(const Node & node)
{
    for (auto & index_node : index)
    {
        if (index_node->result_name == node.result_name)
        {
            index_node = &node;
            return;
        }
    }

    index.push_back(&node);
}

NamesAndTypesList ActionsDAG::getRequiredColumns() const
{
    NamesAndTypesList result;
    for (const auto & input : inputs)
        result.emplace_back(input->result_name, input->result_type);

    return result;
}

Names ActionsDAG::getRequiredColumnsNames() const
{
    Names result;
    result.reserve(inputs.size());

    for (const auto & input : inputs)
        result.emplace_back(input->result_name);

    return result;
}

ColumnsWithTypeAndName ActionsDAG::getResultColumns() const
{
    ColumnsWithTypeAndName result;
    result.reserve(index.size());
    for (const auto & node : index)
        result.emplace_back(node->column, node->result_type, node->result_name);

    return result;
}

NamesAndTypesList ActionsDAG::getNamesAndTypesList() const
{
    NamesAndTypesList result;
    for (const auto & node : index)
        result.emplace_back(node->result_name, node->result_type);

    return result;
}

Names ActionsDAG::getNames() const
{
    Names names;
    names.reserve(index.size());
    for (const auto & node : index)
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

void ActionsDAG::removeUnusedActions(const NameSet & required_names)
{
    NodeRawConstPtrs required_nodes;
    required_nodes.reserve(required_names.size());

    NameSet added;
    for (const auto & node : index)
    {
        if (required_names.count(node->result_name) && added.count(node->result_name) == 0)
        {
            required_nodes.push_back(node);
            added.insert(node->result_name);
        }
    }

    if (added.size() < required_names.size())
    {
        for (const auto & name : required_names)
            if (added.count(name) == 0)
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                                "Unknown column: {}, there are only columns {}", name, dumpNames());
    }

    index.swap(required_nodes);
    removeUnusedActions();
}

void ActionsDAG::removeUnusedActions(const Names & required_names)
{
    NodeRawConstPtrs required_nodes;
    required_nodes.reserve(required_names.size());

    std::unordered_map<std::string_view, const Node *> names_map;
    for (const auto * node : index)
        names_map[node->result_name] = node;

    for (const auto & name : required_names)
    {
        auto it = names_map.find(name);
        if (it == names_map.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", name, dumpDAG());

        required_nodes.push_back(it->second);
    }

    index.swap(required_nodes);
    removeUnusedActions();
}

void ActionsDAG::removeUnusedActions(bool allow_remove_inputs)
{
    std::unordered_set<const Node *> visited_nodes;
    std::stack<Node *> stack;

    for (const auto * node : index)
    {
        visited_nodes.insert(node);
        stack.push(const_cast<Node *>(node));
    }

    for (auto & node : nodes)
    {
        /// We cannot remove arrayJoin because it changes the number of rows.
        bool is_array_join = node.type == ActionType::ARRAY_JOIN;

        if (is_array_join && visited_nodes.count(&node) == 0)
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

        if (!node->children.empty() && node->column && isColumnConst(*node->column))
        {
            /// Constant folding.
            node->type = ActionsDAG::ActionType::COLUMN;
            node->children.clear();
        }

        for (const auto * child : node->children)
        {
            if (visited_nodes.count(child) == 0)
            {
                stack.push(const_cast<Node *>(child));
                visited_nodes.insert(child);
            }
        }
    }

    nodes.remove_if([&](const Node & node) { return visited_nodes.count(&node) == 0; });
    auto it = std::remove_if(inputs.begin(), inputs.end(), [&](const Node * node) { return visited_nodes.count(node) == 0; });
    inputs.erase(it, inputs.end());
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
            // bool all_args_are_const = true;

            // for (const auto & argument : arguments)
            //     if (typeid_cast<const ColumnConst *>(argument.column.get()) == nullptr)
            //         all_args_are_const = false;

            res_column.column = node->function->execute(arguments, res_column.type, 0, true);

            // if (!all_args_are_const)
            //     res_column.column = res_column.column->convertToFullColumnIfConst();

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
                node_to_column[inputs[list.front()]] = std::move(col);
                list.pop_front();
            }
        }
    }

    ColumnsWithTypeAndName result_columns;
    result_columns.reserve(index.size());

    struct Frame
    {
        const Node * node = nullptr;
        size_t next_child = 0;
    };

    {
        for (const auto * output : index)
        {
            if (node_to_column.count(output) == 0)
            {
                std::stack<Frame> stack;
                stack.push({.node = output});

                while (!stack.empty())
                {
                    auto & frame = stack.top();
                    const auto * node = frame.node;

                    while (frame.next_child < node->children.size())
                    {
                        const auto * child = node->children[frame.next_child];
                        if (node_to_column.count(child) == 0)
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

            if (node_to_column[output].column)
                result_columns.push_back(node_to_column[output]);
        }
    }

    if (isInputProjected())
        header.clear();
    else
        header.erase(pos_to_remove);

    Block res;

    for (auto & col : result_columns)
        res.insert(std::move(col));

    for (const auto & item : header)
        res.insert(std::move(item));

    return res;
}

NameSet ActionsDAG::foldActionsByProjection(
    const NameSet & required_columns, const Block & projection_block_for_keys, const String & predicate_column_name, bool add_missing_keys)
{
    std::unordered_set<const Node *> visited_nodes;
    std::unordered_set<std::string_view> visited_index_names;
    std::stack<Node *> stack;
    std::vector<const ColumnWithTypeAndName *> missing_input_from_projection_keys;

    for (const auto & node : index)
    {
        if (required_columns.find(node->result_name) != required_columns.end() || node->result_name == predicate_column_name)
        {
            visited_nodes.insert(node);
            visited_index_names.insert(node->result_name);
            stack.push(const_cast<Node *>(node));
        }
    }

    if (add_missing_keys)
    {
        for (const auto & column : required_columns)
        {
            if (visited_index_names.find(column) == visited_index_names.end())
            {
                if (const ColumnWithTypeAndName * column_with_type_name = projection_block_for_keys.findByName(column))
                {
                    const auto * node = &addInput(*column_with_type_name);
                    visited_nodes.insert(node);
                    index.push_back(node);
                    visited_index_names.insert(column);
                }
                else
                {
                    // Missing column
                    return {};
                }
            }
        }
    }

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
                node->result_type = std::move(column_with_type_name->type);
                node->result_name = std::move(column_with_type_name->name);
                node->children.clear();
                inputs.push_back(node);
            }
        }

        for (const auto * child : node->children)
        {
            if (visited_nodes.count(child) == 0)
            {
                stack.push(const_cast<Node *>(child));
                visited_nodes.insert(child);
            }
        }
    }

    std::erase_if(inputs, [&](const Node * node) { return visited_nodes.count(node) == 0; });
    std::erase_if(index, [&](const Node * node) { return visited_index_names.count(node->result_name) == 0; });
    nodes.remove_if([&](const Node & node) { return visited_nodes.count(&node) == 0; });

    NameSet next_required_columns;
    for (const auto & input : inputs)
        next_required_columns.insert(input->result_name);

    return next_required_columns;
}

void ActionsDAG::reorderAggregationKeysForProjection(const std::unordered_map<std::string_view, size_t> & key_names_pos_map)
{
    std::sort(index.begin(), index.end(), [&key_names_pos_map](const Node * lhs, const Node * rhs)
    {
        return key_names_pos_map.find(lhs->result_name)->second < key_names_pos_map.find(rhs->result_name)->second;
    });
}

void ActionsDAG::addAggregatesViaProjection(const Block & aggregates)
{
    for (const auto & aggregate : aggregates)
        index.push_back(&addInput(aggregate));
}

void ActionsDAG::addAliases(const NamesWithAliases & aliases)
{
    std::unordered_map<std::string_view, size_t> names_map;
    for (size_t i = 0; i < index.size(); ++i)
        names_map[index[i]->result_name] = i;

    NodeRawConstPtrs required_nodes;
    required_nodes.reserve(aliases.size());

    for (const auto & item : aliases)
    {
        auto it = names_map.find(item.first);
        if (it == names_map.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", item.first, dumpNames());

        required_nodes.push_back(index[it->second]);
    }

    for (size_t i = 0; i < aliases.size(); ++i)
    {
        const auto & item = aliases[i];
        const auto * child = required_nodes[i];

        if (!item.second.empty() && item.first != item.second)
        {
            Node node;
            node.type = ActionType::ALIAS;
            node.result_type = child->result_type;
            node.result_name = std::move(item.second);
            node.column = child->column;
            node.children.emplace_back(child);

            child = &addNode(std::move(node));
        }

        auto it = names_map.find(child->result_name);
        if (it == names_map.end())
        {
            names_map[child->result_name] = index.size();
            index.push_back(child);
        }
        else
            index[it->second] = child;
    }
}

void ActionsDAG::project(const NamesWithAliases & projection)
{
    std::unordered_map<std::string_view, const Node *> names_map;
    for (const auto * node : index)
        names_map.emplace(node->result_name, node);

    index.clear();
    index.reserve(projection.size());

    for (const auto & item : projection)
    {
        auto it = names_map.find(item.first);
        if (it == names_map.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", item.first, dumpNames());

        index.push_back(it->second);
    }

    for (size_t i = 0; i < projection.size(); ++i)
    {
        const auto & item = projection[i];
        auto & child = index[i];

        if (!item.second.empty() && item.first != item.second)
        {
            Node node;
            node.type = ActionType::ALIAS;
            node.result_type = child->result_type;
            node.result_name = std::move(item.second);
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
    for (const auto * node : index)
        if (node->result_name == column_name)
            return true;

    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it)
    {
        auto & node = *it;
        if (node.result_name == column_name)
        {
            index.push_back(&node);
            return true;
        }
    }

    return false;
}

bool ActionsDAG::removeUnusedResult(const std::string & column_name)
{
    /// Find column in index and remove.
    const Node * col;
    {
        auto it = index.begin();
        for (; it != index.end(); ++it)
            if ((*it)->result_name == column_name)
                break;

        if (it == index.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found result {} in ActionsDAG\n{}", column_name, dumpDAG());

        col = *it;
        index.erase(it);
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

    /// Do not remove input if it was mentioned in index several times.
    for (const auto * node : index)
        if (col == node)
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

    for (const auto & node : index)
        actions->index.push_back(copy_map[node]);

    for (const auto & node : inputs)
        actions->inputs.push_back(copy_map[node]);

    return actions;
}

#if USE_EMBEDDED_COMPILER
void ActionsDAG::compileExpressions(size_t min_count_to_compile_expression)
{
    compileFunctions(min_count_to_compile_expression);
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

    out << "Index:";
    for (const auto * node : index)
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

void ActionsDAG::addMaterializingOutputActions()
{
    for (auto & node : index)
        node = &materializeNode(*node);
}

const ActionsDAG::Node & ActionsDAG::materializeNode(const Node & node)
{
    FunctionOverloadResolverPtr func_builder_materialize = std::make_unique<FunctionToOverloadResolverAdaptor>(
                            std::make_shared<FunctionMaterialize>());

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
        for (size_t pos = 0; pos < actions_dag->inputs.size(); ++pos)
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
                        throw Exception("Cannot find column " + backQuote(res_elem.name) + " in source stream",
                                        ErrorCodes::THERE_IS_NO_COLUMN);
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
                    throw Exception("Cannot convert column " + backQuote(res_elem.name) + " because "
                                    "it is constant but values of constants are different in source and result",
                                    ErrorCodes::ILLEGAL_COLUMN);
            }
            else
                throw Exception("Cannot convert column " + backQuote(res_elem.name) + " because "
                                "it is non constant in source stream but must be constant in result",
                                ErrorCodes::ILLEGAL_COLUMN);
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

            FunctionCast::Diagnostic diagnostic = {dst_node->result_name, res_elem.name};
            FunctionOverloadResolverPtr func_builder_cast = CastOverloadResolver<CastType::nonAccurate>::createImpl(false, std::move(diagnostic));

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
                    throw Exception("Cannot convert column " + backQuote(res_elem.name) +
                                    " to "+ backQuote(dst_node->result_name) +
                                    " because other column have same name",
                                    ErrorCodes::ILLEGAL_COLUMN);
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

    actions_dag->index.swap(projection);
    actions_dag->removeUnusedActions();
    actions_dag->projectInput();

    return actions_dag;
}

ActionsDAGPtr ActionsDAG::makeAddingColumnActions(ColumnWithTypeAndName column)
{
    auto adding_column_action = std::make_shared<ActionsDAG>();
    FunctionOverloadResolverPtr func_builder_materialize = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMaterialize>());

    auto column_name = column.name;
    const auto * column_node = &adding_column_action->addColumn(std::move(column));
    NodeRawConstPtrs inputs = {column_node};
    const auto & function_node = adding_column_action->addFunction(func_builder_materialize, std::move(inputs), {});
    const auto & alias_node = adding_column_action->addAlias(function_node, std::move(column_name));

    adding_column_action->index.push_back(&alias_node);
    return adding_column_action;
}

ActionsDAGPtr ActionsDAG::merge(ActionsDAG && first, ActionsDAG && second)
{
    /// first: x (1), x (2), y ==> x (2), z, x (3)
    /// second: x (1), x (2), x (3) ==> x (3), x (2), x (1)
    /// merge: x (1), x (2), x (3), y =(first)=> x (2), z, x (4), x (3) =(second)=> x (3), x (4), x (2), z

    /// Will store merged result in `first`.

    /// This map contains nodes which should be removed from `first` index, cause they are used as inputs for `second`.
    /// The second element is the number of removes (cause one node may be repeated several times in result).
    std::unordered_map<const Node *, size_t> removed_first_result;
    /// Map inputs of `second` to nodes of `first`.
    std::unordered_map<const Node *, const Node *> inputs_map;

    /// Update inputs list.
    {
        /// Index may have multiple columns with same name. They also may be used by `second`. Order is important.
        std::unordered_map<std::string_view, std::list<const Node *>> first_result;
        for (const auto & node : first.index)
            first_result[node->result_name].push_back(node);

        for (const auto & node : second.inputs)
        {
            auto it = first_result.find(node->result_name);
            if (it == first_result.end() || it->second.empty())
            {
                if (first.project_input)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Cannot find column {} in ActionsDAG result", node->result_name);

                first.inputs.push_back(node);
            }
            else
            {
                inputs_map[node] = it->second.front();
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

    for (auto & node : second.index)
    {
        if (node->type == ActionType::INPUT)
        {
            auto it = inputs_map.find(node);
            if (it != inputs_map.end())
                node = it->second;
        }
    }

    /// Update index.
    if (second.project_input)
    {
        first.index.swap(second.index);
        first.project_input = true;
    }
    else
    {
        /// Add not removed result from first actions.
        for (const auto * node : first.index)
        {
            auto it = removed_first_result.find(node);
            if (it != removed_first_result.end() && it->second > 0)
                --it->second;
            else
                second.index.push_back(node);
        }

        first.index.swap(second.index);
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
    /// (first_nodes, first_index) is a part which will have split_list in result.
    /// (second_nodes, second_index) is a part which will have same index as current actions.
    Nodes second_nodes;
    Nodes first_nodes;
    NodeRawConstPtrs second_index;
    NodeRawConstPtrs first_index;

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

    for (const auto & node : index)
        data[node].used_in_result = true;

    /// DFS. Decide if node is needed by split.
    for (const auto & node : nodes)
    {
        if (split_nodes.count(&node) == 0)
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

    for (const auto * node : index)
        second_index.push_back(data[node].to_second);

    NodeRawConstPtrs second_inputs;
    NodeRawConstPtrs first_inputs;

    for (const auto * input : inputs)
    {
        const auto & cur = data[input];
        first_inputs.push_back(cur.to_first);
    }

    for (const auto * input : new_inputs)
    {
        const auto & cur = data[input];
        second_inputs.push_back(cur.to_second);
        first_index.push_back(cur.to_first);
    }

    auto first_actions = std::make_shared<ActionsDAG>();
    first_actions->nodes.swap(first_nodes);
    first_actions->index.swap(first_index);
    first_actions->inputs.swap(first_inputs);

    auto second_actions = std::make_shared<ActionsDAG>();
    second_actions->nodes.swap(second_nodes);
    second_actions->index.swap(second_index);
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
        if (visited_nodes.count(&node))
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

                if (visited_nodes.count(child) == 0)
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
                if (cur.node->type == ActionType::INPUT && array_joined_columns.count(cur.node->result_name))
                    depend_on_array_join = true;

                for (const auto * child : cur.node->children)
                {
                    if (split_nodes.count(child) == 0)
                        depend_on_array_join = true;
                }

                if (!depend_on_array_join)
                    split_nodes.insert(cur.node);

                stack.pop();
            }
        }
    }

    auto res = split(split_nodes);
    /// Do not remove array joined columns if they are not used.
    /// res.first->project_input = false;
    return res;
}

ActionsDAG::SplitResult ActionsDAG::splitActionsForFilter(const std::string & column_name) const
{
    const auto * node = tryFindInIndex(column_name);
    if (!node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Index for ActionsDAG does not contain filter column name {}. DAG:\n{}",
                        column_name, dumpDAG());

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

    struct Frame
    {
        const ActionsDAG::Node * node = nullptr;
        /// Node is a part of predicate (predicate itself, or some part of AND)
        bool is_predicate = false;
        size_t next_child_to_visit = 0;
        size_t num_allowed_children = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const ActionsDAG::Node *> visited_nodes;

    stack.push(Frame{.node = predicate, .is_predicate = true});
    visited_nodes.insert(predicate);
    while (!stack.empty())
    {
        auto & cur = stack.top();
        bool is_conjunction = cur.is_predicate
                                && cur.node->type == ActionsDAG::ActionType::FUNCTION
                                && cur.node->function_base->getName() == "and";

        /// At first, visit all children.
        while (cur.next_child_to_visit < cur.node->children.size())
        {
            const auto * child = cur.node->children[cur.next_child_to_visit];

            if (visited_nodes.count(child) == 0)
            {
                visited_nodes.insert(child);
                stack.push({.node = child, .is_predicate = is_conjunction});
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

            /// Add parts of AND to result. Do not add function AND.
            if (cur.is_predicate && ! is_conjunction)
            {
                if (allowed_nodes.count(cur.node))
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

/// Create actions which calculate conjunction of selected nodes.
/// Assume conjunction nodes are predicates (and may be used as arguments of function AND).
///
/// Result actions add single column with conjunction result (it is always first in index).
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
        if (nodes_mapping.count(predicate))
            continue;

        stack.push({.node = predicate});
        while (!stack.empty())
        {
            auto & cur = stack.top();
            /// At first, visit all children.
            while (cur.next_child_to_visit < cur.node->children.size())
            {
                const auto * child = cur.node->children[cur.next_child_to_visit];

                if (nodes_mapping.count(child) == 0)
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

    actions->index.push_back(result_predicate);

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

        actions->index.push_back(input);
    }

    return actions;
}

ActionsDAGPtr ActionsDAG::cloneActionsForFilterPushDown(
    const std::string & filter_name,
    bool can_remove_filter,
    const Names & available_inputs,
    const ColumnsWithTypeAndName & all_inputs)
{
    Node * predicate = const_cast<Node *>(tryFindInIndex(filter_name));
    if (!predicate)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Index for ActionsDAG does not contain filter column name {}. DAG:\n{}",
                            filter_name, dumpDAG());

    /// If condition is constant let's do nothing.
    /// It means there is nothing to push down or optimization was already applied.
    if (predicate->type == ActionType::COLUMN)
        return nullptr;

    std::unordered_set<const Node *> allowed_nodes;

    /// Get input nodes from available_inputs names.
    {
        std::unordered_map<std::string_view, std::list<const Node *>> inputs_map;
        for (const auto & input : inputs)
            inputs_map[input->result_name].emplace_back(input);

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
            /// If filter column is not needed, remove it from index.
            for (auto i = index.begin(); i != index.end(); ++i)
            {
                if (*i == predicate)
                {
                    index.erase(i);
                    break;
                }
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
                /// So we just add a new constant and update index.
                const auto * new_predicate = &addNode(node);
                for (auto & index_node : index)
                    if (index_node == predicate)
                        index_node = new_predicate;
            }
        }

        removeUnusedActions(false);
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

                FunctionOverloadResolverPtr func_builder_cast = CastOverloadResolver<CastType::nonAccurate>::createImpl(false);

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

        removeUnusedActions(false);
    }

    return actions;
}

}
