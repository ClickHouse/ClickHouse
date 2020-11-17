#include <Interpreters/ActionsDAG.h>

#include <DataTypes/DataTypeArray.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionJIT.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int TYPE_MISMATCH;
}


ActionsDAG::ActionsDAG(const NamesAndTypesList & inputs)
{
    for (const auto & input : inputs)
        addInput(input.name, input.type);
}

ActionsDAG::ActionsDAG(const ColumnsWithTypeAndName & inputs)
{
    for (const auto & input : inputs)
    {
        if (input.column && isColumnConst(*input.column))
            addInput(input);
        else
            addInput(input.name, input.type);
    }
}

ActionsDAG::Node & ActionsDAG::addNode(Node node, bool can_replace)
{
    auto it = index.find(node.result_name);
    if (it != index.end() && !can_replace)
        throw Exception("Column '" + node.result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

    auto & res = nodes.emplace_back(std::move(node));

    index.replace(&res);
    return res;
}

ActionsDAG::Node & ActionsDAG::getNode(const std::string & name)
{
    auto it = index.find(name);
    if (it == index.end())
        throw Exception("Unknown identifier: '" + name + "'", ErrorCodes::UNKNOWN_IDENTIFIER);

    return **it;
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

const ActionsDAG::Node & ActionsDAG::addAlias(const std::string & name, std::string alias, bool can_replace)
{
    auto & child = getNode(name);

    Node node;
    node.type = ActionType::ALIAS;
    node.result_type = child.result_type;
    node.result_name = std::move(alias);
    node.column = child.column;
    node.allow_constant_folding = child.allow_constant_folding;
    node.children.emplace_back(&child);

    return addNode(std::move(node), can_replace);
}

const ActionsDAG::Node & ActionsDAG::addArrayJoin(const std::string & source_name, std::string result_name)
{
    auto & child = getNode(source_name);

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
        const Names & argument_names,
        std::string result_name,
        const Context & context [[maybe_unused]])
{
    const auto & all_settings = context.getSettingsRef();
    settings.max_temporary_columns = all_settings.max_temporary_columns;
    settings.max_temporary_non_const_columns = all_settings.max_temporary_non_const_columns;

#if USE_EMBEDDED_COMPILER
    settings.compile_expressions = all_settings.compile_expressions;
    settings.min_count_to_compile_expression = all_settings.min_count_to_compile_expression;

    if (!compilation_cache)
        compilation_cache = context.getCompiledExpressionCache();
#endif

    size_t num_arguments = argument_names.size();

    Node node;
    node.type = ActionType::FUNCTION;
    node.function_builder = function;
    node.children.reserve(num_arguments);

    bool all_const = true;
    ColumnsWithTypeAndName arguments(num_arguments);

    for (size_t i = 0; i < num_arguments; ++i)
    {
        auto & child = getNode(argument_names[i]);
        node.children.emplace_back(&child);
        node.allow_constant_folding = node.allow_constant_folding && child.allow_constant_folding;

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
    /// But if we compile expressions compiled version of this function maybe placed in cache,
    /// so we don't want to unfold non deterministic functions
    if (all_const && node.function_base->isSuitableForConstantFolding()
        && (!settings.compile_expressions || node.function_base->isDeterministic()))
    {
        size_t num_rows = arguments.empty() ? 0 : arguments.front().column->size();
        auto col = node.function->execute(arguments, node.result_type, num_rows, true);

        /// If the result is not a constant, just in case, we will consider the result as unknown.
        if (isColumnConst(*col))
        {
            /// All constant (literal) columns in block are added with size 1.
            /// But if there was no columns in block before executing a function, the result has size 0.
            /// Change the size to 1.

            if (col->empty())
                col = col->cloneResized(1);

            node.column = std::move(col);
        }
    }

    /// Some functions like ignore() or getTypeName() always return constant result even if arguments are not constant.
    /// We can't do constant folding, but can specify in sample block that function result is constant to avoid
    /// unnecessary materialization.
    if (!node.column && node.function_base->isSuitableForConstantFolding())
    {
        if (auto col = node.function_base->getResultIfAlwaysReturnsConstantAndHasArguments(arguments))
        {
            node.column = std::move(col);
            node.allow_constant_folding = false;
        }
    }

    if (result_name.empty())
    {
        result_name = function->getName() + "(";
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            if (i)
                result_name += ", ";
            result_name += argument_names[i];
        }
        result_name += ")";
    }

    node.result_name = std::move(result_name);

    return addNode(std::move(node));
}


NamesAndTypesList ActionsDAG::getRequiredColumns() const
{
    NamesAndTypesList result;
    for (const auto & node : nodes)
        if (node.type == ActionType::INPUT)
            result.emplace_back(node.result_name, node.result_type);

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

void ActionsDAG::removeUnusedActions(const Names & required_names)
{
    std::unordered_set<Node *> nodes_set;
    std::vector<Node *> required_nodes;
    required_nodes.reserve(required_names.size());

    for (const auto & name : required_names)
    {
        auto it = index.find(name);
        if (it == index.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", name, dumpNames());

        if (nodes_set.insert(*it).second)
            required_nodes.push_back(*it);
    }

    removeUnusedActions(required_nodes);
}

void ActionsDAG::removeUnusedActions(const std::vector<Node *> & required_nodes)
{
    {
        Index new_index;

        for (auto * node : required_nodes)
            new_index.insert(node);

        index.swap(new_index);
    }

    removeUnusedActions();
}

void ActionsDAG::removeUnusedActions()
{
    std::unordered_set<const Node *> visited_nodes;
    std::stack<Node *> stack;

    for (auto * node : index)
    {
        visited_nodes.insert(node);
        stack.push(node);
    }

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (!node->children.empty() && node->column && isColumnConst(*node->column) && node->allow_constant_folding)
        {
            /// Constant folding.
            node->type = ActionsDAG::ActionType::COLUMN;
            node->children.clear();
        }

        for (auto * child : node->children)
        {
            if (visited_nodes.count(child) == 0)
            {
                stack.push(child);
                visited_nodes.insert(child);
            }
        }
    }

    nodes.remove_if([&](const Node & node) { return visited_nodes.count(&node) == 0; });
}

void ActionsDAG::addAliases(const NamesWithAliases & aliases, std::vector<Node *> & result_nodes)
{
    std::vector<Node *> required_nodes;

    for (const auto & item : aliases)
    {
        auto & child = getNode(item.first);
        required_nodes.push_back(&child);
    }

    result_nodes.reserve(aliases.size());

    for (size_t i = 0; i < aliases.size(); ++i)
    {
        const auto & item = aliases[i];
        auto * child = required_nodes[i];

        if (!item.second.empty() && item.first != item.second)
        {
            Node node;
            node.type = ActionType::ALIAS;
            node.result_type = child->result_type;
            node.result_name = std::move(item.second);
            node.column = child->column;
            node.allow_constant_folding = child->allow_constant_folding;
            node.children.emplace_back(child);

            auto & alias = addNode(std::move(node), true);
            result_nodes.push_back(&alias);
        }
        else
            result_nodes.push_back(child);
    }
}

void ActionsDAG::addAliases(const NamesWithAliases & aliases)
{
    std::vector<Node *> result_nodes;
    addAliases(aliases, result_nodes);
}

void ActionsDAG::project(const NamesWithAliases & projection)
{
    std::vector<Node *> result_nodes;
    addAliases(projection, result_nodes);
    removeUnusedActions(result_nodes);
    projectInput();
    settings.projected_output = true;
}

void ActionsDAG::removeColumn(const std::string & column_name)
{
    auto & node = getNode(column_name);
    index.remove(&node);
}

bool ActionsDAG::tryRestoreColumn(const std::string & column_name)
{
    if (index.contains(column_name))
        return true;

    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it)
    {
        auto & node = *it;
        if (node.result_name == column_name)
        {
            index.replace(&node);
            return true;
        }
    }

    return false;
}

ActionsDAGPtr ActionsDAG::clone() const
{
    auto actions = cloneEmpty();

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
        actions->index.insert(copy_map[node]);

    return actions;
}

void ActionsDAG::compileExpressions()
{
#if USE_EMBEDDED_COMPILER
    if (settings.compile_expressions)
    {
        compileFunctions();
        removeUnusedActions();
    }
#endif
}

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

        out << "\n";
    }

    return out.str();
}

bool ActionsDAG::hasArrayJoin() const
{
    for (const auto & node : nodes)
        if (node.type == ActionType::ARRAY_JOIN)
            return true;

    return false;
}

bool ActionsDAG::empty() const
{
    for (const auto & node : nodes)
        if (node.type != ActionType::INPUT)
            return false;

    return true;
}

ActionsDAGPtr ActionsDAG::splitActionsBeforeArrayJoin(const NameSet & array_joined_columns)
{
    /// Split DAG into two parts.
    /// (this_nodes, this_index) is a part which depends on ARRAY JOIN and stays here.
    /// (split_nodes, split_index) is a part which will be moved before ARRAY JOIN.
    std::list<Node> this_nodes;
    std::list<Node> split_nodes;
    Index this_index;
    Index split_index;

    struct Frame
    {
        Node * node;
        size_t next_child_to_visit = 0;
    };

    struct Data
    {
        bool depend_on_array_join = false;
        bool visited = false;
        bool used_in_result = false;

        /// Copies of node in one of the DAGs.
        /// For COLUMN and INPUT both copies may exist.
        Node * to_this = nullptr;
        Node * to_split = nullptr;
    };

    std::stack<Frame> stack;
    std::unordered_map<Node *, Data> data;

    for (const auto & node : index)
        data[node].used_in_result = true;

    /// DFS. Decide if node depends on ARRAY JOIN and move it to one of the DAGs.
    for (auto & node : nodes)
    {
        if (!data[&node].visited)
            stack.push({.node = &node});

        while (!stack.empty())
        {
            auto & cur = stack.top();
            auto & cur_data = data[cur.node];

            /// At first, visit all children. We depend on ARRAY JOIN if any child does.
            while (cur.next_child_to_visit < cur.node->children.size())
            {
                auto * child = cur.node->children[cur.next_child_to_visit];
                auto & child_data = data[child];

                if (!child_data.visited)
                {
                    stack.push({.node = child});
                    break;
                }

                ++cur.next_child_to_visit;
                if (child_data.depend_on_array_join)
                    cur_data.depend_on_array_join = true;
            }

            /// Make a copy part.
            if (cur.next_child_to_visit == cur.node->children.size())
            {
                if (cur.node->type == ActionType::INPUT && array_joined_columns.count(cur.node->result_name))
                    cur_data.depend_on_array_join = true;

                cur_data.visited = true;
                stack.pop();

                if (cur_data.depend_on_array_join)
                {
                    auto & copy = this_nodes.emplace_back(*cur.node);
                    cur_data.to_this = &copy;

                    /// Replace children to newly created nodes.
                    for (auto & child : copy.children)
                    {
                        auto & child_data = data[child];

                        /// If children is not created, int may be from split part.
                        if (!child_data.to_this)
                        {
                            if (child->type == ActionType::COLUMN) /// Just create new node for COLUMN action.
                            {
                                child_data.to_this = &this_nodes.emplace_back(*child);
                            }
                            else
                            {
                                /// Node from split part is added as new input.
                                Node input_node;
                                input_node.type = ActionType::INPUT;
                                input_node.result_type = child->result_type;
                                input_node.result_name = child->result_name; // getUniqueNameForIndex(index, child->result_name);
                                child_data.to_this = &this_nodes.emplace_back(std::move(input_node));

                                /// This node is needed for current action, so put it to index also.
                                split_index.replace(child_data.to_split);
                            }
                        }

                        child = child_data.to_this;
                    }
                }
                else
                {
                    auto & copy = split_nodes.emplace_back(*cur.node);
                    cur_data.to_split = &copy;

                    /// Replace children to newly created nodes.
                    for (auto & child : copy.children)
                    {
                        child = data[child].to_split;
                        assert(child != nullptr);
                    }

                    if (cur_data.used_in_result)
                    {
                        split_index.replace(&copy);

                        /// If this node is needed in result, add it as input.
                        Node input_node;
                        input_node.type = ActionType::INPUT;
                        input_node.result_type = node.result_type;
                        input_node.result_name = node.result_name;
                        cur_data.to_this = &this_nodes.emplace_back(std::move(input_node));
                    }
                }
            }
        }
    }

    for (auto * node : index)
        this_index.insert(data[node].to_this);

    /// Consider actions are empty if all nodes are constants or inputs.
    bool split_actions_are_empty = true;
    for (const auto & node : split_nodes)
        if (!node.children.empty())
            split_actions_are_empty = false;

    if (split_actions_are_empty)
        return {};

    index.swap(this_index);
    nodes.swap(this_nodes);

    auto split_actions = cloneEmpty();
    split_actions->nodes.swap(split_nodes);
    split_actions->index.swap(split_index);
    split_actions->settings.project_input = false;

    return split_actions;
}

}
