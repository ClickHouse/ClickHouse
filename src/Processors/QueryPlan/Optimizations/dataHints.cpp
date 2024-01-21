#include <stack>

#include <Processors/QueryPlan/Optimizations/dataHints.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Core/Names.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace
{

struct ProcessedPredicate
{
    std::string column_name;
    Field value;
    bool reversed; // If INPUT is right child
    bool is_column_unsigned;
};

std::optional<ProcessedPredicate> processPredicate(const ActionsDAG::Node & node)
{
    if (node.children.size() != 2)
        return std::nullopt;

    const ActionsDAG::Node * maybe_input_column = nullptr;
    const ActionsDAG::Node * maybe_constant_column = nullptr;

    for (const auto & child : node.children)
    {
        if (child->column)
            maybe_constant_column = child;
        else if (child->type == ActionsDAG::ActionType::INPUT)
            maybe_input_column = child;
    }

    if (!maybe_constant_column || !maybe_input_column ||
            !maybe_input_column->result_type->isValueRepresentedByInteger() ||
            !maybe_constant_column->result_type->isValueRepresentedByInteger())
        return std::nullopt;

    const auto & constant_type_name = maybe_constant_column->result_type->getName();
    if (constant_type_name == "UInt128" || constant_type_name == "Int128" || constant_type_name == "UInt256" || constant_type_name == "Int256" ||
            constant_type_name == "IPv4" || constant_type_name == "IPv6")
        return std::nullopt;

    const auto & input_type = maybe_input_column->result_type;
    const auto & constant_type = maybe_constant_column->result_type;
    if (isDate(input_type) != isDate(constant_type) || isDate32(input_type) != isDate32(constant_type) ||
            isDateTime(input_type) != isDateTime(constant_type) || isDateTime64(input_type) != isDateTime64(constant_type))
        return std::nullopt;

    ProcessedPredicate result;
    result.column_name = maybe_input_column->result_name;
    if (!maybe_constant_column->result_type->isNullable())
    {
        if (maybe_constant_column->result_type->isValueRepresentedByUnsignedInteger())
            result.value = maybe_constant_column->column->getUInt(0);
        else
            result.value = maybe_constant_column->column->getInt(0);
    }
    result.reversed = maybe_input_column == node.children[1];
    result.is_column_unsigned = maybe_input_column->result_type->isValueRepresentedByUnsignedInteger();
    return result;
}

struct ProcessedFunction
{
    const ActionsDAG::Node * hinted_node;
    DataTypePtr result_type;
    ExecutableFunctionPtr executable_function;
    std::optional<Field> value;
    bool reversed; // If INPUT is right child
};

std::optional<ProcessedFunction> processFunction(const ActionsDAG::Node & node)
{
    if (node.children.size() > 2)
        return std::nullopt;

    const ActionsDAG::Node * maybe_hinted_node = nullptr;
    const ActionsDAG::Node * maybe_constant_node = nullptr;

    for (const auto & child : node.children)
    {
        if (child->type == ActionsDAG::ActionType::COLUMN)
            maybe_constant_node = child;
        else
            maybe_hinted_node = child;
    }

    if (!maybe_hinted_node || !maybe_hinted_node->result_type->isValueRepresentedByInteger())
        return std::nullopt;

    if (maybe_constant_node)
    {
        const auto & constant_type_name = maybe_constant_node->result_type->getName();
        if (constant_type_name == "UInt128" || constant_type_name == "Int128" || constant_type_name == "UInt256" || constant_type_name == "Int256" ||
                constant_type_name == "IPv4" || constant_type_name == "IPv6")
            return std::nullopt;

        const auto & input_type = maybe_hinted_node->result_type;
        const auto & constant_type = maybe_constant_node->result_type;
        if (isDate(input_type) != isDate(constant_type) || isDate32(input_type) != isDate32(constant_type) ||
                isDateTime(input_type) != isDateTime(constant_type) || isDateTime64(input_type) != isDateTime64(constant_type))
            return std::nullopt;
    }

    ProcessedFunction result;
    result.hinted_node = maybe_hinted_node;
    result.result_type = node.result_type;
    result.executable_function = node.function;
    if (maybe_constant_node && maybe_constant_node->result_type->isValueRepresentedByNumber())
    {
        if (maybe_constant_node->result_type->isValueRepresentedByUnsignedInteger())
            result.value = maybe_constant_node->column->getUInt(0);
        else
            result.value = maybe_constant_node->column->getInt(0);
        result.reversed = maybe_hinted_node == node.children[1];
    }
    return result;
}

}

void updateDataHintsWithFilterActionsDAG(DataHints & hints, const ActionsDAG::Node & actions)
{
    std::unordered_set<const ActionsDAG::Node *> visited_nodes;
    std::unordered_map<const ActionsDAG::Node *, DataHints> node_to_hints;

    struct Frame
    {
        const ActionsDAG::Node * node = nullptr;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push({.node = &actions});

    while (!stack.empty())
    {
        auto& frame = stack.top();
        const auto * node = frame.node;
        visited_nodes.insert(node);

        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & name = node->function_base->getName();
            if (name == "and")
            {
                while (frame.next_child < node->children.size())
                {
                    const auto * child = node->children[frame.next_child];
                    if (!visited_nodes.contains(child))
                    {
                        stack.push({.node = child});
                        break;
                    }

                    if (node_to_hints.contains(child))
                    {
                        if (!node_to_hints.contains(node))
                        {
                            node_to_hints[node] = node_to_hints[child];
                        }
                        else
                        {
                            intersectDataHints(node_to_hints[node], node_to_hints[child]);
                        }
                        node_to_hints.erase(child);
                    }

                    ++frame.next_child;
                }

                if (frame.next_child == node->children.size())
                {
                    stack.pop();
                }
                continue;
            }
            else if (name == "equals")
            {
                const auto & info = processPredicate(*node);
                if (info)
                {
                    node_to_hints[node] = {{info->column_name, {info->is_column_unsigned}}};
                    node_to_hints[node][info->column_name].setLowerBoundary(info->value);
                    node_to_hints[node][info->column_name].setUpperBoundary(info->value);
                }
            }
            else if (name == "greater")
            {
                const auto & info = processPredicate(*node);
                if (info)
                {
                    node_to_hints[node] = {{info->column_name, {info->is_column_unsigned}}};
                    if (!info->reversed)
                        node_to_hints[node][info->column_name].setStrictLowerBoundary(info->value);
                    else
                        node_to_hints[node][info->column_name].setStrictUpperBoundary(info->value);
                }
            }
            else if (name == "greaterOrEquals")
            {
                const auto & info = processPredicate(*node);
                if (info)
                {
                    node_to_hints[node] = {{info->column_name, {info->is_column_unsigned}}};
                    if (!info->reversed)
                        node_to_hints[node][info->column_name].setLowerBoundary(info->value);
                    else
                        node_to_hints[node][info->column_name].setUpperBoundary(info->value);
                }
            }
            else if (name == "less")
            {
                const auto & info = processPredicate(*node);
                if (info)
                {
                    node_to_hints[node] = {{info->column_name, {info->is_column_unsigned}}};
                    if (!info->reversed)
                        node_to_hints[node][info->column_name].setStrictUpperBoundary(info->value);
                    else
                        node_to_hints[node][info->column_name].setStrictLowerBoundary(info->value);
                }
            }
            else if (name == "lessOrEquals")
            {
                const auto & info = processPredicate(*node);
                if (info)
                {
                    node_to_hints[node] = {{info->column_name, {info->is_column_unsigned}}};
                    if (!info->reversed)
                        node_to_hints[node][info->column_name].setUpperBoundary(info->value);
                    else
                        node_to_hints[node][info->column_name].setLowerBoundary(info->value);
                }
            }
        }

        stack.pop();
    }

    if (node_to_hints.contains(&actions))
        intersectDataHints(hints, node_to_hints[&actions]);
}

const std::unordered_set<std::string> allowed_functions = {"plus", "minus", "multiply", "modulo", "toYear", "toQuarter", "toMonth", "toDayOfYear", "toDayOfMonth", "toDayOfWeek", "toHour", "toMinute", "toSecond"};

void updateDataHintsWithExpressionActionsDAG(DataHints & hints, const ActionsDAG & actions)
{
    std::unordered_set<const ActionsDAG::Node *> visited_nodes;
    std::unordered_map<const ActionsDAG::Node *, DataHint> node_to_hint;

    std::stack<const ActionsDAG::Node *> stack;
    for (const auto & output_node : actions.getOutputs())
        stack.push(output_node);

    while (!stack.empty())
    {
        const auto * node = stack.top();
        if (node_to_hint.contains(node))
        {
            stack.pop();
            continue;
        }
        visited_nodes.insert(node);

        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            if (hints.contains(node->result_name))
                node_to_hint[node] = hints[node->result_name];
        }
        else if (node->type == ActionsDAG::ActionType::ALIAS)
        {
            if (!visited_nodes.contains(node->children[0]))
            {
                stack.push(node->children[0]);
                continue;
            }

            if (node_to_hint.contains(node->children[0]))
                node_to_hint[node] = node_to_hint[node->children[0]];
        }
        else if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & name = node->function_base->getName();
            if (!allowed_functions.contains(name))
            {
                node_to_hint[node] = {};
                stack.pop();
                continue;
            }

            const auto & info = processFunction(*node);
            if (!info)
            {
                node_to_hint[node] = {};
                stack.pop();
                continue;
            }

            if (name == "modulo")
            {
                node_to_hint[node] = {};
                if (!info->reversed && info->value.has_value() && info->value.value().getTypeName() == "UInt64")
                {
                    node_to_hint[node] = {false};
                    node_to_hint[node].setStrictLowerBoundary(-info->value.value().get<int64_t>()); // May be problems with that
                    node_to_hint[node].setStrictUpperBoundary(info->value.value().get<uint64_t>());
                }
                stack.pop();
                continue;
            }

            if (!visited_nodes.contains(info->hinted_node))
            {
                stack.push(info->hinted_node);
                continue;
            }

            if (node_to_hint.contains(info->hinted_node))
            {
                node_to_hint[node] = node_to_hint[info->hinted_node];
                node_to_hint[node].is_column_unsigned = node->result_type->isValueRepresentedByUnsignedInteger();

                if (name == "plus")
                {
                    if (info->value.has_value())
                        node_to_hint[node].plusConst(info->value.value());
                    else
                        node_to_hint[node] = {};
                }
                else if (name == "minus")
                {
                    if (!info->value.has_value())
                        node_to_hint[node] = {};
                    else if (info->reversed)
                    {
                        node_to_hint[node].multiplyConst(-1);
                        node_to_hint[node].plusConst(info->value.value());
                    }
                    else
                        node_to_hint[node].minusConst(info->value.value());
                }
                else if (name == "multiply")
                {
                    if (info->value.has_value())
                        node_to_hint[node].multiplyConst(info->value.value());
                    else
                        node_to_hint[node] = {};
                }
                else if (name == "toHour")
                    node_to_hint[node] = {0, 23, true};
                else if (name == "toMinute" || name == "toSecond")
                    node_to_hint[node] = {0, 59, true};
                else if (name == "toDayOfYear")
                    node_to_hint[node] = {1, 366, true};
                else if (name == "toDayOfMonth")
                    node_to_hint[node] = {1, 31, true};
                else if (name == "toDayOfWeek")
                    node_to_hint[node] = {1, 7, true};
                else if (name == "toMonth")
                    node_to_hint[node] = {1, 12, true};
                else if (name == "toQuarter")
                    node_to_hint[node] = {1, 4, true};
                else if (name == "toYear")
                {
                    if (node_to_hint[node].lower_boundary.has_value())
                    {
                        const auto& col1 = info->result_type->createColumnConst(1, node_to_hint[node].lower_boundary.value());
                        const auto& result1 = info->executable_function->execute({{col1, info->hinted_node->result_type, "lower_boundary_hint"}}, info->result_type, 1, false);
                        node_to_hint[node].lower_boundary = result1->getUInt(0);
                    }
                    if (node_to_hint[node].upper_boundary.has_value())
                    {
                        const auto& col2 = info->result_type->createColumnConst(1, node_to_hint[node].upper_boundary.value());
                        const auto& result2 = info->executable_function->execute({{col2, info->hinted_node->result_type, "upper_boundary_hint"}}, info->result_type, 1, false);
                        node_to_hint[node].upper_boundary = result2->getUInt(0);
                    }
                }
            }
        }

        stack.pop();
    }

    DataHints new_hints;
    for (const auto & output_node : actions.getOutputs())
        if (node_to_hint.contains(output_node) && !node_to_hint[output_node].isEmpty())
            new_hints[output_node->result_name] = node_to_hint[output_node];

    hints = std::move(new_hints);
}

void intersectDataHints(DataHints & left_hints, const DataHints & right_hints)
{
    for (const auto & right_hint : right_hints)
    {
        if (!left_hints.contains(right_hint.first))
            left_hints[right_hint.first] = right_hint.second;
        else
        {
            left_hints[right_hint.first].intersectLowerBoundary(right_hint.second.lower_boundary);
            left_hints[right_hint.first].intersectUpperBoundary(right_hint.second.upper_boundary);
        }
    }
}

void unionDataHints(DataHints & left_hints, const DataHints & right_hints)
{
    for (const auto & right_hint : right_hints)
    {
        if (!left_hints.contains(right_hint.first))
            left_hints[right_hint.first] = right_hint.second;
        else
        {
            left_hints[right_hint.first].unionLowerBoundary(right_hint.second.lower_boundary);
            left_hints[right_hint.first].unionUpperBoundary(right_hint.second.upper_boundary);
        }
    }
}

void unionJoinDataHints(DataHints & left_hints, const DataHints & right_hints, const TableJoin & table_join)
{
    for (const auto & right_hint : right_hints)
    {
        const auto & renamed_key = table_join.renamedRightColumnName(right_hint.first);
        if (!left_hints.contains(renamed_key))
            left_hints[renamed_key] = right_hint.second;
        else
        {
            left_hints[renamed_key].unionLowerBoundary(right_hint.second.lower_boundary);
            left_hints[renamed_key].unionUpperBoundary(right_hint.second.upper_boundary);
        }
    }
}

void updateDataHintsWithOutputHeaderKeys(DataHints & hints, const Names & keys)
{
    DataHints new_hints;
    for (const auto & key : keys)
        if (hints.contains(key))
            new_hints[key] = hints[key];
    hints = std::move(new_hints);
}

}
