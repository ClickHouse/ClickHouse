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
    if (constant_type_name == "UInt128" || constant_type_name == "Int128" || constant_type_name == "UInt256" || constant_type_name == "Int256")
        return std::nullopt;

    const auto & input_type = maybe_input_column->result_type;
    const auto & constant_type = maybe_constant_column->result_type;
    if (isDate(input_type) != isDate(constant_type) || isDate32(input_type) != isDate32(constant_type) ||
            isDateTime(input_type) != isDateTime(constant_type) || isDateTime64(input_type) != isDateTime64(constant_type))
        return std::nullopt;

    ProcessedPredicate result;
    result.column_name = maybe_input_column->result_name;
    if (maybe_constant_column->result_type->isValueRepresentedByUnsignedInteger())
        result.value = maybe_constant_column->column->getUInt(0);
    else
        result.value = maybe_constant_column->column->getInt(0);
    result.reversed = maybe_input_column == node.children[1];
    result.is_column_unsigned = maybe_input_column->result_type->isValueRepresentedByUnsignedInteger();
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

void updateDataHintsWithExpressionActionsDAG(DataHints & hints, const ActionsDAG & actions)
{
    const auto & outputs = actions.getOutputs();
    DataHints new_hints;
    for (const auto & node : outputs)
        if (node->type == ActionsDAG::ActionType::INPUT && hints.contains(node->result_name))
                new_hints[node->result_name] = hints[node->result_name];
    hints = std::move(new_hints);
}

void intersectDataHints(DataHints & left_hints, const DataHints & right_hints)
{
    for (const auto & right_hint : right_hints)
    {
        if (!left_hints.contains(right_hint.first))
        {
            left_hints[right_hint.first] = right_hint.second;
        }
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
        {
            left_hints[right_hint.first] = right_hint.second;
        }
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
        {
            left_hints[renamed_key] = right_hint.second;
        }
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
