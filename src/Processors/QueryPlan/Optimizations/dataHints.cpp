#include <stack>

#include <Processors/QueryPlan/Optimizations/dataHints.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Core/Names.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/FunctionFactory.h>
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

void describeDataHint(const std::string& column, const DataHint& hint)
{
    if (!hint.lower_boundary && !hint.upper_boundary)
    {
        LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "DataHint for column {} has lower_boundary: nullptr and upper_boundary: nullptr", column);
        return;
    }

    if (!hint.lower_boundary)
    {
        LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "DataHint for column {} has lower_boundary: nullptr and upper_boundary: {}", column, hint.upper_boundary->get<int>());
        return;
    }

    if (!hint.upper_boundary)
    {
        LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "DataHint for column {} has lower_boundary: {} and upper_boundary: nullptr", column, hint.lower_boundary->get<int>());
        return;
    }

    LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "DataHint for column {} has lower_boundary: {} and upper_boundary: {}", column, hint.lower_boundary->get<int>(), hint.upper_boundary->get<int>());
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
            else if (name == "equal")
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

    for (const auto & p : hints)
        describeDataHint(p.first, p.second);
}

void updateDataHintsWithExpressionActionsDAG(DataHints & hints, const ActionsDAG & actions)
{
    const auto & outputs = actions.getOutputs();
    DataHints new_hints;
    for (const auto & node : outputs)
    {
        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            if (hints.contains(node->result_name))
            {
                new_hints[node->result_name] = hints[node->result_name];
            }
        }
    }
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

std::tuple<Names, Names, DataTypes> optimizeAggregatingStepWithDataHints(
        QueryPipelineBuilder & pipeline,
        const DataHints & hints,
        const ColumnsWithTypeAndName & input_header,
        const BuildQueryPipelineSettings & settings,
        const bool final)
{
    Names changed_aggregating_keys;
    Names new_aggregating_keys;

    DataTypes changed_data_types;

    auto dag = std::make_shared<ActionsDAG>(input_header);
    for (const auto & old_key : transform_params->params.keys)
    {
        if (hints.contains(old_key) && hints.at(old_key).isRangeLengthLessOrEqualThan(65535))
        {
            const auto & hint = hints.at(old_key);
            const auto new_key = "__hinted_key_" + old_key;

            const ActionsDAG::Node * key_column_node = dag->tryFindInOutputs(old_key);
            const auto & key_column_type = key_column_node->result_type;
            const auto & type_name = key_column_type->getName();

            if (type_name == "UInt8" || type_name == "Int8" ||
                ((type_name == "UInt16" || type_name == "Int16") && !hint.isRangeLengthLessOrEqualThan(255)))
            {
                new_aggregating_keys.push_back(old_key);
                continue;
            }

            changed_aggregating_keys.push_back(old_key);
            new_aggregating_keys.push_back(new_key);

            changed_data_types.push_back(key_column_type);
            ColumnWithTypeAndName const_column;
            if (hint.lower_boundary->getTypeName() == "Int64")
                const_column.type = std::make_shared<DataTypeInt64>();
            else
                const_column.type = std::make_shared<DataTypeUInt64>();
            const_column.column = const_column.type->createColumnConst(1, hint.lower_boundary.value());
            const_column.name = "__minus_value_" + old_key;
            const auto * hint_node = &dag->addColumn(const_column);

            ActionsDAG::NodeRawConstPtrs children = {key_column_node, hint_node};
            auto minus_function = FunctionFactory::instance().get("minus", Context::getGlobalContextInstance());
            const auto & added_function = dag->addFunction(minus_function, children, "__hinted_key_uncasted_" + old_key);

            DataTypePtr result_type;
            if (hint.isRangeLengthLessOrEqualThan(255))
                result_type = std::make_shared<DataTypeUInt8>();
            else
                result_type = std::make_shared<DataTypeUInt16>();

            dag->addOrReplaceInOutputs(dag->addCast(added_function, result_type, new_key));
        }
        else
        {
            new_aggregating_keys.push_back(old_key);
        }
    }

    if (!changed_aggregating_keys.empty())
    {
        auto expression = std::make_shared<ExpressionActions>(dag, settings.getActionsSettings());
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, expression);
        });
    }

    return {changed_aggregating_keys, new_aggregating_keys, changed_data_types};
}

void optimizeAggregatingStepWithDataHintsReturnInitialColumns(
    QueryPipelineBuilder & pipeline,
    const DataHints & hints,
    const Names & changed_keys,
    const DataTypes & changed_data_types,
    const BuildQueryPipelineSettings & settings)
{
    if (changed_keys.empty())
        return;

    auto dag = std::make_shared<ActionsDAG>(pipeline.getHeader().getColumnsWithTypeAndName());

    for (size_t i = 0; i < changed_keys.size(); ++i)
    {
        const auto & changed_key = changed_keys[i];
        const auto & changed_data_type = changed_data_types[i];

        const auto & hint = hints.at(changed_key);

        const auto key_to_remove = "__hinted_key_" + changed_key;

        const auto * hinted_key_casted_column_node = &dag->addCast(dag->findInOutputs(key_to_remove), changed_data_type, "__casted_key_" + changed_key);
        ColumnWithTypeAndName const_column;
        if (hint.lower_boundary->getTypeName() == "Int64")
            const_column.type = std::make_shared<DataTypeInt64>();
        else
            const_column.type = std::make_shared<DataTypeUInt64>();
        const_column.column = const_column.type->createColumnConst(1, hint.lower_boundary.value());
        const_column.name = "__plus_value_" + changed_key;
        const auto * hint_node = &dag->addColumn(const_column);

        ActionsDAG::NodeRawConstPtrs children = {hinted_key_casted_column_node, hint_node};
        auto plus_function = FunctionFactory::instance().get("plus", Context::getGlobalContextInstance());
        dag->addOrReplaceInOutputs(dag->addFunction(plus_function, children, changed_key));
        dag->removeUnusedResult(key_to_remove);
    }

    auto expression = std::make_shared<ExpressionActions>(dag, settings.getActionsSettings());
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression);
    });
}

}
