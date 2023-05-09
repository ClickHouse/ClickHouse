#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

std::shared_ptr<ActionsDAG> buildPredecessorActionsDag(const Block & header, const Names & keys, const DataHints & hints,
        Names & changed_aggregating_keys, Names & new_aggregating_keys, DataTypes & changed_data_types)
{
    auto dag = std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName());
    for (const auto & old_key : keys)
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

    return dag;
}

std::shared_ptr<ActionsDAG> buildSuccessorActionsDag(const Block & header, const Names & changed_keys, const DataTypes& changed_data_types, const DataHints & hints)
{
    auto dag = std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName());

    for (size_t i = 0; i < changed_keys.size(); ++i)
    {
        const auto & changed_key = changed_keys[i];
        const auto & changed_data_type = changed_data_types[i];

        const auto & hint = hints.at(changed_key);

        const auto key_to_remove = "__hinted_key_" + changed_key;

        const auto * node_to_remove = &dag->findInOutputs(key_to_remove);
        ColumnWithTypeAndName const_column;
        if (hint.lower_boundary->getTypeName() == "Int64")
            const_column.type = std::make_shared<DataTypeInt64>();
        else
            const_column.type = std::make_shared<DataTypeUInt64>();
        const_column.column = const_column.type->createColumnConst(1, hint.lower_boundary.value());
        const_column.name = "__plus_value_" + changed_key;
        const auto * hint_node = &dag->addColumn(const_column);

        ActionsDAG::NodeRawConstPtrs children = {node_to_remove, hint_node};
        auto plus_function = FunctionFactory::instance().get("plus", Context::getGlobalContextInstance());
        dag->addOrReplaceInOutputs(dag->addCast(dag->addFunction(plus_function, children, "__uncasted_" + changed_key), changed_data_type, changed_key));
        dag->removeUnusedResult(key_to_remove);
    }

    return dag;
}

}

size_t tryReduceAggregationKeysSize(QueryPlan::Node * node, QueryPlan::Nodes & nodes)
{
    if (node->children.size() != 1)
        return 0;

    auto * rollup_step = typeid_cast<RollupStep *>(node->step.get());
    if (rollup_step)
        return 0;

    auto * cube_step = typeid_cast<CubeStep *>(node->step.get());
    if (cube_step)
        return 0;

    auto * aggregating_node = node->children.front();
    auto * aggregating_step = typeid_cast<AggregatingStep *>(aggregating_node->step.get());
    if (!aggregating_step || aggregating_step->isOptimizedWithDataHints())
        return 0;

    const auto & hints = aggregating_step->getHints();
    if (hints.empty())
        return 0;

    if (aggregating_node->children.size() != 1)
        return 0;

    auto * next_node = aggregating_node->children.front();

    const auto & input_stream = next_node->step->getOutputStream();
    const auto & input_header = input_stream.header;
    const auto & initial_keys = aggregating_step->getParams().keys;

    Names changed_aggregating_keys;
    Names new_aggregating_keys;

    DataTypes changed_data_types;

    const auto & predecessor_dag = buildPredecessorActionsDag(input_header, initial_keys, hints, changed_aggregating_keys, new_aggregating_keys, changed_data_types);
    if (changed_aggregating_keys.empty())
        return 0;

    auto & predecessor_node = nodes.emplace_back();

    predecessor_node.children = {next_node};
    predecessor_node.step = std::make_unique<ExpressionStep>(input_stream, predecessor_dag);

    aggregating_step->updateParams(new_aggregating_keys);
    aggregating_step->updateInputStream(predecessor_node.step->getOutputStream());
    aggregating_step->setOptimizedWithDataHints();

    aggregating_node->children = {&predecessor_node};

    const auto & successor_dag = buildSuccessorActionsDag(aggregating_step->getOutputStream().header, changed_aggregating_keys, changed_data_types, hints);

    auto & successor_node = nodes.emplace_back();

    successor_node.children = {aggregating_node};
    successor_node.step = std::make_unique<ExpressionStep>(aggregating_step->getOutputStream(), successor_dag);

    node->children = {&successor_node};

    return 3;
}

}
