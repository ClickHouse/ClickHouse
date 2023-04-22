#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>
#include <Core/SortDescription.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

std::shared_ptr<ActionsDAG> buildPredecessorActionsDag(const Block & header, const Names & keys, const DataHints & hints,
        Names & changed_sorting_keys, Names & new_sorting_keys, DataTypes & changed_data_types)
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
                new_sorting_keys.push_back(old_key);
                continue;
            }

            changed_sorting_keys.push_back(old_key);
            new_sorting_keys.push_back(new_key);

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
            else if (hint.isRangeLengthLessOrEqualThan(65535))
                result_type = std::make_shared<DataTypeUInt16>();
            else
                result_type = std::make_shared<DataTypeUInt32>();

            dag->addOrReplaceInOutputs(dag->addCast(added_function, result_type, new_key));
        }
        else
        {
            new_sorting_keys.push_back(old_key);
        }
    }

    return dag;
}

}

size_t tryReduceSortingKeysSize(QueryPlan::Node * node, QueryPlan::Nodes & nodes)
{
    if (node->children.size() != 1)
        return 0;

    auto * sorting_node = node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting_step || sorting_step->getType() != SortingStep::Type::Full)
        return 0;

    const auto hints = sorting_step->getInputStreams().front().hints;
    if (hints.empty())
        return 0;

    if (sorting_node->children.size() != 1)
        return 0;

    auto * next_node = sorting_node->children.front();

    const auto & input_stream = next_node->step->getOutputStream();
    const auto & input_header = input_stream.header;

    auto & description = sorting_step->getSortDescription();
    Names initial_keys(description.size());
    for (size_t i = 0; i < description.size(); ++i)
        initial_keys[i] = description[i].column_name;

    Names changed_sorting_keys;
    Names new_sorting_keys;

    DataTypes changed_data_types;

    const auto & predecessor_dag = buildPredecessorActionsDag(input_header, initial_keys, hints, changed_sorting_keys, new_sorting_keys, changed_data_types);
    if (changed_sorting_keys.empty())
        return 0;

    auto & predecessor_node = nodes.emplace_back();

    predecessor_node.children = {next_node};
    predecessor_node.step = std::make_unique<ExpressionStep>(input_stream, predecessor_dag);

    for (size_t i = 0; i < description.size(); ++i)
        description[i].column_name = new_sorting_keys[i];

    sorting_step->updateInputStream(predecessor_node.step->getOutputStream());

    sorting_node->children = {&predecessor_node};

    return 2;
}

}
