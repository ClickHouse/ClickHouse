#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>
#include <Common/JSONBuilder.h>
#include <Common/Logger.h>
#include <Common/FieldVisitorToString.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

Block SourceStepWithFilter::applyPrewhereActions(Block block, const FilterDAGInfoPtr & row_level_filter, const PrewhereInfoPtr & prewhere_info)
{
    if (row_level_filter)
    {
        block = row_level_filter->actions.updateHeader(block);
        auto & row_level_column = block.getByName(row_level_filter->column_name);
        if (!row_level_column.type->canBeUsedInBooleanContext())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Invalid type for filter in PREWHERE: {}",
                row_level_column.type->getName());
        }

        if (row_level_filter->do_remove_column)
            block.erase(row_level_filter->column_name);
    }

    if (prewhere_info)
    {
        {
            block = prewhere_info->prewhere_actions.updateHeader(block);

            auto & prewhere_column = block.getByName(prewhere_info->prewhere_column_name);
            if (!prewhere_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                    "Invalid type for filter in PREWHERE: {}",
                    prewhere_column.type->getName());
            }

            if (prewhere_info->remove_prewhere_column)
            {
                block.erase(prewhere_info->prewhere_column_name);
            }
            else if (prewhere_info->need_filter)
            {
                if (const auto * type = typeid_cast<const DataTypeNullable *>(prewhere_column.type.get()); type && type->onlyNull())
                {
                    prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), Null());
                }
                else
                {
                    WhichDataType which(removeNullable(recursiveRemoveLowCardinality(prewhere_column.type)));

                    if (which.isNativeInt() || which.isNativeUInt())
                        prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1u)->convertToFullColumnIfConst();
                    else if (which.isFloat())
                        prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1.0f)->convertToFullColumnIfConst();
                    else
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                            "Illegal type {} of column for filter",
                            prewhere_column.type->getName());
                }
            }
        }
    }

    return block;
}

void SourceStepWithFilterBase::applyFilters(ActionDAGNodes added_filter_nodes)
{
    auto dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, {});
    filter_actions_dag = dag ? std::make_shared<const ActionsDAG>(std::move(*dag)) : nullptr;
}

void SourceStepWithFilter::applyFilters(ActionDAGNodes added_filter_nodes)
{
    auto dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, query_info.buildNodeNameToInputNodeColumn());
    filter_actions_dag = dag ? std::make_shared<const ActionsDAG>(std::move(*dag)) : nullptr;
}

size_t SourceStepWithFilterBase::updateFilterDagConstants(
    const ConstantValueMap & old_value_to_param,
    const std::vector<Field> & parsed_params)
{
    auto log = getLogger("SourceStepWithFilter");
    size_t total_updated = 0;

    for (auto & dag : filter_dags)
    {
        for (auto node_it = dag.getNodes().begin(); node_it != dag.getNodes().end(); ++node_it)
        {
            auto & dag_node = const_cast<ActionsDAG::Node &>(*node_it);
            if (dag_node.type != ActionsDAG::ActionType::COLUMN || !dag_node.column || !isColumnConst(*dag_node.column))
                continue;

            const auto * col_const = typeid_cast<const ColumnConst *>(dag_node.column.get());
            if (!col_const)
                continue;

            Field old_value = col_const->getField();
            String old_key = applyVisitor(FieldVisitorToString(), old_value);
            auto it = old_value_to_param.find(old_key);
            if (it == old_value_to_param.end())
                continue;

            const auto & [param_index, target_type] = it->second;
            try
            {
                Field raw_value = parsed_params[param_index];
                DataTypePtr final_type = target_type ? target_type : dag_node.result_type;
                if (final_type->getTypeId() != static_cast<TypeIndex>(raw_value.getType()))
                    raw_value = convertFieldToType(raw_value, *final_type);
                ColumnConstPtr new_column = final_type->createColumnConst(1, raw_value);
                dag_node.result_type = std::move(final_type);
                dag_node.column = std::move(new_column);
                ++total_updated;
                LOG_DEBUG(log, "updateFilterDagConstants: updated COLUMN node result_name='{}' to value='{}'",
                    dag_node.result_name, applyVisitor(FieldVisitorToString(), raw_value));
            }
            catch (...)
            {
                LOG_DEBUG(log, "updateFilterDagConstants: failed to update node: {}", getCurrentExceptionMessage(false));
            }
        }
    }

    LOG_DEBUG(log, "updateFilterDagConstants: updated {} nodes across {} dags", total_updated, filter_dags.size());
    return total_updated;
}

void SourceStepWithFilter::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    query_info.prewhere_info = prewhere_info_value;
    output_header = std::make_shared<const Block>(applyPrewhereActions(
        storage_snapshot->getSampleBlockForColumns(required_source_columns),
        query_info.row_level_filter,
        query_info.prewhere_info));
}

void SourceStepWithFilter::describeActions(FormatSettings & format_settings) const
{
    std::string prefix = format_settings.detail_prefix;

    if (format_settings.pretty)
        QueryPlanFormat::formatOutputColumns(format_settings.pretty_names, format_settings.out, *this, prefix);

    if (!format_settings.pretty && (query_info.prewhere_info || query_info.row_level_filter))
    {
        format_settings.out << prefix << "Prewhere info" << '\n';
        if (query_info.prewhere_info)
            format_settings.out << prefix << "Need filter: " << query_info.prewhere_info->need_filter << '\n';

        prefix.push_back(format_settings.indent_char);
        prefix.push_back(format_settings.indent_char);
    }

    if (query_info.prewhere_info)
    {
        const auto pretty_expression = format_settings.pretty
            ? QueryPlanFormat::formatColumnPretty(query_info.prewhere_info->prewhere_column_name, format_settings.pretty_names) : String{};

        if (!format_settings.pretty || !pretty_expression.empty())
        {
            format_settings.out << prefix << "Prewhere filter" << '\n';
            format_settings.out << prefix << "Prewhere filter column: " << (format_settings.pretty ? pretty_expression : query_info.prewhere_info->prewhere_column_name);
            if (!format_settings.pretty && query_info.prewhere_info->remove_prewhere_column)
                format_settings.out << " (removed)";
            format_settings.out << '\n';
        }

        if (format_settings.pretty)
        {
            const auto annotation = QueryPlanFormat::getColumnAnnotation(query_info.prewhere_info->prewhere_column_name, format_settings);
            if (!annotation.empty())
                format_settings.out << prefix << annotation << '\n';
        }

        if (!format_settings.compact)
        {
            auto expression = std::make_shared<ExpressionActions>(query_info.prewhere_info->prewhere_actions.clone());
            expression->describeActions(format_settings.out, prefix);
        }
    }

    if (query_info.row_level_filter)
    {
        const auto pretty_expression = format_settings.pretty
            ? QueryPlanFormat::formatColumnPretty(query_info.row_level_filter->column_name, format_settings.pretty_names) : String{};

        if (!format_settings.pretty || !pretty_expression.empty())
        {
            format_settings.out << prefix << "Row level filter" << '\n';
            format_settings.out << prefix << "Row level filter column: " << (format_settings.pretty ? pretty_expression : query_info.row_level_filter->column_name);
            if (!format_settings.pretty && query_info.row_level_filter->do_remove_column)
                format_settings.out << " (removed)";
            format_settings.out << '\n';
        }

        if (format_settings.pretty)
        {
            const auto annotation = QueryPlanFormat::getColumnAnnotation(query_info.row_level_filter->column_name, format_settings);
            if (!annotation.empty())
                format_settings.out << prefix << annotation << '\n';
        }

        if (!format_settings.compact)
        {
            auto expression = std::make_shared<ExpressionActions>(query_info.row_level_filter->actions.clone());
            expression->describeActions(format_settings.out, prefix);
        }
    }
}

void SourceStepWithFilter::describeActions(JSONBuilder::JSONMap & map) const
{
    std::unique_ptr<JSONBuilder::JSONMap> prewhere_info_map;
    if (query_info.prewhere_info || query_info.row_level_filter)
    {
        prewhere_info_map = std::make_unique<JSONBuilder::JSONMap>();
        if (query_info.prewhere_info)
            prewhere_info_map->add("Need filter", query_info.prewhere_info->need_filter);
    }

    if (query_info.prewhere_info)
    {
        std::unique_ptr<JSONBuilder::JSONMap> prewhere_filter_map = std::make_unique<JSONBuilder::JSONMap>();
        prewhere_filter_map->add("Prewhere filter column", query_info.prewhere_info->prewhere_column_name);
        prewhere_filter_map->add("Prewhere filter remove filter column", query_info.prewhere_info->remove_prewhere_column);
        auto expression = std::make_shared<ExpressionActions>(query_info.prewhere_info->prewhere_actions.clone());
        prewhere_filter_map->add("Prewhere filter expression", expression->toTree());

        prewhere_info_map->add("Prewhere filter", std::move(prewhere_filter_map));
    }

    if (query_info.row_level_filter)
    {
        std::unique_ptr<JSONBuilder::JSONMap> row_level_filter_map = std::make_unique<JSONBuilder::JSONMap>();
        row_level_filter_map->add("Row level filter column", query_info.row_level_filter->column_name);
        auto expression = std::make_shared<ExpressionActions>(query_info.row_level_filter->actions.clone());
        row_level_filter_map->add("Row level filter expression", expression->toTree());

        prewhere_info_map->add("Row level filter", std::move(row_level_filter_map));
    }

    if (prewhere_info_map)
        map.add("Prewhere info", std::move(prewhere_info_map));
}

}
