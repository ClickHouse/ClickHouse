#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

Block SourceStepWithFilter::applyPrewhereActions(Block block, const PrewhereInfoPtr & prewhere_info)
{
    if (prewhere_info)
    {
        if (prewhere_info->row_level_filter)
        {
            block = prewhere_info->row_level_filter->updateHeader(block);
            auto & row_level_column = block.getByName(prewhere_info->row_level_column_name);
            if (!row_level_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                    "Invalid type for filter in PREWHERE: {}",
                    row_level_column.type->getName());
            }

            block.erase(prewhere_info->row_level_column_name);
        }

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
    filter_actions_dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, {});
}

void SourceStepWithFilter::applyFilters(ActionDAGNodes added_filter_nodes)
{
    filter_actions_dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, query_info.buildNodeNameToInputNodeColumn());
}

void SourceStepWithFilter::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    query_info.prewhere_info = prewhere_info_value;
    prewhere_info = prewhere_info_value;
    output_header = applyPrewhereActions(*output_header, prewhere_info);
}

void SourceStepWithFilter::describeActions(FormatSettings & format_settings) const
{
    std::string prefix(format_settings.offset, format_settings.indent_char);

    if (prewhere_info)
    {
        format_settings.out << prefix << "Prewhere info" << '\n';
        format_settings.out << prefix << "Need filter: " << prewhere_info->need_filter << '\n';

        prefix.push_back(format_settings.indent_char);
        prefix.push_back(format_settings.indent_char);

        {
            format_settings.out << prefix << "Prewhere filter" << '\n';
            format_settings.out << prefix << "Prewhere filter column: " << prewhere_info->prewhere_column_name;
            if (prewhere_info->remove_prewhere_column)
                format_settings.out << " (removed)";
            format_settings.out << '\n';

            auto expression = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions.clone());
            expression->describeActions(format_settings.out, prefix);
        }

        if (prewhere_info->row_level_filter)
        {
            format_settings.out << prefix << "Row level filter" << '\n';
            format_settings.out << prefix << "Row level filter column: " << prewhere_info->row_level_column_name << '\n';

            auto expression = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter->clone());
            expression->describeActions(format_settings.out, prefix);
        }
    }
}

void SourceStepWithFilter::describeActions(JSONBuilder::JSONMap & map) const
{
    if (prewhere_info)
    {
        std::unique_ptr<JSONBuilder::JSONMap> prewhere_info_map = std::make_unique<JSONBuilder::JSONMap>();
        prewhere_info_map->add("Need filter", prewhere_info->need_filter);

        {
            std::unique_ptr<JSONBuilder::JSONMap> prewhere_filter_map = std::make_unique<JSONBuilder::JSONMap>();
            prewhere_filter_map->add("Prewhere filter column", prewhere_info->prewhere_column_name);
            prewhere_filter_map->add("Prewhere filter remove filter column", prewhere_info->remove_prewhere_column);
            auto expression = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions.clone());
            prewhere_filter_map->add("Prewhere filter expression", expression->toTree());

            prewhere_info_map->add("Prewhere filter", std::move(prewhere_filter_map));
        }

        if (prewhere_info->row_level_filter)
        {
            std::unique_ptr<JSONBuilder::JSONMap> row_level_filter_map = std::make_unique<JSONBuilder::JSONMap>();
            row_level_filter_map->add("Row level filter column", prewhere_info->row_level_column_name);
            auto expression = std::make_shared<ExpressionActions>(prewhere_info->row_level_filter->clone());
            row_level_filter_map->add("Row level filter expression", expression->toTree());

            prewhere_info_map->add("Row level filter", std::move(row_level_filter_map));
        }

        map.add("Prewhere info", std::move(prewhere_info_map));
    }
}

}
