#include <Storages/TimeSeries/TimeSeriesInnerTablesCreator.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace
{
    using TargetKind = ViewTarget::Kind;

    /// Makes description of the columns of an inner target table.
    ColumnsDescription getInnerTableColumnsDescription(
        TargetKind kind, const ColumnsDescription & time_series_columns, const TimeSeriesSettings & time_series_settings)
    {
        ColumnsDescription columns;

        switch (kind)
        {
            case TargetKind::Data:
            {
                /// Column "id".
                {
                    auto id_column = time_series_columns.get(TimeSeriesColumnNames::ID);
                    /// The expression for calculating the identifier of a time series can be transferred only to the "tags" inner table
                    /// (because it usually depends on columns like "metric_name" or "all_tags").
                    id_column.default_desc = {};
                    columns.add(std::move(id_column));
                }

                /// Column "timestamp".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Timestamp));

                /// Column "value".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Value));
                break;
            }

            case TargetKind::Tags:
            {
                /// Column "id".
                {
                    auto id_column = time_series_columns.get(TimeSeriesColumnNames::ID);
                    if (!time_series_settings.set_id_default_expression_in_tags_table)
                        id_column.default_desc = {};
                    columns.add(std::move(id_column));
                }

                /// Column "metric_name".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::MetricName));

                /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                const Map & tags_to_columns = time_series_settings.tags_to_columns;
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    columns.add(time_series_columns.get(column_name));
                }

                /// Column "tags".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Tags));

                /// Column "all_tags".
                if (time_series_settings.use_all_tags_column_for_calculating_id && time_series_settings.set_id_default_expression_in_tags_table)
                {
                    ColumnDescription all_tags_column;
                    if (const auto * existing_column = time_series_columns.tryGet(TimeSeriesColumnNames::AllTags))
                        all_tags_column = *existing_column;
                    else
                        all_tags_column = ColumnDescription{TimeSeriesColumnNames::AllTags, std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())};
                    /// Column "all_tags" is here only to calculate the identifier of a time series for the "id" column, so it can be ephemeral.
                    all_tags_column.default_desc.kind = ColumnDefaultKind::Ephemeral;
                    if (!all_tags_column.default_desc.expression)
                    {
                        all_tags_column.default_desc.ephemeral_default = true;
                        all_tags_column.default_desc.expression = makeASTFunction("defaultValueOfTypeName", std::make_shared<ASTLiteral>(all_tags_column.type->getName()));
                    }
                    columns.add(std::move(all_tags_column));
                }

                break;
            }

            case TargetKind::Metrics:
            {
                columns.add(time_series_columns.get(TimeSeriesColumnNames::MetricFamilyName));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Type));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Unit));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Help));
                break;
            }

            default:
                UNREACHABLE();
        }

        return columns;
    }
}


StorageID TimeSeriesInnerTablesCreator::getInnerTableId(TargetKind kind, const ViewTarget * target_info) const
{
    StorageID res = time_series_storage_id;
    if (time_series_storage_id.hasUUID())
        res.table_name = fmt::format(".inner_id.{}.{}", toString(kind), time_series_storage_id.uuid);
    else
        res.table_name = fmt::format(".inner.{}.{}", toString(kind), time_series_storage_id.table_name);
    if (target_info)
        res.uuid = target_info->inner_uuid;
    return res;
}

/// Generates a CREATE query for creating an inner target table.
std::shared_ptr<ASTCreateQuery> TimeSeriesInnerTablesCreator::generateCreateQueryForInnerTable(
    TargetKind kind,
    const ViewTarget * target_info,
    const ColumnsDescription & time_series_columns,
    const TimeSeriesSettings & time_series_settings) const
{
    auto create = std::make_shared<ASTCreateQuery>();

    auto inner_table_id = getInnerTableId(kind, target_info);
    create->setDatabase(inner_table_id.database_name);
    create->setTable(inner_table_id.table_name);
    create->uuid = inner_table_id.uuid;

    auto new_columns_list = std::make_shared<ASTColumns>();
    create->set(create->columns_list, new_columns_list);
    new_columns_list->set(
        new_columns_list->columns,
        InterpreterCreateQuery::formatColumns(
            getInnerTableColumnsDescription(kind, time_series_columns, time_series_settings)));

    if (target_info && target_info->table_engine)
        create->set(create->storage, target_info->table_engine->clone());

    return create;
}

StorageID TimeSeriesInnerTablesCreator::createInnerTable(
    TargetKind kind,
    const ViewTarget * target_info,
    const ContextPtr & context,
    const ColumnsDescription & time_series_columns,
    const TimeSeriesSettings & time_series_settings) const
{
    /// We will make a query to create the inner target table.
    auto create_context = Context::createCopy(context);

    auto manual_create_query = generateCreateQueryForInnerTable(kind, target_info, time_series_columns, time_series_settings);

    /// Create the inner target table.
    InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();

    return DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, context)->getStorageID();
}

}
