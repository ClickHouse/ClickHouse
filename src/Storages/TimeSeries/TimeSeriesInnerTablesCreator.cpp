#include <Storages/TimeSeries/TimeSeriesInnerTablesCreator.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
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

TimeSeriesInnerTablesCreator::TimeSeriesInnerTablesCreator(ContextPtr context_,
                                                           StorageID time_series_storage_id_,
                                                           std::reference_wrapper<const ColumnsDescription> time_series_columns_,
                                                           std::reference_wrapper<const TimeSeriesSettings> time_series_settings_)
    : WithContext(context_)
    , time_series_storage_id(std::move(time_series_storage_id_))
    , time_series_columns(time_series_columns_)
    , time_series_settings(time_series_settings_)
{
}

TimeSeriesInnerTablesCreator::~TimeSeriesInnerTablesCreator() = default;


ColumnsDescription TimeSeriesInnerTablesCreator::getInnerTableColumnsDescription(ViewTarget::Kind inner_table_kind) const
{
    ColumnsDescription columns;

    switch (inner_table_kind)
    {
        case ViewTarget::Data:
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

        case ViewTarget::Tags:
        {
            /// Column "id".
            columns.add(time_series_columns.get(TimeSeriesColumnNames::ID));

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
            if (time_series_settings.use_all_tags_column_to_generate_id)
            {
                ColumnDescription all_tags_column = time_series_columns.get(TimeSeriesColumnNames::AllTags);
                /// Column "all_tags" is here only to calculate the identifier of a time series for the "id" column, so it can be ephemeral.
                all_tags_column.default_desc.kind = ColumnDefaultKind::Ephemeral;
                if (!all_tags_column.default_desc.expression)
                {
                    all_tags_column.default_desc.ephemeral_default = true;
                    all_tags_column.default_desc.expression = makeASTFunction("defaultValueOfTypeName", std::make_shared<ASTLiteral>(all_tags_column.type->getName()));
                }
                columns.add(std::move(all_tags_column));
            }

            /// Columns "min_time" and "max_time".
            if (time_series_settings.store_min_time_and_max_time)
            {
                auto min_time_column = time_series_columns.get(TimeSeriesColumnNames::MinTime);
                auto max_time_column = time_series_columns.get(TimeSeriesColumnNames::MaxTime);
                if (time_series_settings.aggregate_min_time_and_max_time)
                {
                    AggregateFunctionProperties properties;
                    auto min_function = AggregateFunctionFactory::instance().get("min", NullsAction::EMPTY, {min_time_column.type}, {}, properties);
                    auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(min_function, DataTypes{min_time_column.type}, Array{});
                    min_time_column.type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));

                    auto max_function = AggregateFunctionFactory::instance().get("max", NullsAction::EMPTY, {max_time_column.type}, {}, properties);
                    custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(max_function, DataTypes{max_time_column.type}, Array{});
                    max_time_column.type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
                }
                columns.add(std::move(min_time_column));
                columns.add(std::move(max_time_column));
            }

            break;
        }

        case ViewTarget::Metrics:
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


StorageID TimeSeriesInnerTablesCreator::getInnerTableID(ViewTarget::Kind inner_table_kind, const UUID & inner_table_uuid) const
{
    StorageID res = time_series_storage_id;
    if (time_series_storage_id.hasUUID())
        res.table_name = fmt::format(".inner_id.{}.{}", toString(inner_table_kind), time_series_storage_id.uuid);
    else
        res.table_name = fmt::format(".inner.{}.{}", toString(inner_table_kind), time_series_storage_id.table_name);
    res.uuid = inner_table_uuid;
    return res;
}


std::shared_ptr<ASTCreateQuery> TimeSeriesInnerTablesCreator::getInnerTableCreateQuery(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    const std::shared_ptr<ASTStorage> & inner_storage_def) const
{
    auto manual_create_query = std::make_shared<ASTCreateQuery>();

    auto inner_table_id = getInnerTableID(inner_table_kind, inner_table_uuid);
    manual_create_query->setDatabase(inner_table_id.database_name);
    manual_create_query->setTable(inner_table_id.table_name);
    manual_create_query->uuid = inner_table_id.uuid;
    manual_create_query->has_uuid = inner_table_id.uuid != UUIDHelpers::Nil;

    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(getInnerTableColumnsDescription(inner_table_kind)));
    manual_create_query->set(manual_create_query->columns_list, new_columns_list);

    if (inner_storage_def)
        manual_create_query->set(manual_create_query->storage, inner_storage_def->clone());

    return manual_create_query;
}

StorageID TimeSeriesInnerTablesCreator::createInnerTable(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    const std::shared_ptr<ASTStorage> & inner_storage_def) const
{
    /// We will make a query to create the inner target table.
    auto create_context = Context::createCopy(getContext());

    auto manual_create_query = getInnerTableCreateQuery(inner_table_kind, inner_table_uuid, inner_storage_def);

    /// Create the inner target table.
    InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();

    return DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, getContext())->getStorageID();
}

}
