#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <boost/algorithm/string.hpp>
#include <base/EnumReflection.h>

namespace DB
{

namespace
{
    /// Generates a CREATE TABLE query for an inner table.
    boost::intrusive_ptr<ASTCreateQuery> getInnerTableCreateQuery(
        ViewTarget::Kind inner_table_kind,
        const UUID & inner_table_uuid,
        const ASTColumns & inner_columns,
        boost::intrusive_ptr<ASTStorage> inner_storage_def,
        const StorageID & time_series_storage_id)
    {
        auto manual_create_query = make_intrusive<ASTCreateQuery>();

        manual_create_query->setDatabase(time_series_storage_id.getDatabaseName());
        manual_create_query->setTable(getTimeSeriesInnerTableName(inner_table_kind, time_series_storage_id));
        manual_create_query->uuid = inner_table_uuid;
        manual_create_query->has_uuid = inner_table_uuid != UUIDHelpers::Nil;

        auto new_columns_list = make_intrusive<ASTColumns>();
        if (inner_columns.columns)
            new_columns_list->set(
                new_columns_list->columns,
                boost::static_pointer_cast<ASTExpressionList>(inner_columns.columns->clone()));
        manual_create_query->set(manual_create_query->columns_list, new_columns_list);

        if (inner_storage_def)
            manual_create_query->set(manual_create_query->storage, inner_storage_def->clone());

        return manual_create_query;
    }
}


void createTimeSeriesInnerTable(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    const ASTColumns & inner_columns,
    boost::intrusive_ptr<ASTStorage> inner_storage_def,
    const StorageID & time_series_storage_id,
    ContextPtr context)
{
    auto create_context = Context::createCopy(context);

    auto manual_create_query = getInnerTableCreateQuery(
        inner_table_kind, inner_table_uuid, inner_columns,
        inner_storage_def, time_series_storage_id);

    InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();
}


String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id)
{
    String kind_str{magic_enum::enum_name(inner_table_kind)};
    boost::algorithm::to_lower(kind_str);
    return getTimeSeriesInnerTableName(kind_str, time_series_storage_id);
}

String getTimeSeriesInnerTableName(std::string_view inner_table_kind, const StorageID & time_series_storage_id)
{
    if (time_series_storage_id.hasUUID())
        return fmt::format(".inner_id.{}.{}", inner_table_kind, time_series_storage_id.uuid);
    else
        return fmt::format(".inner.{}.{}", inner_table_kind, time_series_storage_id.table_name);
}


String getTimeSeriesRecentSamplesMVName(const StorageID & time_series_storage_id)
{
    return getTimeSeriesInnerTableName("recentsamplesmv", time_series_storage_id);
}


void createTimeSeriesRecentSamplesMV(
    const StorageID & samples_table_id,
    const StorageID & recent_samples_table_id,
    const StorageID & time_series_storage_id,
    ContextPtr context)
{
    /// SELECT id, timestamp, value FROM <samples table>
    auto select_query = make_intrusive<ASTSelectQuery>();
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        auto table = make_intrusive<ASTTablesInSelectQueryElement>();
        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(samples_table_id);
        table_exp->children.emplace_back(table_exp->database_and_table_name);
        table->table_expression = table_exp;
        tables->children.push_back(table);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
    }

    auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union->union_mode = SelectUnionMode::UNION_DEFAULT;
    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    select_with_union->children.push_back(std::move(list_of_selects));
    select_with_union->list_of_selects = select_with_union->children.back();

    /// CREATE MATERIALIZED VIEW <mv> TO <recent samples table> AS SELECT ...
    auto manual_create_query = make_intrusive<ASTCreateQuery>();
    manual_create_query->setDatabase(time_series_storage_id.getDatabaseName());
    manual_create_query->setTable(getTimeSeriesRecentSamplesMVName(time_series_storage_id));
    manual_create_query->is_materialized_view = true;
    /// The materialized view can already exist when the TimeSeries table is restored from a backup.
    manual_create_query->if_not_exists = true;

    auto targets = make_intrusive<ASTViewTargets>();
    targets->setTableID(ViewTarget::To, recent_samples_table_id);
    manual_create_query->set(manual_create_query->targets, targets);

    manual_create_query->set(manual_create_query->select, select_with_union);

    auto create_context = Context::createCopy(context);
    InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();
}
}
