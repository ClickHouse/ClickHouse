#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
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
        const StorageID & time_series_storage_id,
        UInt64 version)
    {
        auto manual_create_query = make_intrusive<ASTCreateQuery>();

        manual_create_query->setDatabase(time_series_storage_id.getDatabaseName());
        manual_create_query->setTable(getTimeSeriesInnerTableName(inner_table_kind, time_series_storage_id, version));
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
    UInt64 version,
    ContextPtr context)
{
    auto create_context = Context::createCopy(context);

    auto manual_create_query = getInnerTableCreateQuery(
        inner_table_kind, inner_table_uuid, inner_columns,
        inner_storage_def, time_series_storage_id, version);

    InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();
}


String getTimeSeriesTargetKindName(ViewTarget::Kind target_kind, UInt64 version)
{
    if ((target_kind == ViewTarget::MetricFamilies) && (version < TimeSeriesVersion::FIRST_WITH_METRIC_FAMILIES_NAME))
        return "metrics";

    String kind_str{magic_enum::enum_name(target_kind)};
    boost::algorithm::to_lower(kind_str);
    return kind_str;
}

String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id, UInt64 version)
{
    return getTimeSeriesInnerTableName(getTimeSeriesTargetKindName(inner_table_kind, version), time_series_storage_id);
}

String getTimeSeriesInnerTableName(std::string_view inner_table_kind, const StorageID & time_series_storage_id)
{
    if (time_series_storage_id.hasUUID())
        return fmt::format(".inner_id.{}.{}", inner_table_kind, time_series_storage_id.uuid);
    else
        return fmt::format(".inner.{}.{}", inner_table_kind, time_series_storage_id.table_name);
}
}
