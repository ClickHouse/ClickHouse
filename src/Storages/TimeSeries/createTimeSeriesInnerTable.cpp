#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>

#include <Common/FailPoint.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
#include <boost/algorithm/string.hpp>
#include <base/EnumReflection.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char time_series_inner_table_fail_after_create[];
}

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

        manual_create_query->set(manual_create_query->columns_list, inner_columns.clone());

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
    ContextPtr context,
    bool & created)
{
    created = false;

    auto create_context = Context::createCopy(context);
    /// The generated codecs of the samples and recent samples tables are declared for the elements
    /// of the tuples in the `samples` column (subcolumn codecs) independently of the query settings.
    if ((inner_table_kind == ViewTarget::Samples) || (inner_table_kind == ViewTarget::RecentSamples))
        create_context->setSetting("enable_subcolumn_codecs", true);

    auto manual_create_query = getInnerTableCreateQuery(
        inner_table_kind, inner_table_uuid, inner_columns,
        inner_storage_def, time_series_storage_id, version);

    /// The DDL guard of the inner table is held across its `CREATE`, so no other query can create or drop a table with its name
    /// meanwhile: a table with this name that exists after the `CREATE` and did not exist before it was published by this `CREATE`,
    /// whatever the `CREATE` throws. The error code cannot tell it: e.g. column defaults are checked before the existence check.
    /// The inner `CREATE` is never replicated on its own (in a `Replicated` database it runs inside the replicated `CREATE` of the
    /// `TimeSeries` table), so it does not reach the branch of `InterpreterCreateQuery` that takes this guard regardless.
    const auto & database_name = manual_create_query->getDatabase();
    const auto & table_name = manual_create_query->getTable();
    auto database = DatabaseCatalog::instance().getDatabase(database_name);
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, table_name, database.get());
    bool existed = database->isTableExist(table_name, context);

    try
    {
        InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
        create_interpreter.setInternal(true);
        create_interpreter.setDontNeedDDLGuard(); /// It is held above.
        create_interpreter.execute();

        /// Simulates an error after the inner table is published, such as one from its `startup`.
        fiu_do_on(FailPoints::time_series_inner_table_fail_after_create,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Failpoint time_series_inner_table_fail_after_create");
        });
    }
    catch (...)
    {
        /// An exception from this check would replace the error of the `CREATE` and leave a published table behind.
        LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
        created = !existed && database->isTableExist(table_name, context);
        throw;
    }

    created = true;
}


String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id, UInt64 version)
{
    if ((inner_table_kind == ViewTarget::MetricFamilies) && (version < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME))
        return getTimeSeriesInnerTableName("metrics", time_series_storage_id);

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
}
