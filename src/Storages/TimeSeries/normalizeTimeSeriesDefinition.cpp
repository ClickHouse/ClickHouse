#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <Access/Common/AccessFlags.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

namespace
{
    /// Reads the columns of a table, which must exist.
    ColumnsDescription readTableColumns(const StorageID & table_id, const ContextPtr & context)
    {
        auto resolved_table_id = context->tryResolveStorageID(table_id);
        context->checkAccess(AccessType::SHOW_COLUMNS, resolved_table_id.database_name, resolved_table_id.table_name);
        auto table = DatabaseCatalog::instance().tryGetTable(resolved_table_id, context);
        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", table_id.getNameForLogs());
        auto metadata = table->getInMemoryMetadataPtr(context, false);
        return metadata->columns;
    }

    /// Reads the columns of the external target tables of a CREATE query.
    std::map<ViewTarget::Kind, ColumnsDescription> readExternalTargetColumns(const ASTCreateQuery & create_query, const ContextPtr & context)
    {
        std::map<ViewTarget::Kind, ColumnsDescription> result;
        for (auto kind : StorageTimeSeries::getTargetKinds())
        {
            if (create_query.hasTargetTableID(kind))
                result[kind] = readTableColumns(create_query.getTargetTableID(kind), context);
        }
        return result;
    }

    /// Reads the stored CREATE query of the table from the clause `AS <other_table>` of `create_query`.
    boost::intrusive_ptr<const ASTCreateQuery> readASCreateQuery(const ASTCreateQuery & create_query, const ContextPtr & context)
    {
        chassert(!create_query.as_table.empty());
        auto as_database = context->resolveDatabase(create_query.as_database);
        context->checkAccess(AccessType::SHOW_COLUMNS, as_database, create_query.as_table);
        return boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(as_database)->getCreateTableQuery(create_query.as_table, context));
    }
}


void normalizeTimeSeriesDefinition(ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup)
{
    chassert(create_query.is_time_series_table);

    NormalizeTimeSeriesDefinitionParams params;
    params.mode = mode;
    params.is_restore_from_backup = is_restore_from_backup;

    /// Only a new table needs the information from outside its definition; the other tables must exist then
    /// (on ATTACH they may not be loaded yet).
    if (params.isNewTable())
    {
        params.query_settings = &context->getSettingsRef();
        params.external_target_columns = readExternalTargetColumns(create_query, context);

        if (!create_query.as_table.empty())
            params.as_create_query = readASCreateQuery(create_query, context);
    }

    normalizeTimeSeriesDefinitionImpl(create_query, params);
}

}
