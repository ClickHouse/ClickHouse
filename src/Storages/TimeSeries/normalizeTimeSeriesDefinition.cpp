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

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

namespace
{
    struct ExternalTargetInfo
    {
        std::map<ViewTarget::Kind, ColumnsDescription> columns;
        std::map<ViewTarget::Kind, String> engine_names;
        std::map<ViewTarget::Kind, ASTPtr> sorting_keys;
    };

    /// Reads the columns, engine names, and sorting keys of the external target tables of a definition.
    ExternalTargetInfo readExternalTargets(const ASTCreateQuery & create_query, const ContextPtr & context)
    {
        ExternalTargetInfo result;
        for (auto kind : StorageTimeSeries::getTargetKinds())
        {
            if (create_query.hasTargetTableID(kind))
            {
                const auto & table_id = create_query.getTargetTableID(kind);
                auto resolved_table_id = context->tryResolveStorageID(table_id);
                context->checkAccess(AccessType::SHOW_COLUMNS, resolved_table_id.database_name, resolved_table_id.table_name);
                auto table = DatabaseCatalog::instance().tryGetTable(resolved_table_id, context);
                if (!table)
                    throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", table_id.getNameForLogs());
                auto metadata = table->getInMemoryMetadataPtr(context, false);
                result.columns[kind] = metadata->columns;
                result.engine_names[kind] = table->getName();
                result.sorting_keys[kind] = metadata->getSortingKey().definition_ast;
            }
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

    /// A full user-supplied ATTACH also needs to validate external targets. A short ATTACH or metadata replay
    /// cannot assume that those tables have been loaded yet.
    bool fresh_user_attach = (mode == LoadingStrictnessLevel::ATTACH) && !create_query.attach_short_syntax && !is_restore_from_backup;
    if (params.isNewTable() || fresh_user_attach)
    {
        auto external_targets = readExternalTargets(create_query, context);
        params.external_target_columns = std::move(external_targets.columns);
        params.external_target_engine_names = std::move(external_targets.engine_names);
        params.external_target_sorting_keys = std::move(external_targets.sorting_keys);
    }

    if (params.isNewTable())
    {
        params.query_settings = &context->getSettingsRef();
        if (!create_query.as_table.empty())
            params.as_create_query = readASCreateQuery(create_query, context);
    }

    normalizeTimeSeriesDefinitionImpl(create_query, params);
}

}
