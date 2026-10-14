#include <Access/Common/AccessRightsElement.h>
#include <Backups/BackupUtils.h>
#include <Backups/DDLAdjustingForBackupVisitor.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/StorageFactory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::BackupUtils
{

DDLRenamingMap makeRenamingMap(const ASTBackupQuery::Elements & elements)
{
    DDLRenamingMap map;

    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                const String & table_name = element.table_name;
                const String & database_name = element.database_name;
                const String & new_table_name = element.new_table_name;
                const String & new_database_name = element.new_database_name;
                chassert(!table_name.empty());
                chassert(!new_table_name.empty());
                chassert(!database_name.empty());
                chassert(!new_database_name.empty());
                map.setNewTableName({database_name, table_name}, {new_database_name, new_table_name});
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                const String & table_name = element.table_name;
                const String & new_table_name = element.new_table_name;
                chassert(!table_name.empty());
                chassert(!new_table_name.empty());
                map.setNewTableName({DatabaseCatalog::TEMPORARY_DATABASE, table_name}, {DatabaseCatalog::TEMPORARY_DATABASE, new_table_name});
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                const String & database_name = element.database_name;
                const String & new_database_name = element.new_database_name;
                chassert(!database_name.empty());
                chassert(!new_database_name.empty());
                map.setNewDatabaseName(database_name, new_database_name);
                break;
            }

            case ASTBackupQuery::ALL: break;
        }
    }
    return map;
}


/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements)
{
    AccessRightsElements required_access;
    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                required_access.emplace_back(AccessType::BACKUP, element.database_name, element.table_name);
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                /// It's always allowed to backup temporary tables.
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                /// TODO: It's better to process `element.except_tables` somehow.
                required_access.emplace_back(AccessType::BACKUP, element.database_name);
                break;
            }

            case ASTBackupQuery::ALL:
            {
                /// TODO: It's better to process `element.except_databases` & `element.except_tables` somehow.
                required_access.emplace_back(AccessType::BACKUP);
                break;
            }
        }
    }
    return required_access;
}

bool compareRestoredTableDef(const IAST & restored_table_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context)
{
    auto adjust_before_comparison = [&](const IAST & query) -> boost::intrusive_ptr<ASTCreateQuery>
    {
        auto new_query = boost::static_pointer_cast<ASTCreateQuery>(query.clone());
        adjustCreateQueryForBackup(new_query, global_context);
        new_query->resetUUIDs();
        new_query->if_not_exists = false;
        return new_query;
    };

    auto query1 = adjust_before_comparison(restored_table_create_query);
    auto query2 = adjust_before_comparison(create_query_from_backup);
    if (query1->formatWithSecretsOneLine() == query2->formatWithSecretsOneLine())
        return true;

    if (query1->is_time_series_table && query2->is_time_series_table)
    {
        /// Normally queries are stored already normalized in a backup,
        /// but in case there was an upgrade we may need to normalize the queries explicitly here.
        auto normalize_time_series = [&](ASTCreateQuery & query)
        {
            /// Use the same mode as InterpreterCreateQuery uses during RESTORE.
            normalizeTimeSeriesDefinition(query, global_context, LoadingStrictnessLevel::SECONDARY_CREATE, /*is_restore_from_backup=*/true);
        };
        normalize_time_series(*query1);
        normalize_time_series(*query2);
        if (query1->formatWithSecretsOneLine() == query2->formatWithSecretsOneLine())
            return true;
    }

    return false;
}

bool compareRestoredDatabaseDef(const IAST & restored_database_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context)
{
    return compareRestoredTableDef(restored_database_create_query, create_query_from_backup, global_context);
}

bool isInnerTable(const QualifiedTableName & table_name)
{
    return isInnerTable(table_name.database, table_name.table);
}

bool isInnerTable(const String & /* database_name */, const String & table_name)
{
    /// We skip inner tables of materialized views. They're backed up by StorageMaterializedView.
    return table_name.starts_with(".inner.") || table_name.starts_with(".inner_id.") || table_name.starts_with(".tmp.inner.") || table_name.starts_with(".tmp.inner_id.");
}

namespace
{

/// Looks through the `StorageTableProxy` of a lazily loaded table, which reports no storage policy. Only a `MergeTreeData`
/// keeps its data on a storage policy: `Alias`, `Distributed` and `StorageProxy` report one but keep no data of their own.
bool isMergeTreeOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage)
{
    auto merge_tree = castStorage<MergeTreeData>(storage, DeferredTable::Load);
    if (!merge_tree)
        return false;
    auto policy = merge_tree->getStoragePolicy();
    return policy && policy->isBackupMetadataOnly();
}

}

bool isDataOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage, const ContextPtr & context)
{
    StoragePtr inner_table_on_policy;
    if (const auto * materialized_view = typeid_cast<const StorageMaterializedView *>(storage.get()))
    {
        if (materialized_view->hasInnerTable())
        {
            if (auto target = materialized_view->tryGetTargetTable(); target && isMergeTreeOnBackupMetadataOnlyStoragePolicy(target))
                inner_table_on_policy = target;
        }
    }
    else if (const auto * time_series = typeid_cast<const StorageTimeSeries *>(storage.get()))
    {
        for (auto target_kind : StorageTimeSeries::getTargetKinds())
        {
            if (!time_series->isInnerTable(target_kind))
                continue;
            if (auto target = time_series->tryGetTargetTable(target_kind, context); target && isMergeTreeOnBackupMetadataOnlyStoragePolicy(target))
                inner_table_on_policy = target;
        }
    }
    else
    {
        return isMergeTreeOnBackupMetadataOnlyStoragePolicy(storage);
    }

    if (inner_table_on_policy)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "{} has the inner table {} on a storage policy marked backup_metadata_only. BACKUP and RESTORE support such policies "
            "only for tables that keep their data themselves, not for the inner tables of materialized views and TimeSeries tables",
            storage->getStorageID().getNameForLogs(),
            inner_table_on_policy->getStorageID().getNameForLogs());

    return false;
}

bool isCreatedOnBackupMetadataOnlyStoragePolicy(const ASTCreateQuery & create_query, const ContextPtr & context)
{
    const auto * storage = create_query.storage;
    if (!storage || !storage->engine || !storage->settings)
        return false;

    /// Only an explicit `storage_policy` matters: a read-only policy can't be the default one.
    const auto * policy_name = storage->settings->changes.tryGet("storage_policy");
    if (!policy_name)
        return false;

    /// Only the engines with the setting keep their data on a storage policy. An unknown engine is reported by `CREATE`.
    const auto & storages = StorageFactory::instance().getAllStorages();
    auto it = storages.find(storage->engine->name);
    if (it == storages.end() || !it->second.features.has_builtin_setting_fn || !it->second.features.has_builtin_setting_fn("storage_policy"))
        return false;

    return context->getStoragePolicy(policy_name->safeGet<String>())->isBackupMetadataOnly();
}

}
