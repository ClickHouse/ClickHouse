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
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <Common/typeid_cast.h>


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

/// The `MergeTreeData` behind `storage` if it is on a storage policy marked `backup_metadata_only`. Only a `MergeTreeData`
/// keeps its data on a storage policy: `Alias`, `Distributed` and `StorageProxy` report one but keep no data of their own.
/// A proxy is not resolved here, unlike on master, where `resolveStorageProxyLoading` loads a lazily loaded table: the
/// helpers it needs are not on this branch, so a table behind a proxy keeps its data backed up as usual.
std::shared_ptr<MergeTreeData> getMergeTreeOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage)
{
    auto merge_tree = std::dynamic_pointer_cast<MergeTreeData>(storage);
    if (!merge_tree)
        return nullptr;
    auto policy = merge_tree->getStoragePolicy();
    if (!policy || !policy->isBackupMetadataOnly())
        return nullptr;
    return merge_tree;
}

}

/// `context` is unused here: only the `TimeSeries` targets left out on this branch needed it.
bool isDataOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage, [[maybe_unused]] const ContextPtr & context)
{
    /// Only a table of the plain `MergeTree` family reopens the parts it finds on the storage. `RESTORE` creates a
    /// `ReplicatedMergeTree` table in a mode that refuses them, a `SharedMergeTree` table sees only the parts in Keeper,
    /// and a view looks for the data of its inner tables in the backup. For the others the flag is ignored.
    ///
    /// Master warns when it ignores the flag, naming the inner table that carries it. The warning is left out here:
    /// it needs the inner tables of a `TimeSeries` table through `getTargetKinds`, which is not static on this
    /// branch, and the answer below does not depend on it.
    if (typeid_cast<const StorageMaterializedView *>(storage.get()) || typeid_cast<const StorageTimeSeries *>(storage.get()))
        return false;

    auto merge_tree = getMergeTreeOnBackupMetadataOnlyStoragePolicy(storage);
    if (!merge_tree)
        return false;

    return std::dynamic_pointer_cast<StorageMergeTree>(merge_tree) != nullptr;
}

bool isDataOnBackupMetadataOnlyStoragePolicy(const StoragePtr & storage, const ASTPtr & create_table_query, const ContextPtr & context)
{
    if (storage)
        return isDataOnBackupMetadataOnlyStoragePolicy(storage, context);

    const auto * create_query = create_table_query ? create_table_query->as<ASTCreateQuery>() : nullptr;
    return create_query && isCreatedOnBackupMetadataOnlyStoragePolicy(*create_query, context);
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

    /// Only an engine of the `MergeTree` family keeps its data on a storage policy, see
    /// `isDataOnBackupMetadataOnlyStoragePolicy`. `storage_policy` is a builtin setting of the `Log` family too, so what
    /// identifies the family is the settings the engine declares, not that it declares this one. An unknown engine is
    /// reported by `CREATE`.
    const auto & storages = StorageFactory::instance().getAllStorages();
    auto it = storages.find(storage->engine->name);
    if (it == storages.end() || it->second.features.has_builtin_setting_fn != MergeTreeSettings::hasBuiltin)
        return false;

    /// Only the plain family reopens the parts it finds on the storage: the replicated and shared engines are
    /// registered with `supports_replication`.
    if (it->second.features.supports_replication)
        return false;

    auto policy = context->tryGetStoragePolicy(policy_name->safeGet<String>());
    return policy && policy->isBackupMetadataOnly();
}

}
