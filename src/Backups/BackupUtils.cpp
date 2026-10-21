#include <Access/Common/AccessRightsElement.h>
#include <Backups/BackupUtils.h>
#include <Backups/DDLAdjustingForBackupVisitor.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <Common/typeid_cast.h>

#include <algorithm>


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


namespace
{
    bool wildcardElementCoversSystemNamedCollections(const ASTBackupQuery::Element & element)
    {
        static const DatabaseAndTableName system_named_collections{DatabaseCatalog::SYSTEM_DATABASE, "named_collections"};

        if (element.type == ASTBackupQuery::DATABASE)
            return (element.database_name == system_named_collections.first)
                && !element.except_tables.contains(system_named_collections);

        if (element.type == ASTBackupQuery::ALL)
            return !element.except_databases.contains(system_named_collections.first)
                && !element.except_tables.contains(system_named_collections);

        return false;
    }
}

bool wildcardSelectionCoversSystemNamedCollections(const ASTBackupQuery::Elements & elements)
{
    return std::ranges::any_of(elements, wildcardElementCoversSystemNamedCollections);
}

bool explicitSelectionCoversSystemNamedCollections(const ASTBackupQuery::Elements & elements)
{
    return std::ranges::any_of(
        elements,
        [](const ASTBackupQuery::Element & element)
        {
            return (element.type == ASTBackupQuery::TABLE) && (element.database_name == DatabaseCatalog::SYSTEM_DATABASE)
                && (element.table_name == "named_collections");
        });
}

/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements, std::optional<bool> backup_named_collections)
{
    AccessRightsElements required_access;
    bool exports_named_collections = false;
    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                required_access.emplace_back(AccessType::BACKUP, element.database_name, element.table_name);
                /// Naming the table explicitly exports the collections unless `backup_named_collections`
                /// turns the export off (see `backupNamedCollections`).
                exports_named_collections |= (backup_named_collections != false)
                    && (element.database_name == DatabaseCatalog::SYSTEM_DATABASE)
                    && (element.table_name == "named_collections");
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
                /// A DATABASE or ALL element selecting the table requires the rights only with
                /// `backup_named_collections = true`: with the setting unset such an element exports
                /// the collections only if the user holds the rights (see `backupNamedCollections`),
                /// so nothing is required.
                exports_named_collections |= (backup_named_collections == true) && wildcardElementCoversSystemNamedCollections(element);
                break;
            }

            case ASTBackupQuery::ALL:
            {
                /// TODO: It's better to process `element.except_databases` & `element.except_tables` somehow.
                required_access.emplace_back(AccessType::BACKUP);
                exports_named_collections |= (backup_named_collections == true) && wildcardElementCoversSystemNamedCollections(element);
                break;
            }
        }
    }

    /// Exporting `system.named_collections` writes the values of all SQL-created named collections into
    /// the backup, so it requires the same rights as reading the values (see `backupNamedCollections`,
    /// which re-checks them on every host collecting the entries). The initiating host of a
    /// `BACKUP ON CLUSTER` query never runs the collector, and with the default
    /// `distributed_ddl_use_initial_user_and_roles = false` the other hosts execute the query with full
    /// access, so this list is the only check made against the initiating user before the query is
    /// sent to them.
    if (exports_named_collections)
    {
        required_access.emplace_back(AccessFlags{AccessType::SHOW_NAMED_COLLECTIONS} | AccessFlags{AccessType::SHOW_NAMED_COLLECTIONS_SECRETS});
        /// `SHOW NAMED COLLECTIONS SECRETS` alone does not allow reading the values back, reading them
        /// requires `displaySecretsInShowAndSelect` with it (see `StorageSystemNamedCollections`).
        required_access.emplace_back(AccessType::displaySecretsInShowAndSelect);
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

}
