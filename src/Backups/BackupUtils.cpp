#include <Access/Common/AccessRightsElement.h>
#include <Backups/BackupUtils.h>
#include <Backups/DDLAdjustingForBackupVisitor.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
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

String getMetadataVersionPathInBackup(const String & table_metadata_path_in_backup)
{
    constexpr std::string_view ext = ".sql";
    chassert(table_metadata_path_in_backup.ends_with(ext));
    return table_metadata_path_in_backup.substr(0, table_metadata_path_in_backup.size() - ext.size()) + ".metadata_version.txt";
}

}
