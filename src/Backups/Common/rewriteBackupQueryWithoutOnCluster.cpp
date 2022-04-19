#include <Backups/Common/rewriteBackupQueryWithoutOnCluster.h>
#include <Backups/Common/BackupSettings.h>
#include <Backups/Common/RestoreSettings.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

namespace
{
    void setDatabaseInElements(ASTBackupQuery::Elements & elements, const String & new_database)
    {
        for (auto & element : elements)
        {
            if (element.type == ASTBackupQuery::TABLE)
            {
                if (element.name.first.empty() && !element.name.second.empty() && !element.name_is_in_temp_db)
                    element.name.first = new_database;
                if (element.new_name.first.empty() && !element.name.second.empty() && !element.name_is_in_temp_db)
                    element.new_name.first = new_database;
            }
        }
    }
}

std::shared_ptr<ASTBackupQuery>
rewriteBackupQueryWithoutOnCluster(const ASTBackupQuery & backup_query, const WithoutOnClusterASTRewriteParams & params)
{
    auto backup_settings = BackupSettings::fromBackupQuery(backup_query);
    backup_settings.internal = true;
    backup_settings.async = false;
    backup_settings.shard = params.shard_index;
    backup_settings.replica = params.replica_index;
    auto new_query = std::static_pointer_cast<ASTBackupQuery>(backup_query.clone());
    new_query->cluster.clear();
    backup_settings.copySettingsToBackupQuery(*new_query);
    setDatabaseInElements(new_query->elements, params.default_database);
    return new_query;
}


std::shared_ptr<ASTBackupQuery>
rewriteRestoreQueryWithoutOnCluster(const ASTBackupQuery & restore_query, const WithoutOnClusterASTRewriteParams & params)
{
    auto restore_settings = RestoreSettings::fromRestoreQuery(restore_query);
    restore_settings.internal = true;
    restore_settings.async = false;
    restore_settings.shard = params.shard_index;
    restore_settings.replica = params.replica_index;
    auto new_query = std::static_pointer_cast<ASTBackupQuery>(restore_query.clone());
    new_query->cluster.clear();
    restore_settings.copySettingsToRestoreQuery(*new_query);
    setDatabaseInElements(new_query->elements, params.default_database);
    return new_query;
}

}
