#include <Backups/BackupCoordinationKeeperMapTables.h>

namespace DB
{

void BackupCoordinationKeeperMapTables::addTable(const std::string & table_zookeeper_root_path, const std::string & table_id, const std::string & data_path_in_backup)
{
    if (auto it = tables_with_info.find(table_zookeeper_root_path); it != tables_with_info.end())
    {
        if (table_id > it->second.table_id)
            it->second = KeeperMapTableInfo{table_id, data_path_in_backup};
        return;
    }

    tables_with_info.emplace(table_zookeeper_root_path, KeeperMapTableInfo{table_id, data_path_in_backup});
}

std::string BackupCoordinationKeeperMapTables::getDataPath(const std::string & table_zookeeper_root_path) const
{
    return tables_with_info.at(table_zookeeper_root_path).data_path_in_backup;
}

}
