#pragma once

#include <unordered_map>
#include <string>

namespace DB
{

struct BackupCoordinationKeeperMapTables
{
    void addTable(const std::string & table_zookeeper_root_path, const std::string & table_id, const std::string & data_path_in_backup);
    std::string getDataPath(const std::string & table_zookeeper_root_path) const;

    struct KeeperMapTableInfo
    {
        std::string table_id;
        std::string data_path_in_backup;
    };
private:
    std::unordered_map<std::string /* root zookeeper path */, KeeperMapTableInfo> tables_with_info;
};

}
