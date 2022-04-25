#pragma once

#include <Backups/IRestoreCoordination.h>
#include <Common/ZooKeeper/Common.h>
#include <map>
#include <mutex>


namespace DB
{

/// Stores restore temporary information in Zookeeper, used to perform RESTORE ON CLUSTER.
class RestoreCoordinationDistributed : public IRestoreCoordination
{
public:
    RestoreCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_);
    ~RestoreCoordinationDistributed() override;

    void setOrGetPathInBackupForZkPath(const String & zk_path_, String & path_in_backup_) override;

    bool acquireZkPathAndName(const String & zk_path_, const String & name_) override;
    void setResultForZkPathAndName(const String & zk_path_, const String & name_, Result res_) override;
    bool getResultForZkPathAndName(const String & zk_path_, const String & name_, Result & res_, std::chrono::milliseconds timeout_) const override;

    void drop() override;

private:
    void createRootNodes();
    void removeAllNodes();

    const String zookeeper_path;
    const zkutil::GetZooKeeper get_zookeeper;
    mutable std::mutex mutex;
    mutable std::map<std::pair<String, String>, std::optional<Result>> acquired;
    std::unordered_map<String, String> paths_in_backup_by_zk_path;
};

}
