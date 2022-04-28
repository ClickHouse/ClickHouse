#pragma once

#include <Backups/IRestoreCoordination.h>
#include <map>
#include <mutex>
#include <unordered_map>


namespace DB
{

class RestoreCoordinationLocal : public IRestoreCoordination
{
public:
    RestoreCoordinationLocal();
    ~RestoreCoordinationLocal() override;

    void setOrGetPathInBackupForZkPath(const String & zk_path_, String & path_in_backup_) override;

    bool acquireZkPathAndName(const String & zk_path_, const String & name_) override;
    void setResultForZkPathAndName(const String & zk_path_, const String & name_, Result res_) override;
    bool getResultForZkPathAndName(const String & zk_path_, const String & name_, Result & res_, std::chrono::milliseconds timeout_) const override;

private:
    std::optional<Result> & getResultRef(const String & zk_path_, const String & name_);
    const std::optional<Result> & getResultRef(const String & zk_path_, const String & name_) const;

    mutable std::mutex mutex;
    std::unordered_map<String, String> paths_in_backup_by_zk_path;
    std::map<std::pair<String, String>, std::optional<Result>> acquired;
    mutable std::condition_variable result_changed;
};

}
