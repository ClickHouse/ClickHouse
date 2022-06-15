#pragma once

#include <Backups/RestoreSettings.h>
#include <unordered_map>


namespace DB
{
class AccessControl;
enum class AccessEntityType;
class BackupEntriesCollector;
class RestorerFromBackup;
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IRestoreCoordination;
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;


/// Makes a backup of access entities of a specified type.
void backupAccessEntities(
    BackupEntriesCollector & backup_entries_collector,
    const String & data_path_in_backup,
    const AccessControl & access_control,
    AccessEntityType type);

/// Restores access entities from a backup.
class AccessRestoreTask
{
public:
    AccessRestoreTask(
        const BackupPtr & backup_, const RestoreSettings & restore_settings_, std::shared_ptr<IRestoreCoordination> restore_coordination_);
    ~AccessRestoreTask();

    /// Adds a data path to loads access entities from.
    void addDataPath(const String & data_path);

    /// Inserts all access entities loaded from all the paths added by addDataPath().
    void restore(AccessControl & access_control);

private:
    BackupPtr backup;
    RestoreSettings restore_settings;
    std::shared_ptr<IRestoreCoordination> restore_coordination;
    std::unordered_map<UUID, AccessEntityPtr> entities;
    std::unordered_map<UUID, std::pair<String, AccessEntityType>> dependencies;
};

}
