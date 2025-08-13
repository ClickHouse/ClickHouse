#pragma once

#include <Core/UUID.h>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
class AccessControl;
enum class AccessEntityType : uint8_t;
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;
class AccessRightsElements;
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
struct RestoreSettings;
enum class RestoreAccessCreationMode : uint8_t;


/// Makes a backup of access entities of a specified type.
std::pair<String, BackupEntryPtr> makeBackupEntryForAccess(
    const std::vector<std::pair<UUID, AccessEntityPtr>> & access_entities,
    const String & data_path_in_backup,
    size_t counter,
    const AccessControl & access_control);


/// Restores access entities from a backup.
class AccessRestorerFromBackup
{
public:
    AccessRestorerFromBackup(const BackupPtr & backup_, const RestoreSettings & restore_settings_);
    ~AccessRestorerFromBackup();

    /// Adds a data path to loads access entities from.
    void addDataPath(const String & data_path);

    /// Checks that the current user can do restoring.
    AccessRightsElements getRequiredAccess() const;

    /// Inserts all access entities loaded from all the paths added by addDataPath().
    std::vector<std::pair<UUID, AccessEntityPtr>> getAccessEntities(const AccessControl & access_control) const;

private:
    BackupPtr backup;
    RestoreAccessCreationMode creation_mode;
    bool allow_unresolved_dependencies = false;
    std::vector<std::pair<UUID, AccessEntityPtr>> entities;
    std::unordered_map<UUID, std::pair<String, AccessEntityType>> dependencies;
    std::unordered_set<String> data_paths;
};

}
