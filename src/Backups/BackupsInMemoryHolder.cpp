#include <Backups/BackupsInMemoryHolder.h>

#include <Backups/BackupInMemory.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_NOT_FOUND;
    extern const int BACKUP_ALREADY_EXISTS;
}


BackupsInMemoryHolder::BackupsInMemoryHolder() = default;
BackupsInMemoryHolder::~BackupsInMemoryHolder() = default;


std::shared_ptr<BackupInMemory> BackupsInMemoryHolder::createBackup(const String & backup_name)
{
    std::lock_guard lock{mutex};
    auto it = backups.find(backup_name);
    if (it != backups.end())
        throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", backup_name);
    auto new_backup = std::make_shared<BackupInMemory>(backup_name, *this);
    backups.emplace(backup_name, new_backup);
    return new_backup;
}


std::shared_ptr<const BackupInMemory> BackupsInMemoryHolder::getBackup(const String & backup_name) const
{
    std::lock_guard lock{mutex};
    auto it = backups.find(backup_name);
    if (it == backups.end())
        throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", backup_name);
    return it->second;
}


void BackupsInMemoryHolder::dropBackup(const String & backup_name)
{
    std::lock_guard lock{mutex};
    auto it = backups.find(backup_name);
    if (it == backups.end())
        throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", backup_name);
    backups.erase(it);
}

}
