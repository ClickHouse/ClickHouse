#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <mutex>
#include <unordered_map>


namespace DB
{
class BackupInMemory;

/// Holds all backups stored in memory during the current user's session.
class BackupsInMemoryHolder
{
public:
    BackupsInMemoryHolder();
    ~BackupsInMemoryHolder();

    std::shared_ptr<BackupInMemory> createBackup(const String & backup_name);
    std::shared_ptr<const BackupInMemory> getBackup(const String & backup_name) const;
    void dropBackup(const String & backup_name);

private:
    std::unordered_map<String, std::shared_ptr<BackupInMemory>> backups TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};

}
