#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <memory>
#include <mutex>
#include <unordered_map>


namespace DB
{
class BackupsInMemoryHolder;
class ReadBufferFromFileBase;
class WriteBuffer;

/// Keeps a backup stored in memory (good for testing; they will be cleared when the server stops):
/// BACKUP ... TO Memory('backup_name')
/// RESTORE ... FROM Memory('backup_name')
class BackupInMemory : public std::enable_shared_from_this<BackupInMemory>
{
public:
    BackupInMemory(const String & backup_name_, BackupsInMemoryHolder & holder_);

    bool isEmpty() const;
    bool fileExists(const String & file_name) const;
    UInt64 getFileSize(const String & file_name) const;

    std::unique_ptr<WriteBuffer> writeFile(const String & file_name);
    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name) const;
    void removeFile(const String & file_name);
    void copyFile(const String & from, const String & to);

    void drop();

private:
    class WriteBufferToBackupInMemory;

    const String backup_name;
    BackupsInMemoryHolder & holder;
    std::unordered_map<String, String> files TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};

}
