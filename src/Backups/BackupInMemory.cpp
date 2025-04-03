#include <Backups/BackupInMemory.h>

#include <Backups/BackupsInMemoryHolder.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_ENTRY_NOT_FOUND;
    extern const int BACKUP_ENTRY_ALREADY_EXISTS;
}


BackupInMemory::BackupInMemory(const String & backup_name_, BackupsInMemoryHolder & holder_)
    : backup_name(backup_name_), holder(holder_)
{
}


bool BackupInMemory::isEmpty() const
{
    std::lock_guard lock{mutex};
    return files.empty();
}


bool BackupInMemory::fileExists(const String & file_name) const
{
    std::lock_guard lock{mutex};
    return files.contains(file_name);
}


UInt64 BackupInMemory::getFileSize(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto it = files.find(file_name);
    if (it == files.end())
        throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found in backup {}", file_name, backup_name);
    return it->second.length();
}


void BackupInMemory::removeFile(const String & file_name)
{
    std::lock_guard lock{mutex};
    auto it = files.find(file_name);
    if (it == files.end())
        throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found in backup {}", file_name, backup_name);
    files.erase(it);
}


std::unique_ptr<ReadBufferFromFileBase> BackupInMemory::readFile(const String & file_name) const
{
    String file_contents;

    {
        std::lock_guard lock{mutex};
        auto it = files.find(file_name);
        if (it == files.end())
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found in backup {}", file_name, backup_name);
        file_contents = it->second;
    }

    return std::make_unique<ReadBufferFromOwnMemoryFile>(file_name, std::move(file_contents));
}


class BackupInMemory::WriteBufferToBackupInMemory : public WriteBufferFromOwnString
{
public:
    WriteBufferToBackupInMemory(std::shared_ptr<BackupInMemory> backup_, const String & file_name_)
        : backup(backup_), file_name(file_name_) {}

private:
    void finalizeImpl() override
    {
        String file_contents{stringView()};
        WriteBufferFromOwnString::finalizeImpl();
        std::lock_guard lock{backup->mutex};
        auto it = backup->files.find(file_name);
        if (it == backup->files.end())
            throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found in backup {}", file_name, backup->backup_name);
        it->second = std::move(file_contents);
    }

    std::shared_ptr<BackupInMemory> backup;
    String file_name;
};


std::unique_ptr<WriteBuffer> BackupInMemory::writeFile(const String & file_name)
{
    {
        std::lock_guard lock{mutex};
        auto it = files.find(file_name);
        if (it != files.end())
            throw Exception(ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Backup entry {} already exists in backup {}", file_name, backup_name);
        files.emplace(file_name, "");
    }

    return std::make_unique<AutoFinalizedWriteBuffer<WriteBufferToBackupInMemory>>(shared_from_this(), file_name);
}


void BackupInMemory::copyFile(const String & from, const String & to)
{
    std::lock_guard lock{mutex};
    auto it_from = files.find(from);
    if (it_from == files.end())
        throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found in backup {}", from, backup_name);
    auto it_to = files.find(to);
    if (it_to != files.end())
        throw Exception(ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Backup entry {} already exists in backup {}", to, backup_name);
    files.emplace(to, it_from->second);
}


void BackupInMemory::drop()
{
    holder.dropBackup(backup_name);
}

}
