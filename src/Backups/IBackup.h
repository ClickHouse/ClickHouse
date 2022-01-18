#pragma once

#include <Core/Types.h>
#include <memory>


namespace DB
{
class IBackupEntry;
using BackupEntryPtr = std::unique_ptr<IBackupEntry>;

/// Represents a backup, i.e. a storage of BackupEntries which can be accessed by their names.
/// A backup can be either incremental or non-incremental. An incremental backup doesn't store
/// the data of the entries which are not changed compared to its base backup.
class IBackup
{
public:
    virtual ~IBackup() = default;

    enum class OpenMode
    {
        READ,
        WRITE,
    };

    /// A backup can be open either in READ or WRITE mode.
    virtual OpenMode getOpenMode() const = 0;

    /// Returns the path to the backup.
    virtual String getPath() const = 0;

    /// Returns names of entries stored in the backup.
    /// If `prefix` isn't empty the function will return only the names starting with
    /// the prefix (but without the prefix itself).
    /// If the `terminator` isn't empty the function will returns only parts of the names
    /// before the terminator. For example, list("", "") returns names of all the entries
    /// in the backup; and list("data/", "/") return kind of a list of folders and
    /// files stored in the "data/" directory inside the backup.
    virtual Strings list(const String & prefix = "", const String & terminator = "/") const = 0;

    /// Checks if an entry with a specified name exists.
    virtual bool exists(const String & name) const = 0;

    /// Returns the size of the entry's data.
    /// This function does the same as `read(name)->getSize()` but faster.
    virtual size_t getSize(const String & name) const = 0;

    /// Returns the checksum of the entry's data.
    /// This function does the same as `read(name)->getCheckum()` but faster.
    virtual UInt128 getChecksum(const String & name) const = 0;

    /// Reads an entry from the backup.
    virtual BackupEntryPtr read(const String & name) const = 0;

    /// Puts a new entry to the backup.
    virtual void write(const String & name, BackupEntryPtr entry) = 0;

    /// Finalizes writing the backup, should be called after all entries have been successfully written.
    virtual void finalizeWriting() = 0;
};

using BackupPtr = std::shared_ptr<const IBackup>;
using BackupMutablePtr = std::shared_ptr<IBackup>;

}
