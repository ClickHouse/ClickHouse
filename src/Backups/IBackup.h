#pragma once

#include <Core/Types.h>
#include <memory>
#include <optional>


namespace DB
{
class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;

/// Represents a backup, i.e. a storage of BackupEntries which can be accessed by their names.
/// A backup can be either incremental or non-incremental. An incremental backup doesn't store
/// the data of the entries which are not changed compared to its base backup.
class IBackup : public std::enable_shared_from_this<IBackup>
{
public:
    virtual ~IBackup() = default;

    /// Name of the backup.
    virtual const String & getName() const = 0;

    enum class OpenMode
    {
        READ,
        WRITE,
    };

    /// Returns whether the backup was opened for reading or writing.
    virtual OpenMode getOpenMode() const = 0;

    /// Returns the time point when this backup was created.
    virtual time_t getTimestamp() const = 0;

    /// Returns UUID of the backup.
    virtual UUID getUUID() const = 0;

    /// Returns names of entries stored in the backup.
    /// If `prefix` isn't empty the function will return only the names starting with
    /// the prefix (but without the prefix itself).
    /// If the `terminator` isn't empty the function will returns only parts of the names
    /// before the terminator. For example, list("", "") returns names of all the entries
    /// in the backup; and list("data/", "/") return kind of a list of folders and
    /// files stored in the "data/" directory inside the backup.
    virtual Strings listFiles(const String & prefix = "", const String & terminator = "/") const = 0; /// NOLINT

    /// Checks if an entry with a specified name exists.
    virtual bool fileExists(const String & file_name) const = 0;
    virtual bool fileExists(const std::pair<UInt64, UInt128> & size_and_checksum) const = 0;

    /// Returns the size of the entry's data.
    /// This function does the same as `read(file_name)->getSize()` but faster.
    virtual UInt64 getFileSize(const String & file_name) const = 0;

    /// Returns the checksum of the entry's data.
    /// This function does the same as `read(file_name)->getCheckum()` but faster.
    virtual UInt128 getFileChecksum(const String & file_name) const = 0;

    using SizeAndChecksum = std::pair<UInt64, UInt128>;

    /// Returns both the size and checksum in one call.
    virtual SizeAndChecksum getFileSizeAndChecksum(const String & file_name) const = 0;

    /// Reads an entry from the backup.
    virtual BackupEntryPtr readFile(const String & file_name) const = 0;
    virtual BackupEntryPtr readFile(const SizeAndChecksum & size_and_checksum) const = 0;

    /// Puts a new entry to the backup.
    virtual void writeFile(const String & file_name, BackupEntryPtr entry) = 0;

    /// Finalizes writing the backup, should be called after all entries have been successfully written.
    virtual void finalizeWriting() = 0;

    /// Whether it's possible to add new entries to the backup in multiple threads.
    virtual bool supportsWritingInMultipleThreads() const = 0;
};

using BackupPtr = std::shared_ptr<const IBackup>;
using BackupMutablePtr = std::shared_ptr<IBackup>;

}
