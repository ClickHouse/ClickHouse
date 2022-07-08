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

    /// Returns names of entries stored in a specified directory in the backup.
    /// If `directory` is empty or '/' the functions returns entries in the backup's root.
    virtual Strings listFiles(const String & directory, bool recursive = false) const = 0;

    /// Checks if a specified directory contains any files.
    /// The function returns the same as `!listFiles(directory).empty()`.
    virtual bool hasFiles(const String & directory) const = 0;

    using SizeAndChecksum = std::pair<UInt64, UInt128>;

    /// Checks if an entry with a specified name exists.
    virtual bool fileExists(const String & file_name) const = 0;
    virtual bool fileExists(const SizeAndChecksum & size_and_checksum) const = 0;

    /// Returns the size of the entry's data.
    /// This function does the same as `read(file_name)->getSize()` but faster.
    virtual UInt64 getFileSize(const String & file_name) const = 0;

    /// Returns the checksum of the entry's data.
    /// This function does the same as `read(file_name)->getCheckum()` but faster.
    virtual UInt128 getFileChecksum(const String & file_name) const = 0;

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
