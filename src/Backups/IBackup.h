#pragma once

#include <Core/Types.h>
#include <Disks/WriteMode.h>
#include <IO/WriteSettings.h>
#include <memory>
#include <optional>


namespace DB
{
class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
struct BackupFileInfo;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class SeekableReadBuffer;

/// Represents a backup, i.e. a storage of BackupEntries which can be accessed by their names.
/// A backup can be either incremental or non-incremental. An incremental backup doesn't store
/// the data of the entries which are not changed compared to its base backup.
class IBackup : public std::enable_shared_from_this<IBackup>
{
public:
    virtual ~IBackup() = default;

    /// Name of the backup.
    //virtual const String & getName() const = 0;
    virtual const String & getNameForLogging() const = 0;

    enum class OpenMode : uint8_t
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

    /// Returns the base backup or null if there is no base backup.
    virtual std::shared_ptr<const IBackup> getBaseBackup() const = 0;

    /// Returns the number of files stored in the backup. Compare with getNumEntries().
    virtual size_t getNumFiles() const = 0;

    /// Returns the total size of files stored in the backup. Compare with getTotalSizeOfEntries().
    virtual UInt64 getTotalSize() const  = 0;

    /// Returns the number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder or
    /// the number of files inside the archive if the backup is stored as an archive.
    /// It's not the same as getNumFiles() if it's an incremental backups or if it contains empty files or duplicates.
    /// The following is always true: `getNumEntries() <= getNumFiles()`.
    virtual size_t getNumEntries() const = 0;

    /// Returns the size of entries in the backup, i.e. the total size of files inside the folder if the backup is stored as a folder or
    /// the total size of files inside the archive if the backup is stored as an archive.
    /// It's not the same as getTotalSize() because it doesn't include the size of duplicates and the size of files from the base backup.
    /// The following is always true: `getSizeOfEntries() <= getTotalSize()`.
    virtual UInt64 getSizeOfEntries() const = 0;

    /// Returns the uncompressed size of the backup. It equals to `getSizeOfEntries() + size_of_backup_metadata (.backup)`
    virtual UInt64 getUncompressedSize() const = 0;

    /// Returns the compressed size of the backup. If the backup is not stored as an archive it's the same as getUncompressedSize().
    virtual UInt64 getCompressedSize() const = 0;

    /// Returns the number of files read during RESTORE from this backup.
    /// The following is always true: `getNumFilesRead() <= getNumFiles()`.
    virtual size_t getNumReadFiles() const  = 0;

    // Returns the total size of files read during RESTORE from this backup.
    /// The following is always true: `getNumReadBytes() <= getTotalSize()`.
    virtual UInt64 getNumReadBytes() const = 0;

    /// Returns names of entries stored in a specified directory in the backup.
    /// If `directory` is empty or '/' the functions returns entries in the backup's root.
    virtual Strings listFiles(const String & directory, bool recursive) const = 0;

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
    virtual std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) const = 0;
    virtual std::unique_ptr<SeekableReadBuffer> readFile(const SizeAndChecksum & size_and_checksum) const = 0;

    /// Copies a file from the backup to a specified destination disk. Returns the number of bytes written.
    virtual size_t copyFileToDisk(const String & file_name, DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) const = 0;

    virtual size_t copyFileToDisk(const SizeAndChecksum & size_and_checksum, DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) const = 0;

    /// Puts a new entry to the backup.
    virtual void writeFile(const BackupFileInfo & file_info, BackupEntryPtr entry) = 0;

    /// Whether it's possible to add new entries to the backup in multiple threads.
    virtual bool supportsWritingInMultipleThreads() const = 0;

    /// Finalizes writing the backup, should be called after all entries have been successfully written.
    virtual void finalizeWriting() = 0;

    /// Sets that a non-retriable error happened while the backup was being written which means that
    /// the backup is most likely corrupted and it can't be finalized.
    /// This function is called while handling an exception or if the backup was cancelled.
    virtual bool setIsCorrupted() noexcept = 0;

    /// Try to remove all files copied to the backup. Could be used after setIsCorrupted().
    virtual bool tryRemoveAllFiles() noexcept = 0;
};

using BackupPtr = std::shared_ptr<const IBackup>;
using BackupMutablePtr = std::shared_ptr<IBackup>;

}
