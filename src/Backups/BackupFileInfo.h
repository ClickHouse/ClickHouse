#pragma once

#include <Core/Types.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/Logger.h>

namespace DB
{

class IBackup;
class IBackupEntry;
using BackupPtr = std::shared_ptr<const IBackup>;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
struct ReadSettings;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;


/// Information about a file stored in a backup.
struct BackupFileInfo
{
    String file_name;

    UInt64 size = 0;
    UInt128 checksum{0};

    /// for incremental backups
    UInt64 base_size = 0;
    UInt128 base_checksum{0};

    /// Name of the data file. An empty string means there is no data file (that can happen if the file is empty or was taken from the base backup as a whole).
    /// This field is set during backup coordination (see the class BackupCoordinationFileInfos).
    String data_file_name;

    /// Index of the data file. -1 means there is no data file.
    /// This field is set during backup coordination (see the class BackupCoordinationFileInfos).
    size_t data_file_index = static_cast<size_t>(-1);

    /// Whether this file is encrypted by an encrypted disk.
    bool encrypted_by_disk = false;

    /// Set if this file is just a reference to another file
    String reference_target;

    /// (While writing a backup) if this list is not empty then after writing
    /// `data_file_name` it should be copied to this list of destinations too.
    /// This is used for plain backups.
    Strings data_file_copies;

    struct LessByFileName
    {
        bool operator()(const BackupFileInfo & lhs, const BackupFileInfo & rhs) const { return (lhs.file_name < rhs.file_name); }
        bool operator()(const BackupFileInfo * lhs, const BackupFileInfo * rhs) const { return (lhs->file_name < rhs->file_name); }
    };

    struct EqualByFileName
    {
        bool operator()(const BackupFileInfo & lhs, const BackupFileInfo & rhs) const { return (lhs.file_name == rhs.file_name); }
        bool operator()(const BackupFileInfo * lhs, const BackupFileInfo * rhs) const { return (lhs->file_name == rhs->file_name); }
    };

    struct LessBySizeOrChecksum
    {
        bool operator()(const BackupFileInfo & lhs, const BackupFileInfo & rhs) const
        {
            return (lhs.size < rhs.size) || (lhs.size == rhs.size && lhs.checksum < rhs.checksum);
        }
    };

    /// Note: this format doesn't allow to parse data back.
    /// Must be used only for debugging purposes.
    String describe() const;
};

using BackupFileInfos = std::vector<BackupFileInfo>;

/// Builds a BackupFileInfo for a specified backup entry.
BackupFileInfo buildFileInfoForBackupEntry(const String & file_name, const BackupEntryPtr & backup_entry, const BackupPtr & base_backup, const ReadSettings & read_settings, LoggerPtr log);

/// Builds a vector of BackupFileInfos for specified backup entries.
BackupFileInfos buildFileInfosForBackupEntries(const BackupEntries & backup_entries, const BackupPtr & base_backup, const ReadSettings & read_settings, ThreadPool & thread_pool, QueryStatusPtr process_list_element);

}
