#pragma once

#include <Core/Types.h>
#include <optional>


namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;

/// Keeps information about files contained in a backup.
class IBackupCoordination
{
public:
    virtual ~IBackupCoordination() = default;

    /// Adds a data path in backup for a replicated table.
    /// Multiple replicas of the replicated table call this function and then all the added paths can be returned by call of the function
    /// getReplicatedTableDataPaths().
    virtual void addReplicatedTableDataPath(const String & table_zk_path, const String & table_data_path) = 0;

    struct PartNameAndChecksum
    {
        String part_name;
        UInt128 checksum;
    };

    /// Adds part names which a specified replica of a replicated table is going to put to the backup.
    /// Multiple replicas of the replicated table call this function and then the added part names can be returned by call of the function
    /// getReplicatedTablePartNames().
    /// Checksums are used only to control that parts under the same names on different replicas are the same.
    virtual void addReplicatedTablePartNames(
        const String & host_id,
        const DatabaseAndTableName & table_name,
        const String & table_zk_path,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums)
        = 0;

    /// Sets that a specified host finished preparations for copying the backup's files, successfully or not.
    /// `error_message` should be set to true if it was not successful.
    virtual void finishPreparing(const String & host_id, const String & error_message = {}) = 0;

    /// Waits for a specified time for specified hosts to finish preparation for copying the backup's files.
    virtual void
    waitForAllHostsPrepared(const Strings & host_ids, std::chrono::seconds timeout = std::chrono::seconds(-1) /* no timeout */) const = 0;

    /// Returns all the data paths in backup added for a replicated table (see also addReplicatedTableDataPath()).
    virtual Strings getReplicatedTableDataPaths(const String & table_zk_path) const = 0;

    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addReplicatedTablePartNames() but without duplications and without
    /// parts covered by another parts.
    virtual Strings getReplicatedTablePartNames(const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path) const = 0;

    struct FileInfo
    {
        String file_name;

        UInt64 size = 0;
        UInt128 checksum{0};

        /// for incremental backups
        UInt64 base_size = 0;
        UInt128 base_checksum{0};

        /// Name of the data file.
        String data_file_name;

        /// Suffix of an archive if the backup is stored as a series of archives.
        String archive_suffix;

        /// Position in the archive.
        UInt64 pos_in_archive = static_cast<UInt64>(-1);
    };

    /// Adds file information.
    /// If specified checksum+size are new for this IBackupContentsInfo the function sets `is_data_file_required`.
    virtual void addFileInfo(const FileInfo & file_info, bool & is_data_file_required) = 0;

    void addFileInfo(const FileInfo & file_info)
    {
        bool is_data_file_required;
        addFileInfo(file_info, is_data_file_required);
    }

    /// Updates some fields (currently only `archive_suffix`) of a stored file's information.
    virtual void updateFileInfo(const FileInfo & file_info) = 0;

    virtual std::vector<FileInfo> getAllFileInfos() const = 0;
    virtual Strings listFiles(const String & prefix, const String & terminator) const = 0;

    using SizeAndChecksum = std::pair<UInt64, UInt128>;

    virtual std::optional<FileInfo> getFileInfo(const String & file_name) const = 0;
    virtual std::optional<FileInfo> getFileInfo(const SizeAndChecksum & size_and_checksum) const = 0;
    virtual std::optional<SizeAndChecksum> getFileSizeAndChecksum(const String & file_name) const = 0;

    /// Generates a new archive suffix, e.g. "001", "002", "003", ...
    virtual String getNextArchiveSuffix() = 0;

    /// Returns the list of all the archive suffixes which were generated.
    virtual Strings getAllArchiveSuffixes() const = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
