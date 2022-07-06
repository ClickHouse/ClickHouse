#pragma once

#include <Core/Types.h>
#include <optional>


namespace DB
{
enum class AccessEntityType;

/// Replicas use this class to coordinate what they're writing to a backup while executing BACKUP ON CLUSTER.
/// There are two implementation of this interface: BackupCoordinationLocal and BackupCoordinationRemote.
/// BackupCoordinationLocal is used while executing BACKUP without ON CLUSTER and performs coordination in memory.
/// BackupCoordinationRemote is used while executing BACKUP with ON CLUSTER and performs coordination via ZooKeeper.
class IBackupCoordination
{
public:
    virtual ~IBackupCoordination() = default;

    /// Sets the current status and waits for other hosts to come to this status too. If status starts with "error:" it'll stop waiting on all the hosts.
    virtual void setStatus(const String & current_host, const String & new_status, const String & message) = 0;
    virtual Strings setStatusAndWait(const String & current_host, const String & new_status, const String & message, const Strings & other_hosts) = 0;
    virtual Strings setStatusAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & other_hosts, UInt64 timeout_ms) = 0;

    static constexpr const char * kErrorStatus = "error";

    struct PartNameAndChecksum
    {
        String part_name;
        UInt128 checksum;
    };

    /// Adds part names which a specified replica of a replicated table is going to put to the backup.
    /// Multiple replicas of the replicated table call this function and then the added part names can be returned by call of the function
    /// getReplicatedPartNames().
    /// Checksums are used only to control that parts under the same names on different replicas are the same.
    virtual void addReplicatedPartNames(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name,
                                        const std::vector<PartNameAndChecksum> & part_names_and_checksums) = 0;

    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addReplicatedPartNames() but without duplications and without
    /// parts covered by another parts.
    virtual Strings getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const = 0;

    struct MutationInfo
    {
        String id;
        String entry;
    };

    /// Adds information about mutations of a replicated table.
    virtual void addReplicatedMutations(const String & table_shared_id, const String & table_name_for_logs, const String & replica_name, const std::vector<MutationInfo> & mutations) = 0;

    /// Returns all mutations of a replicated table which are not finished for some data parts added by addReplicatedPartNames().
    virtual std::vector<MutationInfo> getReplicatedMutations(const String & table_shared_id, const String & replica_name) const = 0;

    /// Adds a data path in backup for a replicated table.
    /// Multiple replicas of the replicated table call this function and then all the added paths can be returned by call of the function
    /// getReplicatedDataPaths().
    virtual void addReplicatedDataPath(const String & table_shared_id, const String & data_path) = 0;

    /// Returns all the data paths in backup added for a replicated table (see also addReplicatedDataPath()).
    virtual Strings getReplicatedDataPaths(const String & table_shared_id) const = 0;

    /// Adds a path to access.txt file keeping access entities of a ReplicatedAccessStorage.
    virtual void addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path) = 0;
    virtual Strings getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const = 0;

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
    virtual Strings listFiles(const String & directory, bool recursive) const = 0;
    virtual bool hasFiles(const String & directory) const = 0;

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
