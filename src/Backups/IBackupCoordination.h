#pragma once

#include <Core/Types.h>


namespace DB
{
class Exception;
struct BackupFileInfo;
using BackupFileInfos = std::vector<BackupFileInfo>;
enum class AccessEntityType : uint8_t;
enum class UserDefinedSQLObjectType : uint8_t;

/// Replicas use this class to coordinate what they're writing to a backup while executing BACKUP ON CLUSTER.
/// There are two implementation of this interface: BackupCoordinationLocal and BackupCoordinationRemote.
/// BackupCoordinationLocal is used while executing BACKUP without ON CLUSTER and performs coordination in memory.
/// BackupCoordinationRemote is used while executing BACKUP with ON CLUSTER and performs coordination via ZooKeeper.
class IBackupCoordination
{
public:
    virtual ~IBackupCoordination() = default;

    /// Sets the current stage and waits for other hosts to come to this stage too.
    virtual void setStage(const String & new_stage, const String & message) = 0;
    virtual void setError(const Exception & exception) = 0;
    virtual Strings waitForStage(const String & stage_to_wait) = 0;
    virtual Strings waitForStage(const String & stage_to_wait, std::chrono::milliseconds timeout) = 0;

    struct PartNameAndChecksum
    {
        String part_name;
        UInt128 checksum;
    };

    /// Adds part names which a specified replica of a replicated table is going to put to the backup.
    /// Multiple replicas of the replicated table call this function and then the added part names can be returned by call of the function
    /// getReplicatedPartNames().
    /// Checksums are used only to control that parts under the same names on different replicas are the same.
    virtual void addReplicatedPartNames(const String & table_zk_path, const String & table_name_for_logs, const String & replica_name,
                                        const std::vector<PartNameAndChecksum> & part_names_and_checksums) = 0;

    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addReplicatedPartNames() but without duplications and without
    /// parts covered by another parts.
    virtual Strings getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const = 0;

    struct MutationInfo
    {
        String id;
        String entry;
    };

    /// Adds information about mutations of a replicated table.
    virtual void addReplicatedMutations(const String & table_zk_path, const String & table_name_for_logs, const String & replica_name, const std::vector<MutationInfo> & mutations) = 0;

    /// Returns all mutations of a replicated table which are not finished for some data parts added by addReplicatedPartNames().
    virtual std::vector<MutationInfo> getReplicatedMutations(const String & table_zk_path, const String & replica_name) const = 0;

    /// Adds information about KeeperMap tables
    virtual void addKeeperMapTable(const String & table_zookeeper_root_path, const String & table_id, const String & data_path_in_backup) = 0;

    /// KeeperMap tables use shared storage without local data so only one table should backup the data
    virtual String getKeeperMapDataPath(const String & table_zookeeper_root_path) const = 0;

    /// Adds a data path in backup for a replicated table.
    /// Multiple replicas of the replicated table call this function and then all the added paths can be returned by call of the function
    /// getReplicatedDataPaths().
    virtual void addReplicatedDataPath(const String & table_zk_path, const String & data_path) = 0;

    /// Returns all the data paths in backup added for a replicated table (see also addReplicatedDataPath()).
    virtual Strings getReplicatedDataPaths(const String & table_zk_path) const = 0;

    /// Adds a path to access.txt file keeping access entities of a ReplicatedAccessStorage.
    virtual void addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & file_path) = 0;
    virtual Strings getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type) const = 0;

    /// Adds a path to a directory with user-defined SQL objects inside the backup.
    virtual void addReplicatedSQLObjectsDir(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & dir_path) = 0;
    virtual Strings getReplicatedSQLObjectsDirs(const String & loader_zk_path, UserDefinedSQLObjectType object_type) const = 0;

    /// Adds file information.
    /// If specified checksum+size are new for this IBackupContentsInfo the function sets `is_data_file_required`.
    virtual void addFileInfos(BackupFileInfos && file_infos) = 0;
    virtual BackupFileInfos getFileInfos() const = 0;
    virtual BackupFileInfos getFileInfosForAllHosts() const = 0;

    /// Starts writing a specified file, the function returns false if that file is already being written concurrently.
    virtual bool startWritingFile(size_t data_file_index) = 0;

    /// This function is used to check if concurrent backups are running
    /// other than the backup passed to the function
    virtual bool hasConcurrentBackups(const std::atomic<size_t> & num_active_backups) const = 0;
};

}
