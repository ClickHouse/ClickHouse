#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/IRestoreCoordination.h>
#include <Common/ZooKeeper/Common.h>
#include <map>
#include <unordered_map>


namespace DB
{
enum class AccessEntityType;


/// Helper designed to be used in an implementation of the IBackupCoordination interface in the part related to replicated tables.
class BackupCoordinationReplicatedPartsAndMutations
{
public:
    BackupCoordinationReplicatedPartsAndMutations();
    ~BackupCoordinationReplicatedPartsAndMutations();

    using PartNameAndChecksum = IBackupCoordination::PartNameAndChecksum;

    /// Adds part names which a specified replica of a replicated table is going to put to the backup.
    /// Multiple replicas of the replicated table call this function and then the added part names can be returned by call of the function
    /// getPartNames().
    /// Checksums are used only to control that parts under the same names on different replicas are the same.
    void addPartNames(
        const String & table_shared_id,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums);

    using MutationInfo = IBackupCoordination::MutationInfo;

    /// Adds information about mutations of a replicated table.
    void addMutations(
        const String & table_shared_id,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<MutationInfo> & mutations);
        
    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addPartNames() but without duplications and without
    /// parts covered by another parts.
    Strings getPartNames(const String & table_shared_id, const String & replica_name) const;

    /// Returns all mutations of a replicated table which are not finished for some data parts added by addReplicatedPartNames(). 
    std::vector<MutationInfo> getMutations(const String & table_shared_id, const String & replica_name) const;

private:
    void prepare() const;

    class CoveredPartsFinder;

    struct PartReplicas
    {
        std::vector<std::shared_ptr<const String>> replica_names;
        UInt128 checksum;
    };

    struct TableInfo
    {
        String table_name_for_logs;
        std::map<String /* part_name */, PartReplicas> replicas_by_part_name; /// Should be ordered because we need this map to be in the same order on every replica.
        mutable std::unordered_map<String /* replica_name> */, Strings> part_names_by_replica_name;
        std::unique_ptr<CoveredPartsFinder> covered_parts_finder;
        mutable std::unordered_map<String, Int64> min_data_versions_by_partition;
        mutable std::unordered_map<String, String> mutations;
        String replica_name_to_store_mutations;
    };

    std::map<String /* table_shared_id */, TableInfo> table_infos; /// Should be ordered because we need this map to be in the same order on every replica.
    mutable bool prepared = false;
};


/// Helper designed to be used in an implementation of the IBackupCoordination interface in the part related to replicated access storages.
class BackupCoordinationReplicatedAccess
{
public:
    BackupCoordinationReplicatedAccess();
    ~BackupCoordinationReplicatedAccess();

    /// Adds a path to access.txt file keeping access entities of a ReplicatedAccessStorage.
    void addFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path);
    Strings getFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const;

private:
    using ZkPathAndEntityType = std::pair<String, AccessEntityType>;

    struct FilePathsAndHost
    {
        std::unordered_set<String> file_paths;
        String host_to_store_access;
    };

    std::map<ZkPathAndEntityType, FilePathsAndHost> file_paths_by_zk_path;
};


/// Helps to wait until all hosts come to a specified stage.
class BackupCoordinationStatusSync
{
public:
    BackupCoordinationStatusSync(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, Poco::Logger * log_);

    void set(const String & current_host, const String & new_status, const String & message);
    Strings setAndWait(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts);
    Strings setAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, UInt64 timeout_ms);

    static constexpr const char * kErrorStatus = "error";

private:
    void createRootNodes();
    Strings setImpl(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, const std::optional<UInt64> & timeout_ms);

    String zookeeper_path;
    zkutil::GetZooKeeper get_zookeeper;
    Poco::Logger * log;
};

}
