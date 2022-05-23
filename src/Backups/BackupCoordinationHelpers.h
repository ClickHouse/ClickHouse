#pragma once

#include <Backups/IBackupCoordination.h>
#include <Common/ZooKeeper/Common.h>
#include <Interpreters/StorageID.h>
#include <map>
#include <unordered_map>


namespace DB
{

/// A pair of host ID and storage ID.
struct BackupCoordinationHostIDAndStorageID
{
    String host_id;
    StorageID storage_id;

    BackupCoordinationHostIDAndStorageID(const String & host_id_, const StorageID & storage_id_);
    String serialize() const;
    static String serialize(const String & host_id_, const StorageID & storage_id_);
    static BackupCoordinationHostIDAndStorageID deserialize(const String & str);

    struct Less
    {
        bool operator()(const BackupCoordinationHostIDAndStorageID & lhs, const BackupCoordinationHostIDAndStorageID & rhs) const;
        bool operator()(const std::shared_ptr<const BackupCoordinationHostIDAndStorageID> & lhs, const std::shared_ptr<const BackupCoordinationHostIDAndStorageID> & rhs) const;
    };
};


/// Helper designed to be used in an implementation of the IBackupCoordination interface in the part related to replicated tables.
class BackupCoordinationReplicatedPartNames
{
public:
    BackupCoordinationReplicatedPartNames() = default;

    using PartNameAndChecksum = IBackupCoordination::PartNameAndChecksum;

    /// Adds part names which a specified replica of a replicated table is going to put to the backup.
    /// Multiple replicas of the replicated table call this function and then the added part names can be returned by call of the function
    /// getReplicatedTablePartNames().
    /// Checksums are used only to control that parts under the same names on different replicas are the same.
    void addPartNames(
        const String & host_id,
        const StorageID & table_id,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums,
        const String & table_zk_path);

    bool has(const String & host_id, const StorageID & table_id) const;

    /// Adds a data path in backup for a replicated table.
    /// Multiple replicas of the replicated table call this function and then all the added paths can be returned by call of the function
    /// getReplicatedTableDataPaths().
    void addDataPath(const String & host_id, const StorageID & table_id, const String & data_path);

    void preparePartNames();

    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addReplicatedTablePartNames() but without duplications and without
    /// parts covered by another parts.
    Strings getPartNames(const String & host_id, const StorageID & table_id) const;

    /// Returns all the data paths in backup added for a replicated table (see also addReplicatedTableDataPath()).
    Strings getDataPaths(const String & host_id, const StorageID & table_id) const;

private:
    class CoveredPartsFinder;

    struct PartLocations
    {
        std::vector<std::shared_ptr<const BackupCoordinationHostIDAndStorageID>> hosts_and_tables;
        UInt128 checksum;
    };

    struct TableInfo
    {
        std::map<String /* part_name */, PartLocations> part_names_with_locations; /// Should be ordered because we need this map to be in the same order on every replica.
        Strings data_paths;
    };

    struct ExtendedTableInfo
    {
        TableInfo * table_info;
        Strings part_names;
    };

    ExtendedTableInfo & getTableInfo(const String & host_id, const StorageID & table_id);
    const ExtendedTableInfo & getTableInfo(const String & host_id, const StorageID & table_id) const;

    std::map<String /* table_zk_path */, TableInfo> tables_by_zk_path; /// Should be ordered because we need this map to be in the same order on every replica.
    std::unordered_map<String /* host_id */, std::map<StorageID, ExtendedTableInfo>> tables;
    bool part_names_prepared = false;
};


/// Helper designed to be used in the implementation of the BackupCoordinationDistributed and RestoreCoordinationDistributed classes
/// to implement synchronization when we need all hosts to finish a specific task and then continue.
class BackupCoordinationDistributedBarrier
{
public:
    BackupCoordinationDistributedBarrier(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, const String & logger_name_, const String & operation_name_);

    /// Sets that a specified host has finished the specific task, successfully or with an error.
    /// In the latter case `error_message` should be set.
    void finish(const String & host_id, const String & error_message = {});

    /// Waits for a specified list of hosts to finish the specific task.
    void waitForAllHostsToFinish(const Strings & host_ids, const std::chrono::seconds timeout = std::chrono::seconds(-1) /* no timeout */) const;

private:
    void createRootNodes();

    String zookeeper_path;
    zkutil::GetZooKeeper get_zookeeper;
    const Poco::Logger * log;
    String operation_name;
};

}
