#pragma once

#include <Backups/IBackupCoordination.h>
#include <Common/ZooKeeper/Common.h>
#include <map>
#include <unordered_map>


namespace DB
{

/// Helper designed to be used in an implementation of the IBackupCoordination interface in the part related to replicated tables.
class BackupCoordinationReplicatedTablesInfo
{
public:
    BackupCoordinationReplicatedTablesInfo() = default;

    /// Adds a data path in backup for a replicated table.
    /// Multiple replicas of the replicated table call this function and then all the added paths can be returned by call of the function
    /// getReplicatedTableDataPaths().
    void addDataPath(const String & table_zk_path, const String & table_data_path);

    /// Returns all the data paths in backup added for a replicated table (see also addReplicatedTableDataPath()).
    Strings getDataPaths(const String & table_zk_path) const;

    using PartNameAndChecksum = IBackupCoordination::PartNameAndChecksum;

    /// Adds part names which a specified replica of a replicated table is going to put to the backup.
    /// Multiple replicas of the replicated table call this function and then the added part names can be returned by call of the function
    /// getReplicatedTablePartNames().
    /// Checksums are used only to control that parts under the same names on different replicas are the same.
    void addPartNames(
        const String & host_id,
        const DatabaseAndTableName & table_name,
        const String & table_zk_path,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums);

    void preparePartNamesByLocations();

    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addReplicatedTablePartNames() but without duplications and without
    /// parts covered by another parts.
    Strings getPartNames(const String & host_id, const DatabaseAndTableName & table_name, const String & table_zk_path) const;

private:
    class CoveredPartsFinder;
    struct HostAndTableName;

    struct PartLocations
    {
        std::vector<std::shared_ptr<const HostAndTableName>> host_and_table_names;
        UInt128 checksum;
    };

    struct TableInfo
    {
        Strings data_paths;
        std::map<String /* part_name */, PartLocations> part_locations_by_names; /// Should be ordered because we need this map to be in the same order on every replica.
        std::unordered_map<String /* host_id */, std::map<DatabaseAndTableName, Strings /* part_names */>> part_names_by_locations;
    };

    std::unordered_map<String /* zk_path */, TableInfo> tables;
    bool part_names_by_locations_prepared = false;
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
