#pragma once

#include <Backups/IBackupCoordination.h>
#include <Common/ZooKeeper/Common.h>
#include <map>
#include <unordered_map>


namespace DB
{

/// Helper designed to be used in an implementation of the IBackupCoordination interface in the part related to replicated tables.
class BackupCoordinationReplicatedPartNames
{
public:
    BackupCoordinationReplicatedPartNames();
    ~BackupCoordinationReplicatedPartNames();

    using PartNameAndChecksum = IBackupCoordination::PartNameAndChecksum;

    /// Adds part names which a specified replica of a replicated table is going to put to the backup.
    /// Multiple replicas of the replicated table call this function and then the added part names can be returned by call of the function
    /// getPartNames().
    /// Checksums are used only to control that parts under the same names on different replicas are the same.
    void addPartNames(
        const String & table_zk_path,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums);

    void preparePartNames();

    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addPartNames() but without duplications and without
    /// parts covered by another parts.
    Strings getPartNames(const String & table_zk_path, const String & replica_name) const;

private:
    class CoveredPartsFinder;

    struct PartReplicas
    {
        std::vector<std::shared_ptr<const String>> replica_names;
        UInt128 checksum;
    };

    struct TableInfo
    {
        std::map<String /* part_name */, PartReplicas> parts_replicas; /// Should be ordered because we need this map to be in the same order on every replica.
        std::unordered_map<String /* replica_name> */, Strings> replicas_parts;
        std::unique_ptr<CoveredPartsFinder> covered_parts_finder;
    };

    std::map<String /* table_zk_path */, TableInfo> table_infos; /// Should be ordered because we need this map to be in the same order on every replica.
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
