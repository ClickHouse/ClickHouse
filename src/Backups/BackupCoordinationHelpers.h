#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/IRestoreCoordination.h>
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
        const String & table_shared_id,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums);

    /// Returns the names of the parts which a specified replica of a replicated table should put to the backup.
    /// This is the same list as it was added by call of the function addPartNames() but without duplications and without
    /// parts covered by another parts.
    Strings getPartNames(const String & table_shared_id, const String & replica_name) const;

private:
    void preparePartNames() const;

    class CoveredPartsFinder;

    struct PartReplicas
    {
        std::vector<std::shared_ptr<const String>> replica_names;
        UInt128 checksum;
    };

    struct TableInfo
    {
        std::map<String /* part_name */, PartReplicas> parts_replicas; /// Should be ordered because we need this map to be in the same order on every replica.
        mutable std::unordered_map<String /* replica_name> */, Strings> replicas_parts;
        std::unique_ptr<CoveredPartsFinder> covered_parts_finder;
    };

    std::map<String /* table_shared_id */, TableInfo> table_infos; /// Should be ordered because we need this map to be in the same order on every replica.
    mutable bool part_names_prepared = false;
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
