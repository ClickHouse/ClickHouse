#pragma once

#include <Backups/IBackupCoordination.h>
#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

/// Replicas used this class to coordinate how they're writing replicated tables to a backup.
/// "BACKUP ON CLUSTER" can be executed on multiple hosts and parts of replicated tables on those hosts could be slightly different
/// at any specific moment. This class is designed so that inside the backup all replicas would contain all the parts
/// no matter if the replication queues of those tables are fast or slow.
/// This is important to make RESTORE more correct and not dependent on random things like how fast the replicas doing RESTORE
/// comparing to each other or how many replicas will be when RESTORE will be executed.
///
/// Example 1: Let's consider two replicas of a table, and let the first replica contain part all_1_1_0 and the second replica contain
/// all_2_2_0. The files in the backup will look like this:
/// /shards/1/replicas/1/data/mydb/mytable/all_1_1_0
/// /shards/1/replicas/1/data/mydb/mytable/all_2_2_0
/// /shards/1/replicas/2/data/mydb/mytable/all_1_1_0
/// /shards/1/replicas/2/data/mydb/mytable/all_2_2_0
///
/// Example 2: Let's consider two replicas again, and let the first replica contain parts all_1_1_0 and all_2_2_0 and
/// the second replica contain part all_1_2_1 (i.e. the second replica have those parts merged).
/// In this case the files in the backup will look like this:
/// /shards/1/replicas/1/data/mydb/mytable/all_1_2_1
/// /shards/1/replicas/2/data/mydb/mytable/all_1_2_1

class BackupCoordinationReplicatedTables
{
public:
    BackupCoordinationReplicatedTables();
    ~BackupCoordinationReplicatedTables();

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

    using MutationInfo = IBackupCoordination::MutationInfo;

    /// Adds information about mutations of a replicated table.
    void addMutations(
        const String & table_shared_id,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<MutationInfo> & mutations);

    /// Returns all mutations of a replicated table which are not finished for some data parts added by addReplicatedPartNames().
    std::vector<MutationInfo> getMutations(const String & table_shared_id, const String & replica_name) const;

    /// Adds a data path in backup for a replicated table.
    /// Multiple replicas of the replicated table call this function and then all the added paths can be returned by call of the function
    /// getDataPaths().
    void addDataPath(const String & table_shared_id, const String & data_path);

    /// Returns all the data paths in backup added for a replicated table (see also addReplicatedDataPath()).
    Strings getDataPaths(const String & table_shared_id) const;

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
        std::unordered_set<String> data_paths;
    };

    std::map<String /* table_shared_id */, TableInfo> table_infos; /// Should be ordered because we need this map to be in the same order on every replica.
    mutable bool prepared = false;
};

}
