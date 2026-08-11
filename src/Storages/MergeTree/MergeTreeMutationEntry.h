#pragma once

#include <base/types.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/PartitionIds.h>
#include <Storages/MutationCommands.h>
#include <Common/TransactionID.h>


namespace DB
{
class IBackupEntry;
class StorageMergeTree;

/// A mutation entry for non-replicated MergeTree storage engines.
/// Stores information about mutation in file mutation_*.txt.
struct MergeTreeMutationEntry
{
    time_t create_time = 0;
    std::shared_ptr<MutationCommands> commands;

    DiskPtr disk;
    String path_prefix;
    String file_name;
    bool is_temp = false;

    /// True once this in-memory entry is the canonical owner of the on-disk
    /// `mutation_*.txt` file. Set when the entry is registered in
    /// `current_mutations_by_version` or loaded from disk by `loadMutations`.
    /// If this stays `false` at destruction (e.g. `commit` succeeded but the
    /// caller threw before registering, see `StorageMergeTree::prepareMutationEntry`
    /// and `StorageMergeTree::addPreparedMutationEntry`), the destructor
    /// removes the orphaned file so `loadMutations` cannot replay a stale
    /// mutation against rolled-back metadata. See #80648.
    bool is_registered = false;

    /// This flag is set periodically in a background thread.
    /// If it is true, then mutation is done. If it is false,
    /// then mutation may be already done but not processed by this thread.
    bool is_done = false;

    UInt64 block_number = 0;

    String latest_failed_part;
    MergeTreePartInfo latest_failed_part_info;
    time_t latest_fail_time = 0;
    String latest_fail_reason;
    String latest_fail_error_code_name;

    /// If empty, applied to all partitions.
    PartitionIds partition_ids;

    /// True if the entry was loaded from a legacy `mutation_*.txt` file that has no
    /// persisted `partition ids:` line although some of its commands are partition-scoped.
    /// Such a file has to be upgraded to the current format after loading (see
    /// `upgradeFileWithResolvedPartitionScope`), otherwise every load resolves the
    /// `IN PARTITION` literal through the current table metadata again, which can throw
    /// after a safe partition key type change and make the table unloadable.
    bool needs_file_upgrade = false;

    /// ID of transaction which has created mutation.
    TransactionID tid = Tx::NonTransactionalTID;
    /// CSN of transaction which has created mutation
    /// or UnknownCSN if it's not committed (yet) or RolledBackCSN if it's rolled back or NonTransactionalCSN if there is no transaction.
    CSN csn = Tx::UnknownCSN;

    /// Create a new entry and write it to a temporary file.
    MergeTreeMutationEntry(MutationCommands commands_, DiskPtr disk, const String & path_prefix_, UInt64 tmp_number,
                           PartitionIds && partition_ids, const TransactionID & tid_, const WriteSettings & settings);
    MergeTreeMutationEntry(const MergeTreeMutationEntry &) = delete;
    /// Must clear the moved-from ownership token (`file_name`, `is_temp`,
    /// `is_registered`); a defaulted move leaves `file_name` unspecified (SSO
    /// often keeps it) so the source would destruct as a spurious owner and
    /// remove the file the destination just took over.
    MergeTreeMutationEntry(MergeTreeMutationEntry &&) noexcept;

    /// Commit entry and rename it to a permanent file.
    void commit(UInt64 block_number_);

    void removeFile();

    void writeCSN(CSN csn_);

    /// Rewrite a legacy on-disk file (see `needs_file_upgrade`) in the current format,
    /// with the resolved partition scope of the commands persisted next to them.
    void upgradeFileWithResolvedPartitionScope(const WriteSettings & settings);

    std::shared_ptr<const IBackupEntry> backup() const;

    bool affectsPartition(const String & partition_id) const
    {
        return containsInPartitionIdsOrEmpty(partition_ids, partition_id);
    }

    static String versionToFileName(UInt64 block_number_);
    static UInt64 tryParseFileName(const String & file_name_);
    static UInt64 parseFileName(const String & file_name_);

    /// Load an existing entry.
    MergeTreeMutationEntry(
        DiskPtr disk_,
        const String & path_prefix_,
        const String & file_name_,
        StorageMergeTree * storage_,
        ContextPtr context_);

    ~MergeTreeMutationEntry();
};

}
