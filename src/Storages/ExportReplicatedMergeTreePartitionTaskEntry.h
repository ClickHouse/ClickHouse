#pragma once

#include <map>
#include <optional>
#include <Storages/ExportReplicatedMergeTreePartitionManifest.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include "Core/QualifiedTableName.h"
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/mem_fun.hpp>

namespace DB
{
struct ExportReplicatedMergeTreePartitionTaskEntry
{
    using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
    ExportReplicatedMergeTreePartitionManifest manifest;

    enum class Status
    {
        PENDING,
        COMPLETED,
        FAILED,
        KILLED
    };

    /// Allows us to skip completed / failed entries during scheduling
    mutable Status status;

    /// References to the parts that should be exported
    /// This is used to prevent the parts from being deleted before finishing the export operation
    /// It does not mean this replica will export all the parts
    /// There is also a chance this replica does not contain a given part and it is totally ok.
    mutable std::vector<DataPartPtr> part_references;

    /// In-memory mirror of <export-entry>/last_exception/<replica> leaves in ZK,
    /// keyed by replica name (verbatim, not escaped). Refreshed on every poll() cycle
    /// and on every status-change handler invocation; served verbatim to
    /// system.replicated_partition_exports without any extra ZK read.
    /// An empty map means no replica has recorded an exception yet for this task.
    mutable std::map<String, LastExceptionEntry> last_exception_per_replica;

    /// In-memory mirror of <export-entry>/processed/<part> leaves in ZK, keyed by
    /// part name. Each value is the list of destination file paths produced by the
    /// per-part export (typically Parquet object-storage keys). Refreshed on every
    /// poll() cycle and on status-change handler invocations; served verbatim to
    /// system.replicated_partition_exports without any extra ZK read at query time.
    /// An empty map means no part has finished exporting yet for this task.
    /// Incomplete Keeper refreshes (or unreadable processed leaves) publish
    /// "<failed to read from zk>" as a whole-map key, or as the sole path value
    /// for the affected part leaf.
    mutable std::map<String, std::vector<String>> destination_file_paths_per_part;

    /// In-memory mirror of the <export-entry>/commit_info znode (written atomically
    /// with the COMPLETED status transition; see ExportPartitionUtils::commit).
    /// nullopt until commit_info is observed in ZK. Empty fields inside the struct
    /// for non-Iceberg destinations.
    mutable std::optional<ExportReplicatedMergeTreePartitionCommitInfoEntry> commit_info;

    std::string getCompositeKey() const
    {
        const auto qualified_table_name = QualifiedTableName {manifest.destination_database, manifest.destination_table};
        return manifest.partition_id + "_" + qualified_table_name.getFullName();
    }

    std::string getTransactionId() const
    {
        return manifest.transaction_id;
    }

    /// Get create_time for sorted iteration
    time_t getCreateTime() const
    {
        return manifest.create_time;
    }
};

struct ExportPartitionTaskEntryTagByCompositeKey {};
struct ExportPartitionTaskEntryTagByCreateTime {};
struct ExportPartitionTaskEntryTagByTransactionId {};

// Multi-index container for export partition task entries
// - Index 0 (TagByCompositeKey): hashed_unique on composite key for O(1) lookup
// - Index 1 (TagByCreateTime): ordered_non_unique on create_time for sorted iteration
using ExportPartitionTaskEntriesContainer = boost::multi_index_container<
    ExportReplicatedMergeTreePartitionTaskEntry,
    boost::multi_index::indexed_by<
        boost::multi_index::hashed_unique<
            boost::multi_index::tag<ExportPartitionTaskEntryTagByCompositeKey>,
            boost::multi_index::const_mem_fun<ExportReplicatedMergeTreePartitionTaskEntry, std::string, &ExportReplicatedMergeTreePartitionTaskEntry::getCompositeKey>
        >,
        boost::multi_index::ordered_non_unique<
            boost::multi_index::tag<ExportPartitionTaskEntryTagByCreateTime>,
            boost::multi_index::const_mem_fun<ExportReplicatedMergeTreePartitionTaskEntry, time_t, &ExportReplicatedMergeTreePartitionTaskEntry::getCreateTime>
        >,
        boost::multi_index::hashed_unique<
            boost::multi_index::tag<ExportPartitionTaskEntryTagByTransactionId>,
            boost::multi_index::const_mem_fun<ExportReplicatedMergeTreePartitionTaskEntry, std::string, &ExportReplicatedMergeTreePartitionTaskEntry::getTransactionId>
        >
    >
>;

}
