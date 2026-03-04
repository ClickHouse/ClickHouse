#pragma once

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
    std::vector<DataPartPtr> part_references;

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
