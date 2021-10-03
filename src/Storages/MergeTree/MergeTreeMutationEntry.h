#pragma once

#include <base/types.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MutationCommands.h>


namespace DB
{

/// A mutation entry for non-replicated MergeTree storage engines.
/// Stores information about mutation in file mutation_*.txt.
struct MergeTreeMutationEntry
{
    time_t create_time = 0;
    MutationCommands commands;

    DiskPtr disk;
    String path_prefix;
    String file_name;
    bool is_temp = false;

    Int64 block_number = 0;

    String latest_failed_part;
    MergeTreePartInfo latest_failed_part_info;
    time_t latest_fail_time = 0;
    String latest_fail_reason;

    /// Create a new entry and write it to a temporary file.
    MergeTreeMutationEntry(MutationCommands commands_, DiskPtr disk, const String & path_prefix_, Int64 tmp_number);
    MergeTreeMutationEntry(const MergeTreeMutationEntry &) = delete;
    MergeTreeMutationEntry(MergeTreeMutationEntry &&) = default;

    /// Commit entry and rename it to a permanent file.
    void commit(Int64 block_number_);

    void removeFile();

    /// Load an existing entry.
    MergeTreeMutationEntry(DiskPtr disk_, const String & path_prefix_, const String & file_name_);

    ~MergeTreeMutationEntry();
};

}
