#pragma once

#include <string_view>
#include <common/types.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MutationCommands.h>

namespace DB
{
enum class MutationType { Ordinary, Lightweight };

/**
 * A mutation entry for non-replicated MergeTree storage engines.
 *
 * Stores information about mutation in:
 *   - mutation_N.txt (if type is MutationType::Ordinary) or in
 *   - lwmutation_N.txt (if type is MutationType::Lightweight),
 * where N is block_number.
 *
 * On creation, writes mutation info to tmp_[lw]mutation_N.txt file until commit() is called.
 */
struct MergeTreeMutationEntry
{
    time_t create_time;
    MutationCommands commands;

    DiskPtr disk;
    String path_prefix;
    String file_name;
    bool is_temp;

    MutationType type;

    Int64 block_number = 0;

    String latest_failed_part;
    MergeTreePartInfo latest_failed_part_info;
    time_t latest_fail_time = 0;
    String latest_fail_reason;

    /// Create a new entry and write it to a temporary file.
    MergeTreeMutationEntry(DiskPtr disk_, std::string_view path_prefix_,
        MutationType type_, MutationCommands commands_, Int64 tmp_number);

    /// Load an existing entry from disk.
    MergeTreeMutationEntry(DiskPtr disk_, std::string_view path_prefix_, std::string_view file_name_);

    MergeTreeMutationEntry(const MergeTreeMutationEntry &) = delete;
    MergeTreeMutationEntry(MergeTreeMutationEntry &&) = default;

    /// Commit entry and rename it to a permanent file.
    void commit(Int64 block_number_);

    void removeFile();

    ~MergeTreeMutationEntry();
};
}
