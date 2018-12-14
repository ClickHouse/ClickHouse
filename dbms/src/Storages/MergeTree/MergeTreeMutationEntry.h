#pragma once

#include <Core/Types.h>
#include <Storages/MutationCommands.h>


namespace DB
{

/// A mutation entry for non-replicated MergeTree storage engines.
struct MergeTreeMutationEntry
{
    time_t create_time = 0;
    MutationCommands commands;

    String path_prefix;
    String file_name;
    bool is_temp = false;

    Int64 block_number = 0;

    /// Create a new entry and write it to a temporary file.
    MergeTreeMutationEntry(MutationCommands commands_, const String & path_prefix_, Int64 tmp_number);
    MergeTreeMutationEntry(const MergeTreeMutationEntry &) = delete;
    MergeTreeMutationEntry(MergeTreeMutationEntry &&) = default;

    /// Commit entry and rename it to a permanent file.
    void commit(Int64 block_number_);

    void removeFile();

    /// Load an existing entry.
    MergeTreeMutationEntry(const String & path_prefix_, const String & file_name_);

    ~MergeTreeMutationEntry();
};

}
