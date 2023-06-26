#pragma once

#include <Storages/MutationCommands.h>


namespace DB
{
class IBackup;
class ReadBuffer;
class WriteBuffer;

/// Information about a mutation from a backup.
struct MutationInfoFromBackup
{
    /// Name of the mutation.
    /// For MergeTree this is the name of a corresponding file looking like "mutation_*.txt"
    /// For ReplicatedMergeTree this is the name of a corresponding node in ZooKeeper in the "mutations/" subfolder.
    String name;

    /// Number of the mutation (it's always a part of its name).
    Int64 number = 0;

    /// Mutation commands.
    MutationCommands commands;

    /// The numbers specifying which blocks this mutation is applied to.
    /// For MergeTree `block_number` is used, for ReplicatedMergeTree `block_numbers` is used.
    std::optional<Int64> block_number;
    std::optional<std::map<String, Int64>> block_numbers;

    static MutationInfoFromBackup parseFromString(const String & str, const String & filename);
    String toString(bool one_line = false) const;

    void read(ReadBuffer & in, const String & filename);
    void write(WriteBuffer & out, bool one_line = false) const;
};

/// Reads information about all mutations from a backup.
std::vector<MutationInfoFromBackup> readMutationsFromBackup(const IBackup & backup, const String & dir_in_backup, Strings & file_names_in_backup);

}
