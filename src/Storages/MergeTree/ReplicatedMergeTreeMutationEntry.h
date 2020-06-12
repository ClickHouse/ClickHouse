#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/MutationCommands.h>
#include <map>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Mutation entry in /mutations path in zookeeper. This record contains information about blocks
/// in patitions. We will mutatate all parts with left number less than this numbers.
///
/// These entries processed separately from main replication /log, and produce other entries
/// -- MUTATE_PART in main replication log.
struct ReplicatedMergeTreeMutationEntry
{
    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);

    String toString() const;
    static ReplicatedMergeTreeMutationEntry parse(const String & str, String znode_name);

    /// Name of znode (mutation-xxxxxxx)
    String znode_name;

    /// Create time of znode
    time_t create_time = 0;

    /// Replica which initiated mutation
    String source_replica;

    /// Accuired numbers of blocks
    /// partition_id -> block_number
    std::map<String, Int64> block_numbers;

    /// Mutation commands which will give to MUTATE_PART entries
    MutationCommands commands;

    /// Version of metadata. Not equal to -1 only if this mutation
    /// was created by ALTER MODIFY/DROP queries.
    int alter_version = -1;

    bool isAlterMutation() const { return alter_version != -1; }
};

using ReplicatedMergeTreeMutationEntryPtr = std::shared_ptr<const ReplicatedMergeTreeMutationEntry>;

}
