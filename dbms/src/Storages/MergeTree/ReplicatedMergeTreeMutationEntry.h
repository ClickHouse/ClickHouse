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

struct ReplicatedMergeTreeMutationEntry
{
    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);

    String toString() const;
    static ReplicatedMergeTreeMutationEntry parse(const String & str, String znode_name);

    String znode_name;

    time_t create_time = 0;
    String source_replica;

    std::map<String, Int64> block_numbers;
    MutationCommands commands;
};

using ReplicatedMergeTreeMutationEntryPtr = std::shared_ptr<const ReplicatedMergeTreeMutationEntry>;

}
