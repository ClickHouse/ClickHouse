#pragma once

#include <unordered_map>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

struct ReplicatedMergeTreeQuorumAddedParts
{
    using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;
    using PartitionIdToPartName = std::unordered_map<String, String>;

    PartitionIdToPartName added_parts;

    MergeTreeDataFormatVersion format_version;

    explicit ReplicatedMergeTreeQuorumAddedParts(const MergeTreeDataFormatVersion format_version_)
        : format_version(format_version_)
    {}

    /// Write new parts in buffer with added parts.
    void write(WriteBuffer & out)
    {
        out << "version: " << 2 << '\n';
        out << "parts count: " << added_parts.size() << '\n';

        for (const auto & part : added_parts)
            out << part.first << '\t' << part.second << '\n';
    }

    PartitionIdToMaxBlock getMaxInsertedBlocks()
    {
        PartitionIdToMaxBlock max_added_blocks;

        for (const auto & part : added_parts)
        {
            auto part_info = MergeTreePartInfo::fromPartName(part.second, format_version);
            max_added_blocks[part.first] = part_info.max_block;
        }

        return max_added_blocks;
    }

    void read(ReadBuffer & in)
    {
        if (checkString("version: ", in))
        {
            size_t version;

            readText(version, in);
            assertChar('\n', in);

            if (version == 2)
                added_parts = readV2(in);
        }
        else
            added_parts = readV1(in);
    }

    /// Read added blocks when node in ZooKeeper supports only one partition.
    PartitionIdToPartName readV1(ReadBuffer & in) const
    {
        PartitionIdToPartName parts_in_quorum;

        std::string part_name;

        readText(part_name, in);

        auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
        parts_in_quorum[part_info.partition_id] = part_name;

        return parts_in_quorum;
    }

    /// Read blocks when node in ZooKeeper supports multiple partitions.
    PartitionIdToPartName readV2(ReadBuffer & in)
    {
        assertString("parts count: ", in);

        PartitionIdToPartName parts_in_quorum;

        uint64_t parts_count;
        readText(parts_count, in);
        assertChar('\n', in);

        for (uint64_t i = 0; i < parts_count; ++i)
        {
            std::string partition_id;
            std::string part_name;

            readText(partition_id, in);
            assertChar('\t', in);
            readText(part_name, in);
            assertChar('\n', in);

            parts_in_quorum[partition_id] = part_name;
        }
        return parts_in_quorum;
    }

    void fromString(const std::string & str)
    {
        ReadBufferFromString in(str);
        read(in);
    }

    std::string toString()
    {
        WriteBufferFromOwnString out;
        write(out);
        return out.str();
    }

};

}
