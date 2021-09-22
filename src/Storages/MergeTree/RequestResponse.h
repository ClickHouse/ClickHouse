#pragma once

#include <algorithm>
#include <vector>
#include <compare>
#include <numeric>
#include <unordered_map>
#include <map>
#include <iostream>
#include <set>
#include <cassert>

#include <common/types.h>
#include <Storages/MergeTree/MarkRange.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

/// Represents a segment [left; right]
struct PartBlockRange
{
    Int64 begin;
    Int64 end;

    bool operator==(const PartBlockRange & rhs) const
    {
        return begin == rhs.begin && end == rhs.end;
    }
};

struct PartitionReadRequest
{
    String partition_id;
    String part_name;
    String projection_name;
    PartBlockRange block_range;
    MarkRanges mark_ranges;

    void serialize(WriteBuffer & out) const
    {
        writeStringBinary(partition_id, out);
        writeStringBinary(part_name, out);
        writeStringBinary(projection_name, out);

        writeVarInt(block_range.begin, out);
        writeVarInt(block_range.end, out);

        writeDequeBinary(mark_ranges, out);
    }

    void deserialize(ReadBuffer & in)
    {
        readStringBinary(partition_id, in);
        readStringBinary(part_name, in);
        readStringBinary(projection_name, in);

        readVarInt(block_range.begin, in);
        readVarInt(block_range.end, in);

        readDequeBinary(mark_ranges, in);
    }

};

struct PartitionReadResponce
{
    bool denied;
    MarkRanges mark_ranges;

    void serialize(WriteBuffer & out) const
    {
        writeVarUInt(static_cast<UInt64>(denied), out);
        writeDequeBinary(mark_ranges, out);
    }

    void deserialize(ReadBuffer & in)
    {
        UInt64 value;
        readVarUInt(value, in);
        denied = static_cast<bool>(value);
        readDequeBinary(mark_ranges, in);
    }
};


using MergeTreeReadTaskCallback = std::function<PartitionReadResponce(PartitionReadRequest)>;


}
