#pragma once

#include <common/types.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

#include <Storages/MergeTree/MarkRange.h>


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

    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in);
};

struct PartitionReadResponce
{
    bool denied;
    MarkRanges mark_ranges;

    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in);
};


using MergeTreeReadTaskCallback = std::function<PartitionReadResponce(PartitionReadRequest)>;


}
