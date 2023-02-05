#pragma once

#include <condition_variable>
#include <functional>
#include <optional>

#include <base/types.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

enum class CoordinationMode
{
    Default,
    /// For reading in order
    WithOrder,
    ReverseOrder
};

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

struct ParallelReadRequest
{
    CoordinationMode mode;
    size_t replica_num;
    size_t min_number_of_marks;

    /// Extension for ordered mode
    RangesInDataPartsDescription description;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);
    void merge(ParallelReadRequest & other);
};

struct ParallelReadResponse
{
    bool finish{false};
    RangesInDataPartsDescription description;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);
};


struct InitialAllRangesAnnouncement
{
    RangesInDataPartsDescription description;
    size_t replica_num;

    void serialize(WriteBuffer & out) const;
    String describe();
    void deserialize(ReadBuffer & in);
};


using MergeTreeAllRangesCallback = std::function<void(InitialAllRangesAnnouncement)>;
using MergeTreeReadTaskCallback = std::function<std::optional<ParallelReadResponse>(ParallelReadRequest)>;

}
