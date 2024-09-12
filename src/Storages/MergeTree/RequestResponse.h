#pragma once

#include <functional>
#include <optional>

#include <base/types.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

/// This enum is being serialized and transferred over a network
/// You can't reorder it or add another value in the middle
enum class CoordinationMode : uint8_t
{
    Default = 0,
    /// For reading in order
    WithOrder = 1,
    ReverseOrder = 2,

    MAX = ReverseOrder,
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

/// ParallelReadRequest is used by remote replicas during parallel read
/// to signal an initiator that they need more marks to read.
struct ParallelReadRequest
{
    /// No default constructor, you must initialize all fields at once.

    ParallelReadRequest(
        CoordinationMode mode_,
        size_t replica_num_,
        size_t min_number_of_marks_,
        RangesInDataPartsDescription description_)
        : mode(mode_)
        , replica_num(replica_num_)
        , min_number_of_marks(min_number_of_marks_)
        , description(std::move(description_))
    {}

    CoordinationMode mode;
    size_t replica_num;
    size_t min_number_of_marks;
    /// Extension for Ordered (InOrder or ReverseOrder) mode
    /// Contains only data part names without mark ranges.
    RangesInDataPartsDescription description;

    void serialize(WriteBuffer & out, UInt64 initiator_protocol_version) const;
    String describe() const;
    static ParallelReadRequest deserialize(ReadBuffer & in);
    void merge(ParallelReadRequest & other);
};

/// ParallelReadResponse is used by an initiator to tell
/// remote replicas about what to read during parallel reading.
/// Additionally contains information whether there are more available
/// marks to read (whether it is the last packet or not).
struct ParallelReadResponse
{
    bool finish{false};
    RangesInDataPartsDescription description;

    void serialize(WriteBuffer & out, UInt64 replica_protocol_version) const;
    String describe() const;
    void deserialize(ReadBuffer & in);
};


/// The set of parts (their names) along with ranges to read which is sent back
/// to the initiator by remote replicas during parallel reading.
/// Additionally contains an identifier (replica_num) plus
/// the reading algorithm chosen (Default, InOrder or ReverseOrder).
struct InitialAllRangesAnnouncement
{
    /// No default constructor, you must initialize all fields at once.

    InitialAllRangesAnnouncement(
        CoordinationMode mode_, RangesInDataPartsDescription description_, size_t replica_num_, size_t mark_segment_size_)
        : mode(mode_), description(std::move(description_)), replica_num(replica_num_), mark_segment_size(mark_segment_size_)
    {}

    CoordinationMode mode;
    RangesInDataPartsDescription description;
    size_t replica_num;
    size_t mark_segment_size;

    void serialize(WriteBuffer & out, UInt64 initiator_protocol_version) const;
    String describe();
    static InitialAllRangesAnnouncement deserialize(ReadBuffer & i, UInt64 replica_protocol_version);
};


using MergeTreeAllRangesCallback = std::function<void(InitialAllRangesAnnouncement)>;
using MergeTreeReadTaskCallback = std::function<std::optional<ParallelReadResponse>(ParallelReadRequest)>;

}
