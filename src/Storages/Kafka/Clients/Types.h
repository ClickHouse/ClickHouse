#pragma once

#include <base/types.h>

#include <array>
#include <optional>
#include <vector>

namespace Kafka
{

/// Owned byte buffer. Kafka keys/values/payloads are raw bytes; String is
/// the pragmatic container (contiguous, SSO, maps 1:1 onto ClickHouse columns).
using Bytes = String;

/// Kafka topic UUID (RFC 4122 byte order). Used by Metadata v10+ and Fetch v13+.
struct Uuid
{
    std::array<UInt8, 16> data{};

    bool operator==(const Uuid &) const = default;
    bool isZero() const { return *this == Uuid{}; }

    /// Hyphenated lowercase hex: 8-4-4-4-12.
    String toString() const;
};

struct TopicPartition
{
    String topic;
    Int32 partition = 0;

    auto operator<=>(const TopicPartition &) const = default;
};

struct TopicPartitionOffset
{
    String topic;
    Int32 partition = 0;
    Int64 offset = 0;
    /// Leader epoch of the committed offset, -1 when unknown. Carried through
    /// OffsetCommit/OffsetFetch for KIP-320 fencing; safe to leave at -1.
    Int32 leader_epoch = -1;
    /// Optional commit metadata stored broker-side next to the offset.
    std::optional<String> metadata;

    TopicPartition topicPartition() const { return {topic, partition}; }
};

struct RecordHeader
{
    String key;
    std::optional<Bytes> value;

    bool operator==(const RecordHeader &) const = default;
};

/// One consumed record. Offsets/timestamps are absolute (already combined with
/// the batch base values).
struct Record
{
    Int64 offset = 0;
    /// Milliseconds since epoch. Meaning depends on the topic's timestamp type
    /// (CreateTime or LogAppendTime).
    Int64 timestamp_ms = -1;
    std::optional<Bytes> key;
    std::optional<Bytes> value;
    std::vector<RecordHeader> headers;

    bool operator==(const Record &) const = default;
};

/// Compression codec for produced batches. Values match the wire encoding
/// (record-batch attributes bits 0-2) and Protocol::CompressionType.
enum class Compression : UInt8
{
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
};

/// Target of a ListOffsets query.
class OffsetSpec
{
public:
    static OffsetSpec earliest() { return OffsetSpec{-2}; }
    static OffsetSpec latest() { return OffsetSpec{-1}; }
    /// First offset with timestamp >= t (milliseconds since epoch).
    static OffsetSpec forTimestamp(Int64 timestamp_ms) { return OffsetSpec{timestamp_ms}; }

    /// The raw value sent in ListOffsetsRequest.timestamp.
    Int64 wireTimestamp() const { return wire_timestamp; }

private:
    explicit OffsetSpec(Int64 wire_timestamp_) : wire_timestamp(wire_timestamp_) { }
    Int64 wire_timestamp;
};

}
