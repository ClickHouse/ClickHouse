#pragma once

/// Record batch v2 codec (uncompressed only in v1).
///
/// Wire layout:
///
///   baseOffset            INT64
///   batchLength           INT32   // length of everything that follows
///   partitionLeaderEpoch  INT32
///   magic                 INT8    // must be 2
///   crc                   UINT32  // CRC32C over everything after this field
///   attributes            INT16
///   lastOffsetDelta       INT32
///   firstTimestamp        INT64
///   maxTimestamp          INT64
///   producerId            INT64
///   producerEpoch         INT16
///   baseSequence          INT32
///   records               INT32 count, then records back-to-back
///
/// Inside a record every count/length is a zig-zag varint and null keys/values
/// use length -1.

#include <Storages/Kafka/Clients/Protocol/Wire.h>
#include <Storages/Kafka/Clients/Types.h>

#include <optional>
#include <string_view>
#include <vector>

namespace Kafka::Protocol
{

inline constexpr Int8 record_magic_v2 = 2;

/// attributes bits 0-2. The underlying type is the wire type; it must be a
/// standard integer type (ClickHouse's Int8 is _BitInt(8), invalid here).
enum class CompressionType : int8_t
{
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
};

const char * compressionTypeName(CompressionType type);

/// One record inside a batch, in relative (delta) form.
struct BatchRecord
{
    /// Always 0 in v2 — reserved.
    Int8 attributes = 0;
    /// Offset of this record relative to RecordBatch::base_offset.
    Int32 offset_delta = 0;
    /// Timestamp relative to RecordBatch::first_timestamp.
    Int64 timestamp_delta = 0;
    std::optional<Bytes> key;
    std::optional<Bytes> value;
    std::vector<RecordHeader> headers;

    bool operator==(const BatchRecord &) const = default;
};

struct RecordBatch
{
    Int64 base_offset = 0;
    Int32 partition_leader_epoch = -1;
    Int16 attributes = 0;
    Int32 last_offset_delta = 0;
    Int64 first_timestamp = 0;
    Int64 max_timestamp = 0;
    Int64 producer_id = -1;
    Int16 producer_epoch = -1;
    Int32 base_sequence = -1;
    std::vector<BatchRecord> records;

    bool operator==(const RecordBatch &) const = default;

    CompressionType compressionType() const { return static_cast<CompressionType>(attributes & 0x07); }
    /// attributes bit 3: every record carries the broker-assigned max_timestamp.
    bool usesLogAppendTime() const { return (attributes & 0x08) != 0; }
    /// attributes bit 4: the batch belongs to a transaction (KIP-98).
    bool isTransactional() const { return (attributes & 0x10) != 0; }
    /// Control batches carry transaction markers; consumers skip their records
    /// but must still advance past their offsets.
    bool isControl() const { return (attributes & 0x20) != 0; }
    /// The offset right after this batch.
    Int64 nextOffset() const { return base_offset + last_offset_delta + 1; }

    /// Build a non-idempotent, non-transactional, uncompressed batch.
    static RecordBatch make(Int64 base_offset, std::vector<BatchRecord> records);

    void encode(Writer & w) const { encode(w, CompressionType::None); }
    /// Encode with the records section compressed (attributes bits set
    /// accordingly). level -1 = the codec's default.
    void encode(Writer & w, CompressionType compression, int compression_level = -1) const;
    /// Decode one batch. Verifies CRC32C, rejects non-v2 magic, decompresses
    /// compressed record sections (NotSupported for codecs the build lacks).
    static RecordBatch decode(Reader & r);
};

struct DecodedBatches
{
    std::vector<RecordBatch> batches;
    /// True when the blob ended in a partial batch (the broker cut the
    /// response at max_bytes). The partial tail is discarded; re-fetching from
    /// the next offset returns it whole.
    bool truncated = false;
};

/// Wire size of one record including its varint length prefix — for sizing
/// batches against the broker's message.max.bytes without encoding twice.
size_t recordWireSize(const BatchRecord & record);

/// Fixed size of everything in a batch before the records array
/// (baseOffset..recordCount).
inline constexpr size_t record_batch_header_size = 61;

/// Encode a batch sequence into one blob (the `records` field of Produce/Fetch).
String encodeBatches(
    const std::vector<RecordBatch> & batches, CompressionType compression = CompressionType::None, int compression_level = -1);

/// Decode every complete batch from a Fetch `records` blob, tolerating a
/// truncated trailing batch.
DecodedBatches decodeBatches(std::string_view blob);

/// One aborted transaction from a Fetch response: the records of
/// `producer_id` starting at `first_offset` (until that producer's ABORT
/// control marker) belong to an aborted transaction.
struct AbortedTransactionRange
{
    Int64 producer_id = 0;
    Int64 first_offset = 0;
};

struct AssembledRecords
{
    /// Absolute offsets/timestamps; control batches, pre-min_offset records
    /// (compaction artifacts) and — under read_committed — aborted
    /// transactional records are filtered out.
    std::vector<Record> records;
    /// Where the next fetch should continue. Advances past control batches and
    /// filtered gaps even when `records` is empty.
    Int64 next_offset = 0;
    /// The blob ended in a partial batch (cut at max_bytes) that was discarded.
    bool truncated = false;
};

/// Decode a Fetch `records` blob into consumable records. Under
/// read_committed, records of aborted transactions are dropped following the
/// Java consumer's algorithm (KIP-98): a producer id becomes "aborted" when
/// the fetch response lists an aborted range starting at or before the batch,
/// and stops being aborted at its ABORT control marker.
AssembledRecords assembleFetchedRecords(
    std::string_view blob, Int64 min_offset, bool read_committed, std::vector<AbortedTransactionRange> aborted_transactions);

}
