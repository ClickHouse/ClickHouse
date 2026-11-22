#pragma once

#include "config.h"

#if USE_PULSAR

#include <Storages/Pulsar/Client/Message.h>
#include <base/extended_types.h>
#include <base/types.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/StringWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <MetadataViews.pb.h>
#include <PulsarApi.pb.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>


/// The framing and the message format of the Pulsar binary protocol:
/// https://pulsar.apache.org/docs/next/developing-binary-protocol/
/// Pure functions without I/O. Every size read from the network is checked against the bytes
/// actually present and against `Limits` before it is used.
namespace DB::Pulsar
{

using ValidationError = PulsarProto::CommandAck::ValidationError;
using SharedBuffer = std::shared_ptr<const StringWithMemoryTracking>;

struct Limits
{
    /// Until the broker announces its own in `CONNECTED`. The broker's default.
    size_t max_message_size = 5 * 1024 * 1024;
    /// Commands are small, only `MESSAGE` and `SEND` carry a payload after the command.
    size_t max_command_size = 1024 * 1024;
    /// The size of a decompressed message, and of an assembled chunked message.
    size_t max_uncompressed_size = 256 * 1024 * 1024;
    /// The partitions of the topics of one consumer: the number comes from the broker, and each one needs a connection
    /// and some state.
    size_t max_sub_consumers = 65536;
};

/// The largest `max_message_size` accepted from `CONNECTED`.
constexpr size_t MAX_ALLOWED_MESSAGE_SIZE = 256 * 1024 * 1024;
/// A frame is bigger than the message it carries by the command and the metadata.
constexpr size_t FRAME_PADDING = 10 * 1024;
/// The size of the frame size field.
constexpr size_t FRAME_SIZE_LENGTH = 4;

/// Returns the size of the frame after its 4-byte size field, or throws if it is out of bounds.
size_t decodeFrameSize(const char * data, const Limits & limits);

StringWithMemoryTracking encodeCommand(const PulsarProto::BaseCommand & command);
/// A `SEND` frame with the checksum.
StringWithMemoryTracking encodeSendFrame(
    const PulsarProto::BaseCommand & command, const PulsarProto::MessageMetadata & metadata, std::string_view payload);

struct Frame
{
    PulsarProto::BaseCommand command;
    /// For `MESSAGE`, the bytes after the command.
    std::string_view tail;
};

/// Decodes a frame after its size field. A command of an unknown type is returned without `type`.
void decodeFrame(std::string_view body, const Limits & limits, Frame & frame);

/// A message as stored by the broker: the metadata and the raw payload (compressed, maybe a chunk or a batch).
struct Entry
{
    PulsarProto::MessageMetadataView metadata;
    std::string_view payload;
    /// The checksum does not match.
    bool checksum_mismatch = false;
};

/// Decodes the tail of a `MESSAGE` frame.
void decodeEntry(std::string_view tail, Entry & entry);

/// The decompressed payload of a message, which may be a batch.
struct Batch
{
    SharedBuffer buffer;
    std::string_view data;
    /// For a batch, the number of entries in `data`, which are validated.
    std::optional<size_t> num_entries;
};

struct DecodedPayload
{
    std::optional<ValidationError> error;
    Batch batch;
};

/// Decompresses the payload of a non-chunked message, or of an assembled chunked message, and validates the batch.
/// `owner` keeps `payload` alive. `check_compressed_size` is false for an assembled chunked message.
DecodedPayload decodePayload(
    const PulsarProto::MessageMetadataView & metadata,
    std::string_view payload,
    SharedBuffer owner,
    size_t max_message_size,
    bool check_compressed_size,
    const Limits & limits);

/// The fields of one message of a batch, or of a non-batched message. The views point into the batch or into the cursor.
struct MessageFields
{
    std::string_view payload;
    std::optional<std::string_view> partition_key;
    std::optional<std::string_view> ordering_key;
    std::optional<UInt64> event_time;
    Int32 batch_index = -1;
};

/// Iterates over the messages of a validated batch without materializing all of them.
class BatchCursor
{
public:
    BatchCursor(Batch batch_, const PulsarProto::MessageMetadataView & metadata);

    bool hasNext() const { return index < count; }
    size_t remaining() const { return count - index; }
    size_t size() const { return count; }
    const SharedBuffer & buffer() const { return batch.buffer; }
    bool isBatched() const { return batch.num_entries.has_value(); }

    /// The fields stay valid until the next call.
    const MessageFields & next();

private:
    Batch batch;
    size_t count = 0;
    size_t index = 0;
    size_t offset = 0;
    /// For a non-batched message.
    std::optional<String> partition_key;
    std::optional<String> ordering_key;
    std::optional<UInt64> event_time;
    PulsarProto::SingleMessageMetadataView entry_metadata;
    MessageFields fields;
};

using ChunkId = EntryId;

/// Assembles chunked messages of one topic partition with the policy of the Pulsar C++ client.
/// Every chunk ends in the assembled message, in `ack`, `ack_corrupted` or `redeliver`, or is ignored as a duplicate.
class ChunkAssembler
{
public:
    using Clock = std::function<UInt64()>;

    /// The milliseconds since the epoch.
    explicit ChunkAssembler(Clock clock_);

    struct Result
    {
        /// Acknowledge (the chunked message is dropped).
        VectorWithMemoryTracking<ChunkId> ack;
        /// Acknowledge with `UncompressedSizeCorruption`.
        std::optional<ChunkId> ack_corrupted;
        /// Request redelivery.
        VectorWithMemoryTracking<ChunkId> redeliver;
        /// The assembled message and the ids of all its chunks.
        std::optional<StringWithMemoryTracking> message;
        VectorWithMemoryTracking<ChunkId> message_chunks;
    };

    void add(const PulsarProto::MessageMetadataView & metadata, ChunkId id, std::string_view payload, size_t max_message_size, const Limits & limits, Result & result);

    /// Drops the messages whose last chunk was received more than `EXPIRY_MS` ago, their chunks go to `ack`.
    void expire(Result & result);

    size_t pendingMessages() const { return contexts.size(); }

    static constexpr UInt64 EXPIRY_MS = 60'000;
    static constexpr size_t MAX_PENDING_MESSAGES = 10;
    static constexpr size_t MAX_ABANDONED_MESSAGES = 10'000;

private:
    struct Context
    {
        StringWithMemoryTracking uuid;
        size_t num_chunks = 0;
        size_t total_size = 0;
        StringWithMemoryTracking buffer;
        VectorWithMemoryTracking<ChunkId> chunks;
        UInt64 last_received_ms = 0;
    };

    Context * find(std::string_view uuid);
    void remove(std::string_view uuid);
    void rememberAbandoned(UInt128 uuid_hash, bool redeliver);
    std::optional<bool> abandonedDisposition(UInt128 uuid_hash) const;
    void discardMalformed(std::string_view uuid, ChunkId id, Result & result);

    Clock clock;
    /// The oldest first.
    DequeWithMemoryTracking<Context> contexts;
    /// The hashes of the uuids of the dropped messages (a uuid can be as big as a frame), with whether their chunks were
    /// redelivered (or acknowledged). The oldest first.
    DequeWithMemoryTracking<std::pair<UInt128, bool>> abandoned;
};

/// `topic`, `tenant/namespace/topic` or `persistent://tenant/namespace/topic`, `non-persistent://...`.
String normalizeTopicName(std::string_view name);
String partitionTopicName(const String & topic, size_t partition);

struct ServiceUrl
{
    bool tls = false;
    VectorWithMemoryTracking<std::pair<String, UInt16>> hosts;
};

/// `pulsar://host[:port][,host[:port]...]` or `pulsar+ssl://...`.
ServiceUrl parseServiceUrl(std::string_view url);

}

#endif
