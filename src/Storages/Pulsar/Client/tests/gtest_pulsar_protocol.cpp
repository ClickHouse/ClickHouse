#include "config.h"

#if USE_PULSAR

#include <Storages/Pulsar/Client/Protocol.h>
#include <Common/Exception.h>
#include <base/unaligned.h>

#include <gtest/gtest.h>

#include <fmt/format.h>

#include <crc32c/crc32c.h>
#include <lz4.h>
#include <snappy.h>
#include <zlib.h>
#include <zstd.h>

using namespace DB;
using namespace DB::Pulsar;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace
{

constexpr UInt32 THREE_GIB = 3U * 1024 * 1024 * 1024;

String be32(UInt32 value)
{
    String res(4, '\0');
    unalignedStoreBigEndian<UInt32>(res.data(), value);
    return res;
}

String be16(UInt16 value)
{
    String res(2, '\0');
    unalignedStoreBigEndian<UInt16>(res.data(), value);
    return res;
}

PulsarProto::BaseCommand messageCommand(UInt64 ledger = 1, UInt64 entry = 2)
{
    PulsarProto::BaseCommand command;
    command.set_type(PulsarProto::BaseCommand::MESSAGE);
    command.mutable_message()->set_consumer_id(1);
    command.mutable_message()->mutable_message_id()->set_ledgerid(ledger);
    command.mutable_message()->mutable_message_id()->set_entryid(entry);
    return command;
}

PulsarProto::MessageMetadata baseMetadata()
{
    PulsarProto::MessageMetadata metadata;
    metadata.set_producer_name("p");
    metadata.set_sequence_id(0);
    metadata.set_publish_time(1);
    return metadata;
}

/// The tail of a `MESSAGE` frame: [broker entry metadata][checksum][metadata size][metadata][payload].
String messageTail(const String & metadata, std::string_view payload, bool checksum = true, std::optional<String> broker_entry_metadata = {})
{
    String checksummed = be32(static_cast<UInt32>(metadata.size())) + metadata + String(payload);
    String res;
    if (broker_entry_metadata)
        res += be16(0x0e02) + be32(static_cast<UInt32>(broker_entry_metadata->size())) + *broker_entry_metadata;
    if (checksum)
        res += be16(0x0e01) + be32(crc32c::Crc32c(checksummed.data(), checksummed.size()));
    return res + checksummed;
}

String serialize(const google::protobuf::MessageLite & message)
{
    String res;
    EXPECT_TRUE(message.SerializeToString(&res));
    return res;
}

/// A frame body (after the 4-byte size): [command size][command][tail].
String frameBody(const PulsarProto::BaseCommand & command, const String & tail = {})
{
    String serialized = serialize(command);
    return be32(static_cast<UInt32>(serialized.size())) + serialized + tail;
}

PulsarProto::MessageMetadataView view(const PulsarProto::MessageMetadata & metadata)
{
    PulsarProto::MessageMetadataView res;
    String serialized = serialize(metadata);
    EXPECT_TRUE(res.ParseFromString(serialized));
    return res;
}

String compress(PulsarProto::CompressionType codec, const String & data)
{
    String res;
    switch (codec)
    {
        case PulsarProto::LZ4:
            res.resize(LZ4_compressBound(static_cast<int>(data.size())));
            res.resize(LZ4_compress_default(data.data(), res.data(), static_cast<int>(data.size()), static_cast<int>(res.size())));
            break;
        case PulsarProto::ZSTD:
            res.resize(ZSTD_compressBound(data.size()));
            res.resize(ZSTD_compress(res.data(), res.size(), data.data(), data.size(), 1));
            break;
        case PulsarProto::ZLIB:
        {
            uLongf size = compressBound(data.size());
            res.resize(size);
            EXPECT_EQ(::compress(reinterpret_cast<Bytef *>(res.data()), &size, reinterpret_cast<const Bytef *>(data.data()), data.size()), Z_OK);
            res.resize(size);
            break;
        }
        case PulsarProto::SNAPPY:
            snappy::Compress(data.data(), data.size(), &res);
            break;
        default:
            res = data;
    }
    return res;
}

/// [u32 size][SingleMessageMetadata][payload]
String batchEntry(const String & payload, std::optional<String> ordering_key = {}, std::optional<String> partition_key = {}, std::optional<UInt64> event_time = {})
{
    PulsarProto::SingleMessageMetadata metadata;
    metadata.set_payload_size(static_cast<Int32>(payload.size()));
    if (ordering_key)
        metadata.set_ordering_key(*ordering_key);
    if (partition_key)
        metadata.set_partition_key(*partition_key);
    if (event_time)
        metadata.set_event_time(*event_time);
    String serialized = serialize(metadata);
    return be32(static_cast<UInt32>(serialized.size())) + serialized + payload;
}

DecodedPayload decode(const PulsarProto::MessageMetadata & metadata, const String & payload, const Limits & limits = {})
{
    auto owner = std::make_shared<StringWithMemoryTracking>(payload);
    return decodePayload(view(metadata), *owner, owner, limits.max_message_size, true, limits);
}

void expectUnexpected(auto && f)
{
    try
    {
        f();
        FAIL() << "No exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER) << e.message();
    }
}

}


TEST(PulsarProtocol, MetadataViewsMatchTheUpstreamFieldNumbers)
{
    PulsarProto::MessageMetadata metadata = baseMetadata();
    metadata.set_publish_time(123);
    metadata.add_properties()->set_key("k");
    metadata.mutable_properties(0)->set_value("v");
    metadata.set_replicated_from("r");
    metadata.set_partition_key("pk");
    metadata.add_replicate_to("c");
    metadata.set_compression(PulsarProto::ZSTD);
    metadata.set_uncompressed_size(456);
    metadata.set_num_messages_in_batch(7);
    metadata.set_event_time(789);
    metadata.add_encryption_keys()->set_key("key");
    metadata.mutable_encryption_keys(0)->set_value("value");
    metadata.set_encryption_algo("a");
    metadata.set_encryption_param("p");
    metadata.set_schema_version("s");
    metadata.set_partition_key_b64_encoded(true);
    metadata.set_ordering_key("ok");
    metadata.set_deliver_at_time(1);
    metadata.set_marker_type(1);
    metadata.set_txnid_least_bits(1);
    metadata.set_txnid_most_bits(1);
    metadata.set_highest_sequence_id(1);
    metadata.set_null_value(true);
    metadata.set_uuid("uuid");
    metadata.set_num_chunks_from_msg(3);
    metadata.set_total_chunk_msg_size(30);
    metadata.set_chunk_id(2);
    metadata.set_null_partition_key(true);
    metadata.add_compacted_batch_indexes(1);
    metadata.set_schema_id("id");
    metadata.set_entry_hash_min(1);
    metadata.set_entry_hash_max(2);

    auto v = view(metadata);
    EXPECT_EQ(v.publish_time(), 123);
    EXPECT_EQ(v.partition_key(), "pk");
    EXPECT_EQ(v.compression(), PulsarProto::ZSTD);
    EXPECT_EQ(v.uncompressed_size(), 456);
    EXPECT_EQ(v.num_messages_in_batch(), 7);
    EXPECT_EQ(v.event_time(), 789);
    EXPECT_TRUE(v.has_encryption_keys());
    EXPECT_EQ(v.ordering_key(), "ok");
    EXPECT_EQ(v.uuid(), "uuid");
    EXPECT_EQ(v.num_chunks_from_msg(), 3);
    EXPECT_EQ(v.total_chunk_msg_size(), 30);
    EXPECT_EQ(v.chunk_id(), 2);

    PulsarProto::SingleMessageMetadata single;
    single.add_properties()->set_key("k");
    single.mutable_properties(0)->set_value("v");
    single.set_partition_key("pk");
    single.set_payload_size(5);
    single.set_compacted_out(true);
    single.set_event_time(6);
    single.set_partition_key_b64_encoded(true);
    single.set_ordering_key("ok");
    single.set_sequence_id(7);
    single.set_null_value(true);
    single.set_null_partition_key(true);
    PulsarProto::SingleMessageMetadataView single_view;
    ASSERT_TRUE(single_view.ParseFromString(serialize(single)));
    EXPECT_EQ(single_view.partition_key(), "pk");
    EXPECT_EQ(single_view.payload_size(), 5);
    EXPECT_EQ(single_view.event_time(), 6);
    EXPECT_EQ(single_view.ordering_key(), "ok");
}

TEST(PulsarProtocol, FrameSize)
{
    Limits limits;
    String header = be32(100);
    EXPECT_EQ(decodeFrameSize(header.data(), limits), 100);
    expectUnexpected([&] { String h = be32(3); decodeFrameSize(h.data(), limits); });
    expectUnexpected([&] { String h = be32(THREE_GIB); decodeFrameSize(h.data(), limits); });
    String max = be32(static_cast<UInt32>(limits.max_message_size + FRAME_PADDING));
    EXPECT_EQ(decodeFrameSize(max.data(), limits), limits.max_message_size + FRAME_PADDING);
    expectUnexpected([&] { String h = be32(static_cast<UInt32>(limits.max_message_size + FRAME_PADDING + 1)); decodeFrameSize(h.data(), limits); });
}

TEST(PulsarProtocol, CommandValidation)
{
    Limits limits;
    Frame frame;

    PulsarProto::BaseCommand ping;
    ping.set_type(PulsarProto::BaseCommand::PING);
    ping.mutable_ping();
    decodeFrame(frameBody(ping), limits, frame);
    EXPECT_EQ(frame.command.type(), PulsarProto::BaseCommand::PING);
    EXPECT_TRUE(frame.tail.empty());

    /// The command size beyond the frame, beyond the limit, a frame shorter than the command size field.
    expectUnexpected([&] { decodeFrame(be32(100) + "abc", limits, frame); });
    expectUnexpected([&] { decodeFrame(be32(THREE_GIB) + "abc", limits, frame); });
    Limits small;
    small.max_command_size = 2;
    expectUnexpected([&] { decodeFrame(frameBody(ping), small, frame); });
    expectUnexpected([&] { decodeFrame("ab", limits, frame); });
    /// Garbage instead of a command.
    expectUnexpected([&] { decodeFrame(be32(3) + "\xff\xff\xff", limits, frame); });
    /// Bytes after a command other than `MESSAGE`.
    expectUnexpected([&] { decodeFrame(frameBody(ping, "x"), limits, frame); });

    /// An unknown command type is returned without `type`.
    String unknown;
    unknown += static_cast<char>(0x08);
    unknown += static_cast<char>(0x7f);
    decodeFrame(be32(static_cast<UInt32>(unknown.size())) + unknown, limits, frame);
    EXPECT_FALSE(frame.command.has_type());
}

TEST(PulsarProtocol, EntryValidation)
{
    String metadata = serialize(baseMetadata());
    Entry entry;

    /// `entry` points into the tail.
    String tail = messageTail(metadata, "payload");
    decodeEntry(tail, entry);
    EXPECT_FALSE(entry.checksum_mismatch);
    EXPECT_EQ(entry.payload, "payload");

    /// Without the checksum.
    tail = messageTail(metadata, "payload", false);
    decodeEntry(tail, entry);
    EXPECT_FALSE(entry.checksum_mismatch);
    EXPECT_EQ(entry.payload, "payload");

    /// With broker entry metadata.
    PulsarProto::BrokerEntryMetadata broker_entry_metadata;
    broker_entry_metadata.set_broker_timestamp(1);
    tail = messageTail(metadata, "payload", true, serialize(broker_entry_metadata));
    decodeEntry(tail, entry);
    EXPECT_EQ(entry.payload, "payload");

    /// A checksum mismatch is a discard, not an error.
    tail = messageTail(metadata, "payload");
    tail.back() ^= 1;
    decodeEntry(tail, entry);
    EXPECT_TRUE(entry.checksum_mismatch);

    /// Sizes beyond the frame.
    expectUnexpected([&] { decodeEntry(be16(0x0e02) + be32(THREE_GIB) + "x", entry); });
    expectUnexpected([&] { decodeEntry(be16(0x0e02) + be32(2) + "\xff\xff" + messageTail(metadata, "p"), entry); });
    expectUnexpected([&] { decodeEntry(be16(0x0e01) + "ab", entry); });
    expectUnexpected([&] { decodeEntry(be32(THREE_GIB) + metadata, entry); });
    expectUnexpected([&] { decodeEntry("ab", entry); });
    expectUnexpected([&] { decodeEntry(messageTail("\xff\xff\xff", "payload", false), entry); });
}

TEST(PulsarProtocol, Codecs)
{
    const String data(100000, 'x');
    for (auto codec : {PulsarProto::LZ4, PulsarProto::ZLIB, PulsarProto::ZSTD, PulsarProto::SNAPPY})
    {
        SCOPED_TRACE(PulsarProto::CompressionType_Name(codec));
        String compressed = compress(codec, data);
        auto metadata = baseMetadata();
        metadata.set_compression(codec);

        metadata.set_uncompressed_size(static_cast<UInt32>(data.size()));
        auto decoded = decode(metadata, compressed);
        ASSERT_FALSE(decoded.error);
        EXPECT_EQ(decoded.batch.data, data);
        EXPECT_FALSE(decoded.batch.num_entries);

        for (UInt32 size : {static_cast<UInt32>(data.size() - 1), static_cast<UInt32>(data.size() + 1), 0U})
        {
            metadata.set_uncompressed_size(size);
            decoded = decode(metadata, compressed);
            ASSERT_TRUE(decoded.error);
            EXPECT_EQ(*decoded.error, PulsarProto::CommandAck::DecompressionError);
        }

        /// Nothing is allocated for a declared size above the limit.
        metadata.set_uncompressed_size(THREE_GIB);
        decoded = decode(metadata, compressed);
        ASSERT_TRUE(decoded.error);
        EXPECT_EQ(*decoded.error, PulsarProto::CommandAck::UncompressedSizeCorruption);

        /// Corrupted data.
        metadata.set_uncompressed_size(static_cast<UInt32>(data.size()));
        String corrupted = compressed.substr(0, compressed.size() / 2);
        decoded = decode(metadata, corrupted);
        ASSERT_TRUE(decoded.error);

        /// An empty payload.
        String empty_compressed = compress(codec, "");
        metadata.set_uncompressed_size(0);
        decoded = decode(metadata, empty_compressed);
        ASSERT_FALSE(decoded.error);
        EXPECT_TRUE(decoded.batch.data.empty());
    }

    {
        /// The Java client ends a zlib stream with a sync flush, without the final block and the checksum.
        z_stream stream{};
        ASSERT_EQ(deflateInit(&stream, Z_DEFAULT_COMPRESSION), Z_OK);
        String sync_flushed(compressBound(data.size()) + 64, '\0');
        stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(data.data()));
        stream.avail_in = static_cast<uInt>(data.size());
        stream.next_out = reinterpret_cast<Bytef *>(sync_flushed.data());
        stream.avail_out = static_cast<uInt>(sync_flushed.size());
        ASSERT_EQ(deflate(&stream, Z_SYNC_FLUSH), Z_OK);
        sync_flushed.resize(stream.total_out);
        deflateEnd(&stream);

        auto metadata = baseMetadata();
        metadata.set_compression(PulsarProto::ZLIB);
        metadata.set_uncompressed_size(static_cast<UInt32>(data.size()));
        auto decoded = decode(metadata, sync_flushed);
        ASSERT_FALSE(decoded.error);
        EXPECT_EQ(decoded.batch.data, data);
        /// Truncated, or with another size.
        EXPECT_TRUE(decode(metadata, sync_flushed.substr(0, sync_flushed.size() - 5)).error);
        metadata.set_uncompressed_size(static_cast<UInt32>(data.size() - 1));
        EXPECT_TRUE(decode(metadata, sync_flushed).error);
        metadata.set_uncompressed_size(static_cast<UInt32>(data.size() + 1));
        EXPECT_TRUE(decode(metadata, sync_flushed).error);
    }

    auto metadata = baseMetadata();
    metadata.set_compression(PulsarProto::LZ4);
    metadata.set_uncompressed_size(10);
    /// A compressed payload bigger than the maximum message size.
    Limits limits;
    limits.max_message_size = 4;
    auto decoded = decode(metadata, "12345", limits);
    ASSERT_TRUE(decoded.error);
    EXPECT_EQ(*decoded.error, PulsarProto::CommandAck::UncompressedSizeCorruption);

    /// An unknown codec is not read as `NONE`.
    PulsarProto::MessageMetadataView unknown_codec = view(baseMetadata());
    unknown_codec.set_compression(42);
    unknown_codec.set_uncompressed_size(3);
    auto owner = std::make_shared<StringWithMemoryTracking>("abc");
    decoded = decodePayload(unknown_codec, *owner, owner, 100, true, {});
    ASSERT_TRUE(decoded.error);
    EXPECT_EQ(*decoded.error, PulsarProto::CommandAck::DecompressionError);
}

TEST(PulsarProtocol, Batches)
{
    String batch = batchEntry("a", "ok", "pk", 0) + batchEntry("bb") + batchEntry("", {}, {}, 5);
    auto metadata = baseMetadata();
    metadata.set_num_messages_in_batch(3);
    metadata.set_ordering_key("outer");
    metadata.set_event_time(7);

    for (auto codec : {PulsarProto::NONE, PulsarProto::LZ4, PulsarProto::ZSTD})
    {
        metadata.set_compression(codec);
        metadata.set_uncompressed_size(static_cast<UInt32>(batch.size()));
        auto decoded = decode(metadata, compress(codec, batch));
        ASSERT_FALSE(decoded.error);
        ASSERT_EQ(decoded.batch.num_entries, 3);

        BatchCursor cursor(decoded.batch, view(metadata));
        ASSERT_EQ(cursor.size(), 3);
        const auto & first = cursor.next();
        EXPECT_EQ(first.payload, "a");
        EXPECT_EQ(first.ordering_key, "ok");
        EXPECT_EQ(first.partition_key, "pk");
        /// Present and 0, not absent.
        EXPECT_EQ(first.event_time, 0);
        EXPECT_EQ(first.batch_index, 0);
        const auto & second = cursor.next();
        EXPECT_EQ(second.payload, "bb");
        /// Only from the entry, not from the batch metadata.
        EXPECT_FALSE(second.ordering_key);
        EXPECT_FALSE(second.partition_key);
        EXPECT_FALSE(second.event_time);
        const auto & third = cursor.next();
        EXPECT_EQ(third.payload, "");
        EXPECT_EQ(third.event_time, 5);
        EXPECT_EQ(third.batch_index, 2);
        EXPECT_FALSE(cursor.hasNext());
    }

    metadata.set_compression(PulsarProto::NONE);
    auto expect_corrupted = [&](const String & payload, Int32 declared)
    {
        metadata.set_num_messages_in_batch(declared);
        auto decoded = decode(metadata, payload);
        ASSERT_TRUE(decoded.error);
        EXPECT_EQ(*decoded.error, PulsarProto::CommandAck::BatchDeSerializeError);
    };
    expect_corrupted(batch, 0);
    expect_corrupted(batch, -1);
    /// More than the payload can hold, checked before anything is reserved.
    expect_corrupted(batch, std::numeric_limits<Int32>::max());
    /// Fewer entries than declared.
    expect_corrupted(batch, 4);
    /// An entry size beyond the end.
    expect_corrupted(be32(THREE_GIB) + String(8, 'x'), 1);
    /// A payload size beyond the end.
    expect_corrupted(batchEntry("abc").substr(0, batchEntry("abc").size() - 1), 1);
    /// An entry without `payload_size`.
    {
        PulsarProto::SingleMessageMetadata no_payload_size;
        no_payload_size.set_ordering_key("k");
        String serialized;
        ASSERT_TRUE(no_payload_size.SerializePartialToString(&serialized));
        expect_corrupted(be32(static_cast<UInt32>(serialized.size())) + serialized, 1);
    }
    /// Unparsable entry metadata.
    expect_corrupted(be32(3) + "\xff\xff\xff" + "x", 1);
}

TEST(PulsarProtocol, LargeBatchIsNotMaterialized)
{
    constexpr size_t count = 1'000'000;
    const String entry = batchEntry("");
    String batch;
    batch.reserve(entry.size() * count);
    for (size_t i = 0; i < count; ++i)
        batch += entry;

    auto metadata = baseMetadata();
    metadata.set_num_messages_in_batch(static_cast<Int32>(count));
    auto decoded = decode(metadata, batch);
    ASSERT_FALSE(decoded.error);
    ASSERT_EQ(decoded.batch.num_entries, count);

    /// The cursor holds no per-entry state: its size does not depend on the batch.
    static_assert(sizeof(BatchCursor) < 1024);
    BatchCursor cursor(decoded.batch, view(metadata));
    size_t seen = 0;
    while (cursor.hasNext())
    {
        EXPECT_TRUE(cursor.next().payload.empty());
        ++seen;
    }
    EXPECT_EQ(seen, count);
}

TEST(PulsarProtocol, SendFrameRoundTrip)
{
    PulsarProto::BaseCommand command = messageCommand(5, 6);
    PulsarProto::MessageMetadata metadata = baseMetadata();
    metadata.set_ordering_key("key");
    metadata.set_event_time(42);
    auto frame_bytes = encodeSendFrame(command, metadata, "hello");

    Limits limits;
    ASSERT_EQ(decodeFrameSize(frame_bytes.data(), limits), frame_bytes.size() - 4);
    Frame frame;
    decodeFrame(std::string_view(frame_bytes).substr(4), limits, frame);
    ASSERT_EQ(frame.command.type(), PulsarProto::BaseCommand::MESSAGE);
    EXPECT_EQ(frame.command.message().message_id().entryid(), 6);

    Entry entry;
    decodeEntry(frame.tail, entry);
    ASSERT_FALSE(entry.checksum_mismatch);
    EXPECT_EQ(entry.metadata.ordering_key(), "key");

    auto owner = std::make_shared<StringWithMemoryTracking>(entry.payload);
    auto decoded = decodePayload(entry.metadata, *owner, owner, limits.max_message_size, true, limits);
    ASSERT_FALSE(decoded.error);
    BatchCursor cursor(decoded.batch, entry.metadata);
    const auto & fields = cursor.next();
    EXPECT_EQ(fields.payload, "hello");
    EXPECT_EQ(fields.ordering_key, "key");
    EXPECT_EQ(fields.event_time, 42);
    EXPECT_FALSE(fields.partition_key);
    EXPECT_EQ(fields.batch_index, -1);
}

TEST(PulsarProtocol, Names)
{
    EXPECT_EQ(normalizeTopicName("t"), "persistent://public/default/t");
    EXPECT_EQ(normalizeTopicName("a/b/t"), "persistent://a/b/t");
    EXPECT_EQ(normalizeTopicName("persistent://a/b/t"), "persistent://a/b/t");
    EXPECT_EQ(normalizeTopicName("non-persistent://a/b/t"), "non-persistent://a/b/t");
    for (const char * bad : {"", "a/t", "a/b/c/t", "persistent://t", "persistent://a/b", "http://a/b/t", "a//t", "/a/b", "a/b/"})
        EXPECT_THROW(normalizeTopicName(bad), Exception) << bad;
    EXPECT_EQ(partitionTopicName("persistent://a/b/t", 3), "persistent://a/b/t-partition-3");

    auto url = parseServiceUrl("pulsar://h1:1234, h2 ,[::1]:5/path");
    EXPECT_FALSE(url.tls);
    ASSERT_EQ(url.hosts.size(), 3);
    EXPECT_EQ(url.hosts[0], std::make_pair(String("h1"), UInt16(1234)));
    EXPECT_EQ(url.hosts[1], std::make_pair(String("h2"), UInt16(6650)));
    EXPECT_EQ(url.hosts[2], std::make_pair(String("[::1]"), UInt16(5)));
    url = parseServiceUrl("pulsar+ssl://h");
    EXPECT_TRUE(url.tls);
    EXPECT_EQ(url.hosts[0].second, 6651);
    for (const char * bad : {"http://h", "pulsar://", "pulsar://h,", "h:6650"})
        EXPECT_THROW(parseServiceUrl(bad), Exception) << bad;
}


namespace
{

struct ChunkTest
{
    UInt64 now = 1'000'000;
    ChunkAssembler assembler{[this] { return now; }};
    Limits limits;
    static constexpr size_t max_message_size = 100;

    PulsarProto::MessageMetadataView chunk(const String & uuid, Int32 chunk_id, Int32 num_chunks, Int32 total, UInt64 publish_time = 1'000'000)
    {
        PulsarProto::MessageMetadataView metadata;
        metadata.set_uuid(uuid);
        metadata.set_chunk_id(chunk_id);
        metadata.set_num_chunks_from_msg(num_chunks);
        metadata.set_total_chunk_msg_size(total);
        metadata.set_publish_time(publish_time);
        return metadata;
    }

    ChunkAssembler::Result add(const PulsarProto::MessageMetadataView & metadata, UInt64 entry, std::string_view payload)
    {
        ChunkAssembler::Result result;
        assembler.add(metadata, ChunkId{1, entry}, payload, max_message_size, limits, result);
        return result;
    }
};

using Ids = VectorWithMemoryTracking<ChunkId>;

Ids ids(std::initializer_list<UInt64> entries)
{
    Ids res;
    for (UInt64 entry : entries)
        res.push_back({1, entry});
    return res;
}

void expectNothing(const ChunkAssembler::Result & result)
{
    EXPECT_TRUE(result.ack.empty());
    EXPECT_FALSE(result.ack_corrupted);
    EXPECT_TRUE(result.redeliver.empty());
    EXPECT_FALSE(result.message);
}

}

TEST(PulsarChunks, Completion)
{
    ChunkTest t;
    expectNothing(t.add(t.chunk("u", 0, 3, 6), 1, "ab"));
    expectNothing(t.add(t.chunk("u", 1, 3, 6), 2, "cd"));
    auto result = t.add(t.chunk("u", 2, 3, 6), 3, "ef");
    ASSERT_TRUE(result.message);
    EXPECT_EQ(*result.message, "abcdef");
    EXPECT_EQ(result.message_chunks, ids({1, 2, 3}));
    EXPECT_EQ(t.assembler.pendingMessages(), 0);
}

TEST(PulsarChunks, CompressedThenSliced)
{
    /// The producer compresses the whole payload and then slices it into chunks.
    const String data(10000, 'y');
    String compressed = compress(PulsarProto::ZSTD, data);
    ASSERT_GE(compressed.size(), 3);
    size_t part = compressed.size() / 3;
    String parts[3] = {compressed.substr(0, part), compressed.substr(part, part), compressed.substr(2 * part)};

    ChunkTest t;
    const Int32 total = static_cast<Int32>(compressed.size());
    ChunkAssembler::Result result;
    for (Int32 i = 0; i < 3; ++i)
    {
        auto metadata = t.chunk("z", i, 3, total);
        metadata.set_compression(PulsarProto::ZSTD);
        metadata.set_uncompressed_size(static_cast<UInt32>(data.size()));
        /// A single chunk does not decompress.
        auto owner = std::make_shared<StringWithMemoryTracking>(parts[i]);
        EXPECT_TRUE(decodePayload(metadata, *owner, owner, ChunkTest::max_message_size, false, t.limits).error);
        result = t.add(metadata, i + 1, parts[i]);
        if (i < 2)
            expectNothing(result);
    }
    ASSERT_TRUE(result.message);
    auto assembled = std::make_shared<StringWithMemoryTracking>(*result.message);
    auto metadata = t.chunk("z", 2, 3, total);
    metadata.set_compression(PulsarProto::ZSTD);
    metadata.set_uncompressed_size(static_cast<UInt32>(data.size()));
    auto decoded = decodePayload(metadata, *assembled, assembled, 0, false, t.limits);
    ASSERT_FALSE(decoded.error);
    EXPECT_EQ(decoded.batch.data, data);
}

TEST(PulsarChunks, RedeliveredFirstChunk)
{
    ChunkTest t;
    t.add(t.chunk("u", 0, 3, 6), 1, "ab");
    t.add(t.chunk("u", 1, 3, 6), 2, "cd");
    /// The same first chunk again: the old context is dropped without acknowledgements.
    expectNothing(t.add(t.chunk("u", 0, 3, 6), 1, "ab"));
    t.add(t.chunk("u", 1, 3, 6), 2, "cd");
    auto result = t.add(t.chunk("u", 2, 3, 6), 3, "ef");
    ASSERT_TRUE(result.message);
    EXPECT_EQ(*result.message, "abcdef");
}

TEST(PulsarChunks, ReusedUuid)
{
    ChunkTest t;
    t.add(t.chunk("u", 0, 3, 6), 1, "ab");
    t.add(t.chunk("u", 1, 3, 6), 2, "cd");
    /// A different first chunk with the same uuid: the old chunks are acknowledged.
    auto result = t.add(t.chunk("u", 0, 2, 4), 5, "gh");
    EXPECT_EQ(result.ack, ids({1, 2}));
    EXPECT_FALSE(result.message);
    result = t.add(t.chunk("u", 1, 2, 4), 6, "ij");
    ASSERT_TRUE(result.message);
    EXPECT_EQ(*result.message, "ghij");
}

TEST(PulsarChunks, Eviction)
{
    ChunkTest t;
    for (UInt64 i = 0; i < ChunkAssembler::MAX_PENDING_MESSAGES; ++i)
        expectNothing(t.add(t.chunk(fmt::format("u{}", i), 0, 2, 4), i + 1, "ab"));
    EXPECT_EQ(t.assembler.pendingMessages(), 10);
    /// The 11th evicts the oldest, whose chunks are redelivered.
    auto result = t.add(t.chunk("u10", 0, 2, 4), 100, "ab");
    EXPECT_EQ(result.redeliver, ids({1}));
    EXPECT_TRUE(result.ack.empty());
    EXPECT_EQ(t.assembler.pendingMessages(), 10);
    /// A later chunk of the evicted message is redelivered too (abandoned with redelivery).
    t.now += 2 * ChunkAssembler::EXPIRY_MS;
    result = t.add(t.chunk("u0", 1, 2, 4, t.now), 200, "cd");
    EXPECT_EQ(result.redeliver, ids({200}));
}

TEST(PulsarChunks, UncachedChunk)
{
    ChunkTest t;
    /// Fresh: redelivered.
    auto result = t.add(t.chunk("a", 1, 2, 4), 1, "cd");
    EXPECT_EQ(result.redeliver, ids({1}));
    EXPECT_TRUE(result.ack.empty());
    /// Expired by its publish time: acknowledged.
    result = t.add(t.chunk("b", 1, 2, 4, t.now - ChunkAssembler::EXPIRY_MS - 1), 2, "cd");
    EXPECT_EQ(result.ack, ids({2}));
    EXPECT_TRUE(result.redeliver.empty());

    /// Abandoned with acknowledgement (an expired context): acknowledged.
    t.add(t.chunk("c", 0, 3, 6), 3, "ab");
    t.now += ChunkAssembler::EXPIRY_MS + 1;
    ChunkAssembler::Result expired;
    t.assembler.expire(expired);
    EXPECT_EQ(expired.ack, ids({3}));
    result = t.add(t.chunk("c", 1, 3, 6, t.now), 4, "cd");
    EXPECT_EQ(result.ack, ids({4}));
    EXPECT_TRUE(result.redeliver.empty());
}

TEST(PulsarChunks, Duplicates)
{
    ChunkTest t;
    t.add(t.chunk("u", 0, 3, 6), 1, "ab");
    t.add(t.chunk("u", 1, 3, 6), 2, "cd");
    /// A redelivered chunk is ignored.
    expectNothing(t.add(t.chunk("u", 1, 3, 6), 2, "cd"));
    /// A different entry with an old chunk id is acknowledged.
    auto result = t.add(t.chunk("u", 1, 3, 6), 9, "cd");
    EXPECT_EQ(result.ack, ids({9}));
    result = t.add(t.chunk("u", 2, 3, 6), 3, "ef");
    ASSERT_TRUE(result.message);
    EXPECT_EQ(*result.message, "abcdef");
}

TEST(PulsarChunks, Gap)
{
    ChunkTest t;
    t.add(t.chunk("u", 0, 4, 8), 1, "ab");
    /// Fresh: the context and this chunk are redelivered.
    auto result = t.add(t.chunk("u", 2, 4, 8), 3, "ef");
    EXPECT_EQ(result.redeliver, ids({1, 3}));
    EXPECT_EQ(t.assembler.pendingMessages(), 0);
    /// The next chunk of the abandoned message is redelivered.
    result = t.add(t.chunk("u", 3, 4, 8), 4, "gh");
    EXPECT_EQ(result.redeliver, ids({4}));

    /// Expired: the context is redelivered, this chunk is acknowledged.
    t.add(t.chunk("v", 0, 4, 8), 10, "ab");
    result = t.add(t.chunk("v", 2, 4, 8, t.now - ChunkAssembler::EXPIRY_MS - 1), 12, "ef");
    EXPECT_EQ(result.redeliver, ids({10}));
    EXPECT_EQ(result.ack, ids({12}));
}

TEST(PulsarChunks, InvalidLayout)
{
    ChunkTest t;
    const std::pair<Int32, Int32> layouts[] = {
        {0, 4},  /// no chunks
        {2, 0},  /// no size
        {5, 4},  /// more chunks than bytes
        {2, 201},  /// bigger than the chunks can carry
    };
    UInt64 entry = 1;
    for (const auto & [num_chunks, total] : layouts)
    {
        auto result = t.add(t.chunk("u", 0, num_chunks, total), entry, "a");
        ASSERT_TRUE(result.ack_corrupted);
        EXPECT_EQ(*result.ack_corrupted, (ChunkId{1, entry}));
        ++entry;
    }
    /// Above the uncompressed size limit, with a big maximum message size.
    {
        ChunkAssembler::Result result;
        t.assembler.add(t.chunk("u", 0, 2, THREE_GIB / 2), {1, 50}, "a", THREE_GIB, t.limits, result);
        EXPECT_TRUE(result.ack_corrupted);
    }
    /// Without uuid.
    {
        PulsarProto::MessageMetadataView metadata = t.chunk("x", 0, 2, 4);
        metadata.clear_uuid();
        EXPECT_TRUE(t.add(metadata, 51, "ab").ack_corrupted);
    }
    /// The first chunk is bigger than the total.
    EXPECT_TRUE(t.add(t.chunk("w", 0, 2, 2), 52, "abc").ack_corrupted);
    /// The chunks of the discarded message are acknowledged later (abandoned with acknowledgement).
    auto result = t.add(t.chunk("w", 1, 2, 2), 53, "x");
    EXPECT_EQ(result.ack, ids({53}));

    /// The chunks exceed the total: the context and this chunk are discarded.
    t.add(t.chunk("o", 0, 3, 5), 60, "ab");
    result = t.add(t.chunk("o", 1, 3, 5), 61, "cdef");
    EXPECT_EQ(result.ack, ids({60}));
    EXPECT_EQ(result.ack_corrupted, (ChunkId{1, 61}));
    EXPECT_EQ(t.assembler.pendingMessages(), 0);

    /// The chunks are smaller than the total.
    t.add(t.chunk("s", 0, 2, 5), 70, "ab");
    result = t.add(t.chunk("s", 1, 2, 5), 71, "c");
    EXPECT_FALSE(result.message);
    EXPECT_EQ(result.ack, ids({70}));
    EXPECT_EQ(result.ack_corrupted, (ChunkId{1, 71}));
}

TEST(PulsarChunks, Expiry)
{
    ChunkTest t;
    t.add(t.chunk("u", 0, 3, 6), 1, "ab");
    t.now += ChunkAssembler::EXPIRY_MS / 2;
    t.add(t.chunk("u", 1, 3, 6), 2, "cd");
    /// Counted from the last chunk.
    t.now += ChunkAssembler::EXPIRY_MS / 2 + 1;
    ChunkAssembler::Result result;
    t.assembler.expire(result);
    EXPECT_TRUE(result.ack.empty());
    t.now += ChunkAssembler::EXPIRY_MS;
    t.assembler.expire(result);
    EXPECT_EQ(result.ack, ids({1, 2}));
    EXPECT_EQ(t.assembler.pendingMessages(), 0);
}

#endif
