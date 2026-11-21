#include <Storages/Pulsar/Client/Protocol.h>

#if USE_PULSAR

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>
#include <base/unaligned.h>

#include <crc32c/crc32c.h>
#include <lz4.h>
#include <snappy.h>
#include <zlib.h>
#include <zstd.h>

#include <fmt/format.h>

#include <algorithm>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace Pulsar
{

namespace
{

constexpr UInt16 MAGIC_CRC32C = 0x0e01;
constexpr UInt16 MAGIC_BROKER_ENTRY_METADATA = 0x0e02;

class Reader
{
public:
    explicit Reader(std::string_view data_) : data(data_) {}

    [[nodiscard]] size_t remaining() const { return data.size() - pos; }
    [[nodiscard]] std::string_view rest() const { return data.substr(pos); }

    bool peekUInt16(UInt16 & value) const
    {
        if (remaining() < 2)
            return false;
        value = unalignedLoadBigEndian<UInt16>(data.data() + pos);
        return true;
    }

    UInt32 readUInt32(const char * what)
    {
        need(4, what);
        UInt32 value = unalignedLoadBigEndian<UInt32>(data.data() + pos);
        pos += 4;
        return value;
    }

    void skip(size_t size, const char * what)
    {
        need(size, what);
        pos += size;
    }

    std::string_view read(size_t size, const char * what)
    {
        need(size, what);
        std::string_view res = data.substr(pos, size);
        pos += size;
        return res;
    }

private:
    void need(size_t size, const char * what) const
    {
        if (size > remaining())
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                "Malformed Pulsar frame: {} needs {} bytes, but only {} remain", what, size, remaining());
    }

    std::string_view data;
    size_t pos = 0;
};

void appendUInt32(StringWithMemoryTracking & out, UInt32 value)
{
    char buf[4];
    unalignedStoreBigEndian<UInt32>(buf, value);
    out.append(buf, 4);
}

void appendUInt16(StringWithMemoryTracking & out, UInt16 value)
{
    char buf[2];
    unalignedStoreBigEndian<UInt16>(buf, value);
    out.append(buf, 2);
}

void appendMessage(StringWithMemoryTracking & out, const google::protobuf::MessageLite & message, size_t size)
{
    size_t offset = out.size();
    out.resize(offset + size);
    if (!message.SerializeToArray(out.data() + offset, static_cast<int>(size)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot serialize a Pulsar command");
}

size_t checkedSize(const google::protobuf::MessageLite & message)
{
    size_t size = message.ByteSizeLong();
    if (size > std::numeric_limits<Int32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Pulsar command of size {} is too large", size);
    return size;
}

bool decompress(Int32 codec, std::string_view in, char * out, size_t size)
{
    switch (codec)
    {
        case PulsarProto::LZ4:
        {
            if (in.size() > std::numeric_limits<int>::max())
                return false;
            int res = LZ4_decompress_safe(in.data(), out, static_cast<int>(in.size()), static_cast<int>(size));
            return res >= 0 && static_cast<size_t>(res) == size;
        }
        case PulsarProto::ZSTD:
        {
            size_t res = ZSTD_decompress(out, size, in.data(), in.size());
            return !ZSTD_isError(res) && res == size;
        }
        case PulsarProto::ZLIB:
        {
            if (in.size() > std::numeric_limits<uInt>::max())
                return false;
            z_stream stream{};
            int res = inflateInit(&stream);
            if (res == Z_MEM_ERROR)
                throw std::bad_alloc();
            if (res != Z_OK)
                return false;
            stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(in.data()));
            stream.avail_in = static_cast<uInt>(in.size());
            stream.next_out = reinterpret_cast<Bytef *>(out);
            stream.avail_out = static_cast<uInt>(size);

            /// Block by block, so that `data_type` tells where the input ended. One byte past the size catches longer output.
            Bytef extra = 0;
            int data_type = 0;
            while (true)
            {
                if (stream.avail_out == 0 && stream.total_out == size)
                {
                    stream.next_out = &extra;
                    stream.avail_out = 1;
                }
                res = inflate(&stream, Z_BLOCK);
                if (res != Z_OK || stream.total_out > size)
                    break;
                data_type = stream.data_type;
            }
            const uLong total_out = stream.total_out;
            const uInt remaining_in = stream.avail_in;
            inflateEnd(&stream);
            if (res == Z_MEM_ERROR)
                throw std::bad_alloc();

            /// The Java client ends the stream with a sync flush, without the final block and the checksum: accept it
            /// when all input is consumed at a block boundary (128) outside of the final block (64), as the C++ client does.
            const bool complete = res == Z_STREAM_END
                || (res == Z_BUF_ERROR && remaining_in == 0 && (data_type & 128) != 0 && (data_type & 64) == 0);
            return complete && total_out == size;
        }
        case PulsarProto::SNAPPY:
        {
            size_t declared = 0;
            if (!snappy::GetUncompressedLength(in.data(), in.size(), &declared) || declared != size)
                return false;
            return snappy::RawUncompress(in.data(), in.size(), out);
        }
        default:
            return false;
    }
}

}


size_t decodeFrameSize(const char * data, const Limits & limits)
{
    size_t size = unalignedLoadBigEndian<UInt32>(data);
    size_t max_size = limits.max_message_size + FRAME_PADDING;
    if (size < 4 || size > max_size)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Malformed Pulsar frame: its size {} is not between 4 and {}", size, max_size);
    return size;
}

StringWithMemoryTracking encodeCommand(const PulsarProto::BaseCommand & command)
{
    size_t command_size = checkedSize(command);
    StringWithMemoryTracking out;
    out.reserve(8 + command_size);
    appendUInt32(out, static_cast<UInt32>(4 + command_size));
    appendUInt32(out, static_cast<UInt32>(command_size));
    appendMessage(out, command, command_size);
    return out;
}

StringWithMemoryTracking encodeSendFrame(
    const PulsarProto::BaseCommand & command, const PulsarProto::MessageMetadata & metadata, std::string_view payload)
{
    size_t command_size = checkedSize(command);
    size_t metadata_size = checkedSize(metadata);
    size_t total = 4 + command_size + 2 + 4 + 4 + metadata_size + payload.size();
    if (total > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Pulsar message of size {} is too large", payload.size());

    StringWithMemoryTracking out;
    out.reserve(4 + total);
    appendUInt32(out, static_cast<UInt32>(total));
    appendUInt32(out, static_cast<UInt32>(command_size));
    appendMessage(out, command, command_size);
    appendUInt16(out, MAGIC_CRC32C);
    size_t checksum_offset = out.size();
    appendUInt32(out, 0);
    size_t checksummed_offset = out.size();
    appendUInt32(out, static_cast<UInt32>(metadata_size));
    appendMessage(out, metadata, metadata_size);
    out.append(payload);
    UInt32 checksum = crc32c::Crc32c(out.data() + checksummed_offset, out.size() - checksummed_offset);
    unalignedStoreBigEndian<UInt32>(out.data() + checksum_offset, checksum);
    return out;
}

void decodeFrame(std::string_view body, const Limits & limits, Frame & frame)
{
    Reader reader(body);
    size_t command_size = reader.readUInt32("the command size");
    if (command_size > limits.max_command_size)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Malformed Pulsar frame: the command size {} is bigger than {}", command_size, limits.max_command_size);
    std::string_view command = reader.read(command_size, "the command");

    frame.command.Clear();
    if (!frame.command.ParsePartialFromArray(command.data(), static_cast<int>(command.size())))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Malformed Pulsar frame: cannot parse the command");

    frame.tail = reader.rest();
    if (!frame.tail.empty() && frame.command.has_type() && frame.command.type() != PulsarProto::BaseCommand::MESSAGE)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Malformed Pulsar frame: {} bytes after a command of type {}", frame.tail.size(), static_cast<int>(frame.command.type()));
}

void decodeEntry(std::string_view tail, Entry & entry)
{
    Reader reader(tail);
    entry.checksum_mismatch = false;

    UInt16 magic = 0;
    if (reader.peekUInt16(magic) && magic == MAGIC_BROKER_ENTRY_METADATA)
    {
        reader.skip(2, "the broker entry metadata magic");
        size_t size = reader.readUInt32("the broker entry metadata size");
        std::string_view data = reader.read(size, "the broker entry metadata");
        PulsarProto::BrokerEntryMetadata broker_entry_metadata;
        if (!broker_entry_metadata.ParsePartialFromArray(data.data(), static_cast<int>(data.size())))
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Malformed Pulsar frame: cannot parse the broker entry metadata");
    }

    std::optional<UInt32> checksum;
    if (reader.peekUInt16(magic) && magic == MAGIC_CRC32C)
    {
        reader.skip(2, "the checksum magic");
        checksum = reader.readUInt32("the checksum");
    }

    std::string_view checksummed = reader.rest();
    size_t metadata_size = reader.readUInt32("the metadata size");
    std::string_view metadata = reader.read(metadata_size, "the metadata");

    if (checksum && crc32c::Crc32c(checksummed.data(), checksummed.size()) != *checksum)
    {
        entry.metadata.Clear();
        entry.payload = {};
        entry.checksum_mismatch = true;
        return;
    }

    entry.metadata.Clear();
    if (!entry.metadata.ParsePartialFromArray(metadata.data(), static_cast<int>(metadata.size())))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "Malformed Pulsar frame: cannot parse the message metadata");
    entry.payload = reader.rest();
}

DecodedPayload decodePayload(
    const PulsarProto::MessageMetadataView & metadata,
    std::string_view payload,
    SharedBuffer owner,
    size_t max_message_size,
    bool check_compressed_size,
    const Limits & limits)
{
    DecodedPayload res;
    Int32 codec = metadata.compression();
    if (codec == PulsarProto::NONE)
    {
        res.batch.buffer = std::move(owner);
        res.batch.data = payload;
    }
    else
    {
        size_t size = metadata.uncompressed_size();
        if ((check_compressed_size && payload.size() > max_message_size) || size > limits.max_uncompressed_size)
        {
            res.error = PulsarProto::CommandAck::UncompressedSizeCorruption;
            return res;
        }
        if (codec != PulsarProto::LZ4 && codec != PulsarProto::ZLIB && codec != PulsarProto::ZSTD && codec != PulsarProto::SNAPPY)
        {
            res.error = PulsarProto::CommandAck::DecompressionError;
            return res;
        }

        auto buffer = std::make_shared<StringWithMemoryTracking>();
        buffer->resize(size);
        if (!decompress(codec, payload, buffer->data(), size))
        {
            res.error = PulsarProto::CommandAck::DecompressionError;
            return res;
        }
        res.batch.data = *buffer;
        res.batch.buffer = std::move(buffer);
    }

    if (!metadata.has_num_messages_in_batch())
        return res;

    /// Every entry takes at least its 4-byte size.
    Int32 declared = metadata.num_messages_in_batch();
    if (declared < 1 || static_cast<size_t>(declared) > res.batch.data.size() / 4)
    {
        res.error = PulsarProto::CommandAck::BatchDeSerializeError;
        return res;
    }

    /// Validate every entry in one pass, without allocating per entry.
    PulsarProto::SingleMessageMetadataView entry_metadata;
    std::string_view rest = res.batch.data;
    for (Int32 i = 0; i < declared; ++i)
    {
        bool ok = rest.size() >= 4;
        size_t size = ok ? unalignedLoadBigEndian<UInt32>(rest.data()) : 0;
        ok = ok && size <= rest.size() - 4;
        if (ok)
        {
            entry_metadata.Clear();
            ok = entry_metadata.ParsePartialFromArray(rest.data() + 4, static_cast<int>(size)) && entry_metadata.has_payload_size()
                && entry_metadata.payload_size() >= 0 && static_cast<size_t>(entry_metadata.payload_size()) <= rest.size() - 4 - size;
        }
        if (!ok)
        {
            res.error = PulsarProto::CommandAck::BatchDeSerializeError;
            return res;
        }
        rest.remove_prefix(4 + size + entry_metadata.payload_size());
    }
    res.batch.num_entries = declared;
    return res;
}


BatchCursor::BatchCursor(Batch batch_, const PulsarProto::MessageMetadataView & metadata)
    : batch(std::move(batch_))
{
    if (batch.num_entries)
    {
        count = *batch.num_entries;
        return;
    }
    count = 1;
    if (metadata.has_partition_key())
        partition_key = metadata.partition_key();
    if (metadata.has_ordering_key())
        ordering_key = metadata.ordering_key();
    if (metadata.has_event_time())
        event_time = metadata.event_time();
}

const MessageFields & BatchCursor::next()
{
    chassert(index < count);
    fields = {};
    if (!batch.num_entries)
    {
        fields.payload = batch.data;
        if (partition_key)
            fields.partition_key = *partition_key;
        if (ordering_key)
            fields.ordering_key = *ordering_key;
        fields.event_time = event_time;
        ++index;
        return fields;
    }

    /// The batch was validated by `decodePayload`.
    std::string_view rest = batch.data.substr(offset);
    size_t size = unalignedLoadBigEndian<UInt32>(rest.data());
    entry_metadata.Clear();
    [[maybe_unused]] bool parsed = entry_metadata.ParsePartialFromArray(rest.data() + 4, static_cast<int>(size));
    chassert(parsed);
    size_t payload_size = entry_metadata.payload_size();
    fields.payload = rest.substr(4 + size, payload_size);
    if (entry_metadata.has_partition_key())
        fields.partition_key = entry_metadata.partition_key();
    if (entry_metadata.has_ordering_key())
        fields.ordering_key = entry_metadata.ordering_key();
    if (entry_metadata.has_event_time())
        fields.event_time = entry_metadata.event_time();
    fields.batch_index = static_cast<Int32>(index);
    offset += 4 + size + payload_size;
    ++index;
    return fields;
}


ChunkAssembler::ChunkAssembler(Clock clock_)
    : clock(std::move(clock_))
{
}

ChunkAssembler::Context * ChunkAssembler::find(const String & uuid)
{
    for (auto & context : contexts)
        if (context.uuid == uuid)
            return &context;
    return nullptr;
}

void ChunkAssembler::remove(const String & uuid)
{
    std::erase_if(contexts, [&](const Context & context) { return context.uuid == uuid; });
}

void ChunkAssembler::rememberAbandoned(const String & uuid, bool redeliver)
{
    std::erase_if(abandoned, [&](const auto & entry) { return entry.first == uuid; });
    if (abandoned.size() >= MAX_ABANDONED_MESSAGES)
        abandoned.pop_front();
    abandoned.emplace_back(uuid, redeliver);
}

std::optional<bool> ChunkAssembler::abandonedDisposition(const String & uuid) const
{
    for (const auto & [abandoned_uuid, redeliver] : abandoned)
        if (abandoned_uuid == uuid)
            return redeliver;
    return std::nullopt;
}

void ChunkAssembler::discardMalformed(const String & uuid, ChunkId id, Result & result)
{
    LOG_ERROR(getLogger("PulsarClient"), "Discarding a malformed chunked message (uuid: {}, chunk at {}:{})", uuid, id.ledger_id, id.entry_id);
    if (Context * context = find(uuid))
    {
        for (const auto & chunk : context->chunks)
            if (!(chunk == id))
                result.ack.push_back(chunk);
        remove(uuid);
    }
    rememberAbandoned(uuid, false);
    result.ack_corrupted = id;
}

void ChunkAssembler::add(
    const PulsarProto::MessageMetadataView & metadata, ChunkId id, std::string_view payload, size_t max_message_size, const Limits & limits, Result & result)
{
    const String & uuid = metadata.uuid();
    const Int64 chunk_id = metadata.chunk_id();
    const UInt64 now = clock();
    const bool publish_expired = now > metadata.publish_time() + EXPIRY_MS;

    if (!metadata.has_uuid())
    {
        LOG_ERROR(getLogger("PulsarClient"), "Discarding a chunk without uuid at {}:{}", id.ledger_id, id.entry_id);
        result.ack_corrupted = id;
        return;
    }

    if (chunk_id == 0)
    {
        if (Context * context = find(uuid))
        {
            /// The same first chunk again is a redelivery: the old chunks will be redelivered too.
            /// Otherwise a producer reused the uuid, and the old chunks are acknowledged to avoid an acknowledgement hole.
            if (std::find(context->chunks.begin(), context->chunks.end(), id) == context->chunks.end())
                result.ack.insert(result.ack.end(), context->chunks.begin(), context->chunks.end());
            remove(uuid);
        }

        const Int64 num_chunks = metadata.num_chunks_from_msg();
        const Int64 total = metadata.total_chunk_msg_size();
        if (num_chunks < 1 || total < 1 || num_chunks > total || static_cast<UInt64>(total) > limits.max_uncompressed_size
            || static_cast<UInt64>(total) > static_cast<UInt64>(num_chunks) * max_message_size || payload.size() > static_cast<UInt64>(total))
        {
            discardMalformed(uuid, id, result);
            return;
        }

        if (contexts.size() >= MAX_PENDING_MESSAGES)
        {
            Context & oldest = contexts.front();
            LOG_WARNING(getLogger("PulsarClient"), "Requesting redelivery of the incomplete chunked message {}, because {} chunked messages are pending",
                oldest.uuid, contexts.size());
            result.redeliver.insert(result.redeliver.end(), oldest.chunks.begin(), oldest.chunks.end());
            String evicted = oldest.uuid;
            contexts.pop_front();
            rememberAbandoned(evicted, true);
        }

        std::erase_if(abandoned, [&](const auto & entry) { return entry.first == uuid; });
        Context & context = contexts.emplace_back();
        context.uuid = uuid;
        context.num_chunks = num_chunks;
        context.total_size = total;
        context.buffer.append(payload);
        context.chunks.push_back(id);
        context.last_received_ms = now;
        return;
    }

    Context * context = find(uuid);
    if (!context)
    {
        /// The first chunk was not received or the message was dropped.
        bool redeliver = true;
        if (publish_expired)
            redeliver = false;
        else if (auto disposition = abandonedDisposition(uuid))
            redeliver = *disposition;
        LOG_ERROR(getLogger("PulsarClient"), "Received an uncached chunk {} of {} at {}:{}", chunk_id, uuid, id.ledger_id, id.entry_id);
        (redeliver ? result.redeliver : result.ack).push_back(id);
        return;
    }

    const Int64 last_chunk_id = static_cast<Int64>(context->chunks.size()) - 1;
    if (chunk_id != last_chunk_id + 1)
    {
        if (chunk_id <= last_chunk_id)
        {
            /// A duplicate: a redelivered chunk is ignored, a different entry with the same chunk id is acknowledged.
            if (std::find(context->chunks.begin(), context->chunks.end(), id) == context->chunks.end())
                result.ack.push_back(id);
            return;
        }

        LOG_WARNING(getLogger("PulsarClient"), "Received chunk {} of {} after chunk {}", chunk_id, uuid, last_chunk_id);
        result.redeliver.insert(result.redeliver.end(), context->chunks.begin(), context->chunks.end());
        remove(uuid);
        rememberAbandoned(uuid, true);
        (publish_expired ? result.ack : result.redeliver).push_back(id);
        return;
    }

    if (payload.size() > context->total_size - context->buffer.size())
    {
        discardMalformed(uuid, id, result);
        return;
    }

    context->buffer.append(payload);
    context->chunks.push_back(id);
    context->last_received_ms = now;
    if (context->chunks.size() < context->num_chunks)
        return;

    if (context->buffer.size() != context->total_size)
    {
        discardMalformed(uuid, id, result);
        return;
    }

    result.message = std::move(context->buffer);
    result.message_chunks = std::move(context->chunks);
    remove(uuid);
}

void ChunkAssembler::expire(Result & result)
{
    const UInt64 now = clock();
    while (true)
    {
        auto it = std::find_if(contexts.begin(), contexts.end(), [&](const Context & context) { return now > context.last_received_ms + EXPIRY_MS; });
        if (it == contexts.end())
            return;
        LOG_INFO(getLogger("PulsarClient"), "Removing the expired incomplete chunked message {}", it->uuid);
        result.ack.insert(result.ack.end(), it->chunks.begin(), it->chunks.end());
        String uuid = it->uuid;
        contexts.erase(it);
        rememberAbandoned(uuid, false);
    }
}


String normalizeTopicName(std::string_view name)
{
    std::string_view domain = "persistent";
    std::string_view rest = name;
    if (auto pos = name.find("://"); pos != std::string_view::npos)
    {
        domain = name.substr(0, pos);
        rest = name.substr(pos + 3);
        if (domain != "persistent" && domain != "non-persistent")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Pulsar topic name '{}': unknown domain '{}'", name, domain);
    }

    size_t parts = std::count(rest.begin(), rest.end(), '/') + 1;
    bool has_empty_part = rest.empty() || rest.starts_with('/') || rest.ends_with('/') || rest.contains("//");
    if (has_empty_part || (parts != 3 && (parts != 1 || name.contains("://"))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid Pulsar topic name '{}': it must be `topic`, `tenant/namespace/topic` or `<domain>://tenant/namespace/topic`", name);

    if (parts == 1)
        return fmt::format("{}://public/default/{}", domain, rest);
    return fmt::format("{}://{}", domain, rest);
}

String partitionTopicName(const String & topic, size_t partition)
{
    return fmt::format("{}-partition-{}", topic, partition);
}

ServiceUrl parseServiceUrl(std::string_view url)
{
    ServiceUrl res;
    std::string_view rest = url;
    UInt16 default_port = 0;
    if (rest.starts_with("pulsar+ssl://"))
    {
        rest.remove_prefix(strlen("pulsar+ssl://"));
        default_port = 6651;
        res.tls = true;
    }
    else if (rest.starts_with("pulsar://"))
    {
        rest.remove_prefix(strlen("pulsar://"));
        default_port = 6650;
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Pulsar service URL '{}': it must start with `pulsar://` or `pulsar+ssl://`", url);

    if (auto slash = rest.find('/'); slash != std::string_view::npos)
        rest = rest.substr(0, slash);

    while (true)
    {
        size_t comma = rest.find(',');
        std::string_view address = rest.substr(0, comma);
        while (!address.empty() && address.front() == ' ')
            address.remove_prefix(1);
        while (!address.empty() && address.back() == ' ')
            address.remove_suffix(1);
        if (address.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Pulsar service URL '{}': empty broker address", url);
        auto [host, port] = parseAddress(String(address), default_port);
        res.hosts.emplace_back(std::move(host), port);
        if (comma == std::string_view::npos)
            break;
        rest.remove_prefix(comma + 1);
    }
    return res;
}

}

}

#endif
