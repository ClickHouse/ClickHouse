#include <Storages/Kafka/Clients/Protocol/Records.h>

#include <Storages/Kafka/Clients/Protocol/Crc32c.h>

#include <algorithm>

namespace Kafka::Protocol
{

namespace
{

void encodeVarBytes(Writer & w, const std::optional<Bytes> & data)
{
    if (!data)
    {
        w.writeVarint(-1);
        return;
    }
    if (data->size() > static_cast<size_t>(INT32_MAX))
        throw Exception(ErrorKind::Protocol, "Record field too long for varint length: {}", data->size());
    w.writeVarint(static_cast<Int32>(data->size()));
    w.writeRaw(*data);
}

std::optional<Bytes> decodeVarBytes(Reader & r)
{
    const Int32 len = r.readVarint();
    if (len < 0)
        return std::nullopt;
    return Bytes(r.readRaw(static_cast<size_t>(len)));
}

/// Wire sizes of the varint primitives, so records encode in place without a
/// per-record scratch buffer (which cost one allocation + copy per record).
size_t unsignedVarintSize(UInt64 value)
{
    size_t n = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++n;
    }
    return n;
}

size_t varintSize(Int32 value)
{
    return unsignedVarintSize((static_cast<UInt32>(value) << 1) ^ static_cast<UInt32>(value >> 31));
}

size_t varlongSize(Int64 value)
{
    return unsignedVarintSize((static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63));
}

size_t varBytesSize(const std::optional<Bytes> & data)
{
    if (!data)
        return varintSize(-1);
    return varintSize(static_cast<Int32>(data->size())) + data->size();
}

size_t recordBodySize(const BatchRecord & record)
{
    size_t size = 1; /// attributes
    size += varlongSize(record.timestamp_delta);
    size += varintSize(record.offset_delta);
    size += varBytesSize(record.key);
    size += varBytesSize(record.value);
    size += varintSize(static_cast<Int32>(record.headers.size()));
    for (const auto & header : record.headers)
    {
        size += varintSize(static_cast<Int32>(header.key.size())) + header.key.size();
        size += varBytesSize(header.value);
    }
    return size;
}

void encodeRecord(Writer & w, const BatchRecord & record)
{
    const size_t body_size = recordBodySize(record);
    if (body_size > static_cast<size_t>(INT32_MAX))
        throw Exception(ErrorKind::Protocol, "Record too long for varint length: {}", body_size);
    w.writeVarint(static_cast<Int32>(body_size));
    w.writeInt8(record.attributes);
    w.writeVarlong(record.timestamp_delta);
    w.writeVarint(record.offset_delta);
    encodeVarBytes(w, record.key);
    encodeVarBytes(w, record.value);
    w.writeVarint(static_cast<Int32>(record.headers.size()));
    for (const auto & header : record.headers)
    {
        w.writeVarint(static_cast<Int32>(header.key.size()));
        w.writeRaw(header.key);
        encodeVarBytes(w, header.value);
    }
}

BatchRecord decodeRecord(Reader & r)
{
    const Int32 length = r.readVarint();
    if (length < 0)
        throw Exception(ErrorKind::Protocol, "Negative record length: {}", length);
    Reader body(r.readRaw(static_cast<size_t>(length)));

    BatchRecord record;
    record.attributes = body.readInt8();
    record.timestamp_delta = body.readVarlong();
    record.offset_delta = body.readVarint();
    record.key = decodeVarBytes(body);
    record.value = decodeVarBytes(body);

    const Int32 header_count = body.readVarint();
    if (header_count < 0)
        throw Exception(ErrorKind::Protocol, "Negative record header count: {}", header_count);
    record.headers.reserve(static_cast<size_t>(header_count));
    for (Int32 i = 0; i < header_count; ++i)
    {
        const Int32 key_len = body.readVarint();
        if (key_len < 0)
            throwProtocol("Null record header key");
        RecordHeader header;
        header.key = String(body.readRaw(static_cast<size_t>(key_len)));
        header.value = decodeVarBytes(body);
        record.headers.push_back(std::move(header));
    }
    if (!body.empty())
        throwProtocol("Trailing bytes after record body");
    return record;
}

}

size_t recordWireSize(const BatchRecord & record)
{
    const size_t body = recordBodySize(record);
    return varintSize(static_cast<Int32>(body)) + body;
}

const char * compressionTypeName(CompressionType type)
{
    switch (type)
    {
        case CompressionType::None: return "none";
        case CompressionType::Gzip: return "gzip";
        case CompressionType::Snappy: return "snappy";
        case CompressionType::Lz4: return "lz4";
        case CompressionType::Zstd: return "zstd";
    }
    return "unknown";
}

RecordBatch RecordBatch::make(Int64 base_offset, std::vector<BatchRecord> records)
{
    RecordBatch batch;
    batch.base_offset = base_offset;
    batch.last_offset_delta = records.empty() ? 0 : static_cast<Int32>(records.size() - 1);
    if (!records.empty())
    {
        Int64 max_delta = 0;
        for (const auto & record : records)
            max_delta = std::max(max_delta, record.timestamp_delta);
        batch.first_timestamp = records.front().timestamp_delta;
        batch.max_timestamp = batch.first_timestamp + max_delta;
    }
    batch.records = std::move(records);
    return batch;
}

void RecordBatch::encode(Writer & w) const
{
    w.writeInt64(base_offset);
    const size_t batch_length_pos = w.size();
    w.writeInt32(0); /// batchLength placeholder
    w.writeInt32(partition_leader_epoch);
    w.writeInt8(record_magic_v2);
    const size_t crc_pos = w.size();
    w.writeUInt32(0); /// crc placeholder

    const size_t crc_body_start = w.size();
    w.writeInt16(attributes);
    w.writeInt32(last_offset_delta);
    w.writeInt64(first_timestamp);
    w.writeInt64(max_timestamp);
    w.writeInt64(producer_id);
    w.writeInt16(producer_epoch);
    w.writeInt32(base_sequence);
    w.writeInt32(static_cast<Int32>(records.size()));
    for (const auto & record : records)
        encodeRecord(w, record);

    const size_t batch_end = w.size();
    const size_t batch_length = batch_end - (batch_length_pos + 4);
    if (batch_length > static_cast<size_t>(INT32_MAX))
        throw Exception(ErrorKind::Protocol, "BatchLength exceeds i32: {}", batch_length);
    w.overwriteInt32At(batch_length_pos, static_cast<Int32>(batch_length));

    const UInt32 crc = crc32c(w.view().substr(crc_body_start, batch_end - crc_body_start));
    w.overwriteUInt32At(crc_pos, crc);
}

RecordBatch RecordBatch::decode(Reader & r)
{
    RecordBatch batch;
    batch.base_offset = r.readInt64();
    const Int32 batch_length = r.readInt32();
    if (batch_length < 0)
        throw Exception(ErrorKind::Protocol, "Negative batchLength: {}", batch_length);
    /// batch_length covers everything after itself. Pull that exact slice so a
    /// corrupt batch cannot read into the next batch's bytes.
    Reader body(r.readRaw(static_cast<size_t>(batch_length)));

    batch.partition_leader_epoch = body.readInt32();
    const Int8 magic = body.readInt8();
    if (magic != record_magic_v2)
        throw Exception(ErrorKind::NotSupported, "Unsupported record batch magic {} (only v2)", magic);
    const UInt32 expected_crc = body.readUInt32();

    /// The CRC covers everything after the crc field. Capture the region as a
    /// view, verify, then parse the same view.
    const std::string_view crc_region = body.readRaw(body.remaining());
    const UInt32 computed = crc32c(crc_region);
    if (computed != expected_crc)
        throw Exception(
            ErrorKind::Protocol,
            "Record batch CRC mismatch: expected {}, computed {}",
            expected_crc, computed);

    Reader fields(crc_region);
    batch.attributes = fields.readInt16();
    if (batch.compressionType() != CompressionType::None)
        throw Exception(
            ErrorKind::NotSupported,
            "Compressed record batch (codec {}) is not supported in v1; produce uncompressed or add a decompressor",
            compressionTypeName(batch.compressionType()));
    batch.last_offset_delta = fields.readInt32();
    batch.first_timestamp = fields.readInt64();
    batch.max_timestamp = fields.readInt64();
    batch.producer_id = fields.readInt64();
    batch.producer_epoch = fields.readInt16();
    batch.base_sequence = fields.readInt32();

    const Int32 count = fields.readInt32();
    if (count < 0)
        throw Exception(ErrorKind::Protocol, "Negative record count: {}", count);
    batch.records.reserve(static_cast<size_t>(count));
    for (Int32 i = 0; i < count; ++i)
        batch.records.push_back(decodeRecord(fields));
    if (!fields.empty())
        throwProtocol("Trailing bytes after records in batch");
    return batch;
}

String encodeBatches(const std::vector<RecordBatch> & batches)
{
    Writer w;
    for (const auto & batch : batches)
        batch.encode(w);
    return w.take();
}

DecodedBatches decodeBatches(std::string_view blob)
{
    DecodedBatches out;
    Reader r(blob);
    while (!r.empty())
    {
        /// A complete batch needs at least baseOffset(8) + batchLength(4).
        if (r.remaining() < 12)
        {
            out.truncated = true;
            break;
        }
        /// Peek batchLength without consuming: bytes 8..12 from the cursor.
        Reader peek(blob.substr(r.position()));
        peek.skip(8);
        const Int32 batch_length = peek.readInt32();
        if (batch_length < 0)
            throw Exception(ErrorKind::Protocol, "Negative batchLength: {}", batch_length);
        if (r.remaining() < 12 + static_cast<size_t>(batch_length))
        {
            out.truncated = true;
            break;
        }
        out.batches.push_back(RecordBatch::decode(r));
    }
    return out;
}

}
