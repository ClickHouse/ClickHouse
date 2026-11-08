#include <Storages/Kafka/Clients/Protocol/Records.h>

#include <Storages/Kafka/Clients/Protocol/Compression.h>
#include <Storages/Kafka/Clients/Protocol/Crc32c.h>

#include <algorithm>
#include <unordered_set>

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

void RecordBatch::encode(Writer & w, CompressionType compression, int compression_level) const
{
    w.writeInt64(base_offset);
    const size_t batch_length_pos = w.size();
    w.writeInt32(0); /// batchLength placeholder
    w.writeInt32(partition_leader_epoch);
    w.writeInt8(record_magic_v2);
    const size_t crc_pos = w.size();
    w.writeUInt32(0); /// crc placeholder

    const size_t crc_body_start = w.size();
    /// Compression lives in attributes bits 0-2; the stored attributes must
    /// not carry a stale codec.
    w.writeInt16(static_cast<Int16>((attributes & ~0x07) | static_cast<Int16>(compression)));
    w.writeInt32(last_offset_delta);
    w.writeInt64(first_timestamp);
    w.writeInt64(max_timestamp);
    w.writeInt64(producer_id);
    w.writeInt16(producer_epoch);
    w.writeInt32(base_sequence);
    /// The record count stays uncompressed; only the records themselves are
    /// compressed (DefaultRecordBatch layout).
    w.writeInt32(static_cast<Int32>(records.size()));
    if (compression == CompressionType::None)
    {
        for (const auto & record : records)
            encodeRecord(w, record);
    }
    else
    {
        Writer scratch;
        for (const auto & record : records)
            encodeRecord(scratch, record);
        w.writeRaw(compress(compression, scratch.view(), compression_level));
    }

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
    batch.last_offset_delta = fields.readInt32();
    batch.first_timestamp = fields.readInt64();
    batch.max_timestamp = fields.readInt64();
    batch.producer_id = fields.readInt64();
    batch.producer_epoch = fields.readInt16();
    batch.base_sequence = fields.readInt32();

    /// The record count stays uncompressed; the records after it are the
    /// compressed section (DefaultRecordBatch layout).
    const Int32 count = fields.readInt32();
    if (count < 0)
        throw Exception(ErrorKind::Protocol, "Negative record count: {}", count);
    batch.records.reserve(static_cast<size_t>(count));

    const auto read_records = [&](Reader & records_reader)
    {
        for (Int32 i = 0; i < count; ++i)
            batch.records.push_back(decodeRecord(records_reader));
        if (!records_reader.empty())
            throwProtocol("Trailing bytes after records in batch");
    };

    if (batch.compressionType() == CompressionType::None)
    {
        read_records(fields);
    }
    else
    {
        const Bytes decompressed = decompress(batch.compressionType(), fields.readRaw(fields.remaining()));
        Reader records_reader(decompressed);
        read_records(records_reader);
    }
    return batch;
}

String encodeBatches(const std::vector<RecordBatch> & batches, CompressionType compression, int compression_level)
{
    Writer w;
    for (const auto & batch : batches)
        batch.encode(w, compression, compression_level);
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

AssembledRecords assembleFetchedRecords(
    std::string_view blob, Int64 min_offset, bool read_committed, std::vector<AbortedTransactionRange> aborted_transactions)
{
    AssembledRecords result;
    result.next_offset = min_offset;

    auto decoded = decodeBatches(blob);
    result.truncated = decoded.truncated;

    std::sort(
        aborted_transactions.begin(),
        aborted_transactions.end(),
        [](const AbortedTransactionRange & a, const AbortedTransactionRange & b) { return a.first_offset < b.first_offset; });
    size_t next_aborted = 0;
    std::unordered_set<Int64> aborted_producers;

    /// NOTE: no reserve() here on purpose — an exact-size reserve of a
    /// multi-MB vector per fetch keeps hitting the glibc mmap threshold
    /// (mmap/munmap + page faults every call, measured ~6% slower on small
    /// records); growth-doubling stays on the warm heap instead.
    for (auto & batch : decoded.batches)
    {
        result.next_offset = std::max(result.next_offset, batch.nextOffset());

        /// A producer id becomes "aborted" once the response lists an aborted
        /// range starting at or before this batch.
        if (read_committed)
            while (next_aborted < aborted_transactions.size() && aborted_transactions[next_aborted].first_offset <= batch.base_offset)
                aborted_producers.insert(aborted_transactions[next_aborted++].producer_id);

        if (batch.isControl())
        {
            /// Control records mark transaction ends. Key layout: version
            /// Int16, type Int16 (0 = ABORT, 1 = COMMIT), big-endian. An ABORT
            /// marker closes its producer's aborted range. Unknown or short
            /// control records are ignored, like in the Java consumer.
            if (read_committed)
            {
                for (const auto & record : batch.records)
                {
                    if (!record.key || record.key->size() < 4)
                        continue;
                    Reader key_reader(*record.key);
                    key_reader.readInt16(); /// version
                    if (key_reader.readInt16() == 0)
                        aborted_producers.erase(batch.producer_id);
                }
            }
            continue;
        }

        if (read_committed && batch.isTransactional() && aborted_producers.contains(batch.producer_id))
            continue;

        /// attributes bit 3: LogAppendTime — every record carries max_timestamp.
        const bool log_append_time = batch.usesLogAppendTime();
        for (auto & record : batch.records)
        {
            const Int64 offset = batch.base_offset + record.offset_delta;
            /// Batches can start before the requested offset (compaction).
            if (offset < min_offset)
                continue;
            Record out;
            out.offset = offset;
            out.timestamp_ms = log_append_time ? batch.max_timestamp : batch.first_timestamp + record.timestamp_delta;
            out.key = std::move(record.key);
            out.value = std::move(record.value);
            out.headers = std::move(record.headers);
            result.records.push_back(std::move(out));
        }
    }
    return result;
}

}
