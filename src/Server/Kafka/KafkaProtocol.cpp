#include <Server/Kafka/KafkaProtocol.h>
#include <Server/Kafka/KafkaError.h>

#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <crc32c/crc32c.h>
#include <snappy.h>

#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TIMEOUT_EXCEEDED;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_DECOMPRESS;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int VIOLATED_CONSTRAINT;
    extern const int QUERY_WAS_CANCELLED;
}

namespace Kafka
{

namespace
{

/// The size of the decompressed records of one batch is limited to protect from decompression bombs.
constexpr size_t max_decompressed_size = 1ULL << 30;

template <typename T>
T readBigEndian(Reader & reader)
{
    auto data = reader.raw(sizeof(T));
    T x;
    memcpy(&x, data.data(), sizeof(T));
    if constexpr (std::endian::native == std::endian::little)
        x = std::byteswap(x);
    return x;
}

template <typename T>
void writeBigEndian(String & out, T x)
{
    if constexpr (std::endian::native == std::endian::little)
        x = std::byteswap(x);
    out.append(reinterpret_cast<const char *>(&x), sizeof(T));
}

[[noreturn]] void throwMalformed(const char * what)
{
    throw Error(ErrorCode::CORRUPT_MESSAGE, ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Malformed Kafka message: {}", what);
}

}

Int8 Reader::int8()
{
    return static_cast<Int8>(raw(1)[0]);
}

Int16 Reader::int16()
{
    return readBigEndian<Int16>(*this);
}

Int32 Reader::int32()
{
    return readBigEndian<Int32>(*this);
}

Int64 Reader::int64()
{
    return readBigEndian<Int64>(*this);
}

UInt32 Reader::uint32()
{
    return readBigEndian<UInt32>(*this);
}

UInt64 Reader::unsignedVarInt()
{
    UInt64 result = 0;
    for (size_t shift = 0; shift < 64; shift += 7)
    {
        UInt8 byte = static_cast<UInt8>(int8());
        result |= static_cast<UInt64>(byte & 0x7F) << shift;
        if (!(byte & 0x80))
            return result;
    }
    throwMalformed("too long varint");
}

Int64 Reader::varLong()
{
    UInt64 zigzag = unsignedVarInt();
    return static_cast<Int64>(zigzag >> 1) ^ -static_cast<Int64>(zigzag & 1);
}

std::optional<String> Reader::nullableString()
{
    Int64 size = 0;
    if (flexible)
        size = static_cast<Int64>(unsignedVarInt()) - 1;
    else
        size = int16();

    if (size < 0)
        return std::nullopt;
    return String(raw(size));
}

String Reader::string()
{
    auto res = nullableString();
    if (!res)
        throwMalformed("unexpected null string");
    return std::move(*res);
}

std::optional<String> Reader::nullableBytes()
{
    Int64 size = 0;
    if (flexible)
        size = static_cast<Int64>(unsignedVarInt()) - 1;
    else
        size = int32();

    if (size < 0)
        return std::nullopt;
    return String(raw(size));
}

Int32 Reader::arrayLength()
{
    Int64 size = 0;
    if (flexible)
        size = static_cast<Int64>(unsignedVarInt()) - 1;
    else
        size = int32();

    /// Every element takes at least one byte, which protects from huge allocations.
    if (size > static_cast<Int64>(remaining()))
        throwMalformed("array length exceeds the size of the message");
    return static_cast<Int32>(size);
}

void Reader::taggedFields()
{
    if (!flexible)
        return;

    UInt64 num_fields = unsignedVarInt();
    for (UInt64 i = 0; i < num_fields; ++i)
    {
        unsignedVarInt(); /// tag
        UInt64 size = unsignedVarInt();
        skip(size);
    }
}

std::string_view Reader::raw(size_t size)
{
    if (size > remaining())
        throwMalformed("unexpected end of message");
    std::string_view res(pos, size);
    pos += size;
    return res;
}


void Writer::int16(Int16 x)
{
    writeBigEndian(out, x);
}

void Writer::int32(Int32 x)
{
    writeBigEndian(out, x);
}

void Writer::int64(Int64 x)
{
    writeBigEndian(out, x);
}

void Writer::uint32(UInt32 x)
{
    writeBigEndian(out, x);
}

void Writer::unsignedVarInt(UInt64 x)
{
    while (x >= 0x80)
    {
        out.push_back(static_cast<char>((x & 0x7F) | 0x80));
        x >>= 7;
    }
    out.push_back(static_cast<char>(x));
}

void Writer::varLong(Int64 x)
{
    unsignedVarInt((static_cast<UInt64>(x) << 1) ^ static_cast<UInt64>(x >> 63));
}

void Writer::string(std::string_view s)
{
    if (flexible)
        unsignedVarInt(s.size() + 1);
    else
        int16(static_cast<Int16>(s.size()));
    out.append(s);
}

void Writer::nullString()
{
    if (flexible)
        unsignedVarInt(0);
    else
        int16(-1);
}

void Writer::nullableString(const std::optional<String> & s)
{
    if (s)
        string(*s);
    else
        nullString();
}

void Writer::bytes(std::string_view s)
{
    if (flexible)
        unsignedVarInt(s.size() + 1);
    else
        int32(static_cast<Int32>(s.size()));
    out.append(s);
}

void Writer::nullableBytes(const std::optional<String> & s)
{
    if (s)
        bytes(*s);
    else if (flexible)
        unsignedVarInt(0);
    else
        int32(-1);
}

void Writer::arrayLength(size_t size)
{
    if (flexible)
        unsignedVarInt(size + 1);
    else
        int32(static_cast<Int32>(size));
}

void Writer::nullArray()
{
    if (flexible)
        unsignedVarInt(0);
    else
        int32(-1);
}

void Writer::taggedFields()
{
    if (flexible)
        unsignedVarInt(0);
}


namespace
{

String decompressStream(std::string_view data, CompressionMethod method)
{
    auto in = wrapReadBufferWithCompressionMethod(
        std::make_unique<ReadBufferFromMemory>(data.data(), data.size()), method);

    String res;
    while (!in->eof())
    {
        res.append(in->position(), in->buffer().end());
        in->position() = in->buffer().end();
        if (res.size() > max_decompressed_size)
            throw Error(ErrorCode::CORRUPT_MESSAGE, ErrorCodes::TOO_LARGE_STRING_SIZE, "Decompressed Kafka record batch is too large");
    }
    return res;
}

String snappyUncompressBlock(std::string_view data)
{
    size_t size = 0;
    if (!snappy::GetUncompressedLength(data.data(), data.size(), &size) || size > max_decompressed_size)
        throw Error(ErrorCode::CORRUPT_MESSAGE, ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Snappy block of a Kafka record batch");

    String res(size, '\0');
    if (!snappy::RawUncompress(data.data(), data.size(), res.data()))
        throw Error(ErrorCode::CORRUPT_MESSAGE, ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Snappy block of a Kafka record batch");
    return res;
}

/// The Java client writes Snappy in the framing of `snappy-java` (xerial): a magic header,
/// then blocks, each prefixed by its big-endian 32-bit size. Other clients write a raw Snappy block.
String snappyUncompress(std::string_view data)
{
    static constexpr std::string_view xerial_magic("\x82SNAPPY\0", 8);
    if (!data.starts_with(xerial_magic))
        return snappyUncompressBlock(data);

    Reader reader(data.data(), data.data() + data.size());
    reader.skip(xerial_magic.size());
    reader.int32(); /// version
    reader.int32(); /// compatible version

    String res;
    while (!reader.eof())
    {
        Int32 block_size = reader.int32();
        if (block_size < 0)
            throwMalformed("negative size of Snappy block");
        res += snappyUncompressBlock(reader.raw(block_size));
        if (res.size() > max_decompressed_size)
            throw Error(ErrorCode::CORRUPT_MESSAGE, ErrorCodes::TOO_LARGE_STRING_SIZE, "Decompressed Kafka record batch is too large");
    }
    return res;
}

void patchBigEndian(String & out, size_t pos, UInt32 value)
{
    if constexpr (std::endian::native == std::endian::little)
        value = std::byteswap(value);
    memcpy(out.data() + pos, &value, sizeof(value));
}

std::optional<String> readVarBytes(Reader & reader)
{
    Int32 size = reader.varInt();
    if (size < 0)
        return std::nullopt;
    return String(reader.raw(size));
}

void writeVarBytes(String & out, const std::optional<String> & s)
{
    Writer writer(out);
    if (!s)
    {
        writer.varInt(-1);
        return;
    }
    writer.varInt(static_cast<Int32>(s->size()));
    out.append(*s);
}

}

std::vector<RecordBatch> parseRecordBatches(std::string_view data)
{
    std::vector<RecordBatch> batches;
    Reader reader(data.data(), data.data() + data.size());

    while (!reader.eof())
    {
        RecordBatch & batch = batches.emplace_back();
        batch.base_offset = reader.int64();
        Int32 batch_length = reader.int32();
        if (batch_length < 0)
            throwMalformed("negative batch length");

        std::string_view batch_data = reader.raw(batch_length);
        Reader batch_reader(batch_data.data(), batch_data.data() + batch_data.size());

        batch_reader.int32(); /// partition leader epoch
        Int8 magic = batch_reader.int8();
        if (magic != 2)
            throw Error(ErrorCode::UNSUPPORTED_FOR_MESSAGE_FORMAT, ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
                "Unsupported Kafka message format version {}, only version 2 is supported", static_cast<int>(magic));

        UInt32 crc = batch_reader.uint32();
        UInt32 computed_crc = crc32c::Crc32c(batch_data.data() + 9, batch_data.size() - 9);
        if (crc != computed_crc)
            throw Error(ErrorCode::CORRUPT_MESSAGE, ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
                "Checksum mismatch in a Kafka record batch: expected {}, computed {}", crc, computed_crc);

        batch.attributes = batch_reader.int16();
        batch_reader.int32(); /// last offset delta
        batch.base_timestamp = batch_reader.int64();
        batch_reader.int64(); /// max timestamp
        batch.producer_id = batch_reader.int64();
        batch.producer_epoch = batch_reader.int16();
        batch.base_sequence = batch_reader.int32();
        Int32 num_records = batch_reader.int32();
        if (num_records < 0)
            throwMalformed("negative number of records");

        std::string_view records_data = batch_reader.raw(batch_reader.remaining());
        String decompressed;
        switch (batch.attributes & 0x07)
        {
            case 0:
                break;
            case 1:
                decompressed = decompressStream(records_data, CompressionMethod::Gzip);
                break;
            case 2:
                decompressed = snappyUncompress(records_data);
                break;
            case 3:
                decompressed = decompressStream(records_data, CompressionMethod::Lz4);
                break;
            case 4:
                decompressed = decompressStream(records_data, CompressionMethod::Zstd);
                break;
            default:
                throw Error(ErrorCode::UNSUPPORTED_COMPRESSION_TYPE, ErrorCodes::CANNOT_DECOMPRESS,
                    "Unsupported compression type {} of a Kafka record batch", batch.attributes & 0x07);
        }
        if (batch.attributes & 0x07)
            records_data = decompressed;

        Reader records_reader(records_data.data(), records_data.data() + records_data.size());
        if (static_cast<size_t>(num_records) > records_data.size())
            throwMalformed("number of records exceeds the size of the batch");
        batch.records.reserve(num_records);

        for (Int32 i = 0; i < num_records; ++i)
        {
            Int32 record_length = records_reader.varInt();
            if (record_length < 0)
                throwMalformed("negative record length");

            std::string_view record_data = records_reader.raw(record_length);
            Reader record_reader(record_data.data(), record_data.data() + record_data.size());

            Record & record = batch.records.emplace_back();
            record_reader.int8(); /// attributes
            record.timestamp = batch.base_timestamp + record_reader.varLong();
            record.offset = batch.base_offset + record_reader.varInt();
            record.key = readVarBytes(record_reader);
            record.value = readVarBytes(record_reader);

            Int32 num_headers = record_reader.varInt();
            if (num_headers < 0 || static_cast<size_t>(num_headers) > record_reader.remaining())
                throwMalformed("wrong number of record headers");
            record.headers.reserve(num_headers);
            for (Int32 j = 0; j < num_headers; ++j)
            {
                auto header_key = readVarBytes(record_reader);
                if (!header_key)
                    throwMalformed("null header key");
                auto header_value = readVarBytes(record_reader);
                record.headers.emplace_back(std::move(*header_key), std::move(header_value));
            }
        }
    }

    return batches;
}

void writeRecordBatch(String & out, const std::vector<Record> & records)
{
    chassert(!records.empty());
    Writer writer(out);

    const Int64 base_offset = records.front().offset;
    const Int64 base_timestamp = records.front().timestamp;
    Int64 max_timestamp = base_timestamp;
    for (const auto & record : records)
        max_timestamp = std::max(max_timestamp, record.timestamp);

    const size_t batch_start = out.size();
    writer.int64(base_offset);
    writer.int32(0); /// batch length, filled below
    writer.int32(0); /// partition leader epoch
    writer.int8(2); /// magic
    const size_t crc_pos = out.size();
    writer.uint32(0); /// crc, filled below
    const size_t crc_start = out.size();

    writer.int16(0); /// attributes: no compression, CreateTime, not transactional
    writer.int32(static_cast<Int32>(records.back().offset - base_offset));
    writer.int64(base_timestamp);
    writer.int64(max_timestamp);
    writer.int64(-1); /// producer id
    writer.int16(-1); /// producer epoch
    writer.int32(-1); /// base sequence
    writer.int32(static_cast<Int32>(records.size()));

    String body;
    for (const auto & record : records)
    {
        body.clear();
        Writer body_writer(body);
        body_writer.int8(0); /// attributes
        body_writer.varLong(record.timestamp - base_timestamp);
        body_writer.varInt(static_cast<Int32>(record.offset - base_offset));
        writeVarBytes(body, record.key);
        writeVarBytes(body, record.value);
        body_writer.varInt(static_cast<Int32>(record.headers.size()));
        for (const auto & [header_key, header_value] : record.headers)
        {
            writeVarBytes(body, header_key);
            writeVarBytes(body, header_value);
        }

        writer.varInt(static_cast<Int32>(body.size()));
        out.append(body);
    }

    patchBigEndian(out, batch_start + 8, static_cast<UInt32>(out.size() - batch_start - 12));
    patchBigEndian(out, crc_pos, crc32c::Crc32c(out.data() + crc_start, out.size() - crc_start));
}

void writeEmptyRecordBatch(String & out, Int64 last_offset)
{
    Writer writer(out);
    const size_t batch_start = out.size();
    writer.int64(last_offset);
    writer.int32(0); /// batch length, filled below
    writer.int32(0); /// partition leader epoch
    writer.int8(2); /// magic
    const size_t crc_pos = out.size();
    writer.uint32(0); /// crc, filled below
    const size_t crc_start = out.size();

    writer.int16(0); /// attributes
    writer.int32(0); /// last offset delta
    writer.int64(-1); /// base timestamp
    writer.int64(-1); /// max timestamp
    writer.int64(-1); /// producer id
    writer.int16(-1); /// producer epoch
    writer.int32(-1); /// base sequence
    writer.int32(0); /// number of records

    patchBigEndian(out, batch_start + 8, static_cast<UInt32>(out.size() - batch_start - 12));
    patchBigEndian(out, crc_pos, crc32c::Crc32c(out.data() + crc_start, out.size() - crc_start));
}

ErrorCode getCurrentExceptionKafkaErrorCode()
{
    try
    {
        throw;
    }
    catch (const Error & e)
    {
        return e.kafkaCode();
    }
    catch (const Exception & e)
    {
        const int code = e.code();
        if (code == ErrorCodes::UNKNOWN_TABLE || code == ErrorCodes::UNKNOWN_DATABASE)
            return ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
        if (code == ErrorCodes::ACCESS_DENIED)
            return ErrorCode::TOPIC_AUTHORIZATION_FAILED;
        if (code == ErrorCodes::TIMEOUT_EXCEEDED || code == ErrorCodes::QUERY_WAS_CANCELLED)
            return ErrorCode::REQUEST_TIMED_OUT;
        if (code == ErrorCodes::VIOLATED_CONSTRAINT)
            return ErrorCode::INVALID_RECORD;
        if (code == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF || code == ErrorCodes::CANNOT_DECOMPRESS)
            return ErrorCode::CORRUPT_MESSAGE;
        return ErrorCode::UNKNOWN_SERVER_ERROR;
    }
    catch (...)
    {
        /// Ok: any other exception is reported to the client as an unknown server error.
        return ErrorCode::UNKNOWN_SERVER_ERROR;
    }
}

}

}
