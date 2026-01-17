#include "AvroBlockReader.h"

#if USE_AVRO

#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

int64_t AvroBlockReader::readVarInt(ReadBuffer & in)
{
    Int64 value;
    DB::readVarInt(value, in);
    return value;
}

int64_t AvroBlockReader::readBlockInto(ReadBuffer & in, Memory<> & memory)
{
    int64_t object_count = readVarInt(in);
    int64_t byte_count = readVarInt(in);

    if (byte_count < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Invalid Avro block: negative byte count {}", byte_count);

    /// Write varints directly to memory using stack buffer
    char varint_buf[20];  /// Max 10 bytes each for two varints
    char * ptr = varint_buf;
    ptr = DB::writeVarInt(object_count, ptr);
    ptr = DB::writeVarInt(byte_count, ptr);
    size_t header_size = ptr - varint_buf;

    /// Resize memory and append header + compressed data
    size_t old_size = memory.size();
    memory.resize(old_size + header_size + static_cast<size_t>(byte_count));
    memcpy(memory.data() + old_size, varint_buf, header_size);
    in.readStrict(memory.data() + old_size + header_size, static_cast<size_t>(byte_count));

    return object_count;
}

bool AvroBlockReader::verifySyncMarker(ReadBuffer & in, const avro::DataFileSync & expected)
{
    if (in.eof())
        return false;

    avro::DataFileSync actual{};
    in.readStrict(reinterpret_cast<char *>(actual.data()), actual.size());

    if (actual != expected)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Avro sync marker mismatch");
    return true;
}

void AvroBlockReader::decompressBlock(const char * data, size_t size, avro::Codec codec, std::string & out)
{
    avro::DataFileReaderBase::decompressBlock(data, size, codec, out);
}

AvroHeaderState AvroBlockReader::parseHeader(ReadBuffer & in)
{
    /// Use the Avro library to parse the header.
    /// The adapter bridges ClickHouse's ReadBuffer to Avro's InputStream interface.
    ReadBufferInputStream adapter(in);
    avro::AvroFileHeader header = avro::readAvroHeader(adapter);

    /// The library's drain() call ensures byteCount() reflects actual bytes consumed.
    /// Position the ReadBuffer to exactly after the header.
    /// The adapter's byteCount() tells us where we are; headerSize tells us where we should be.
    if (adapter.byteCount() > header.headerSize)
    {
        /// We over-read; back up to the correct position
        in.position() -= (adapter.byteCount() - header.headerSize);
    }

    return AvroHeaderState{
        .schema = std::move(header.schema),
        .sync_marker = header.sync,
        .codec = header.codec
    };
}

}

#endif
