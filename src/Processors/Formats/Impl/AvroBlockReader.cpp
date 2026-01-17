#include "AvroBlockReader.h"

#if USE_AVRO

#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <Compiler.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
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

namespace
{
    /// Read Avro string (varint length + bytes)
    std::string readAvroString(ReadBuffer & in)
    {
        Int64 len;
        DB::readVarInt(len, in);
        if (len < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative string length in Avro header: {}", len);
        std::string result;
        result.resize(len);
        in.readStrict(result.data(), len);
        return result;
    }

    /// Read Avro bytes (same encoding as string)
    std::string readAvroBytes(ReadBuffer & in)
    {
        return readAvroString(in);
    }
}

AvroHeaderState AvroBlockReader::parseHeader(ReadBuffer & in)
{
    AvroHeaderState state;

    /// 1. Read and verify magic: "Obj\x01"
    char magic[4];
    in.readStrict(magic, 4);
    if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j' || magic[3] != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Avro file: bad magic bytes");

    /// 2. Read metadata map
    std::string schema_json;
    std::string codec_name = "null";  // default

    while (true)
    {
        Int64 count;
        DB::readVarInt(count, in);

        if (count == 0)
            break;

        /// If count is negative, there's also a byte size (which we ignore)
        if (count < 0)
        {
            count = -count;
            Int64 byte_size;
            DB::readVarInt(byte_size, in);
            (void)byte_size;
        }

        for (Int64 i = 0; i < count; ++i)
        {
            std::string key = readAvroString(in);
            std::string value = readAvroBytes(in);

            if (key == "avro.schema")
                schema_json = std::move(value);
            else if (key == "avro.codec")
                codec_name = std::move(value);
        }
    }

    /// 3. Read sync marker (16 bytes)
    in.readStrict(reinterpret_cast<char *>(state.sync_marker.data()), state.sync_marker.size());

    /// 4. Parse schema JSON
    if (schema_json.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Avro file missing schema in metadata");

    state.schema = avro::compileJsonSchemaFromString(schema_json);

    /// 5. Parse codec
    if (codec_name == "null")
        state.codec = avro::NULL_CODEC;
    else if (codec_name == "deflate")
        state.codec = avro::DEFLATE_CODEC;
#ifdef SNAPPY_CODEC_AVAILABLE
    else if (codec_name == "snappy")
        state.codec = avro::SNAPPY_CODEC;
#endif
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Avro codec: {}", codec_name);

    return state;
}

}

#endif
