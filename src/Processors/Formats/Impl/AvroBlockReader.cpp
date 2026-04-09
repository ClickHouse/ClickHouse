#include <Processors/Formats/Impl/AvroBlockReader.h>

#if USE_AVRO

#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>

#include <Compiler.hh>
#include <Decoder.hh>
#include <Stream.hh>
#include <ValidSchema.hh>

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filter/zstd.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#ifdef SNAPPY_CODEC_AVAILABLE
#include <snappy.h>
#include <boost/crc.hpp>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

int64_t AvroBlockReader::readBlockInto(ReadBuffer & in, Memory<> & memory)
{
    Int64 object_count;
    Int64 byte_count;
    DB::readVarInt(object_count, in);
    DB::readVarInt(byte_count, in);

    if (object_count < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Invalid Avro block: negative object count {}", object_count);

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
    if (codec == avro::NULL_CODEC)
    {
        out.assign(data, size);
        return;
    }

#ifdef SNAPPY_CODEC_AVAILABLE
    if (codec == avro::SNAPPY_CODEC)
    {
        if (size < 4)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Avro snappy block too small: expected at least 4 bytes, got {}", size);

        /// Last 4 bytes are the CRC32 checksum (big-endian)
        size_t compressed_size = size - 4;
        const auto * bytes = reinterpret_cast<const uint8_t *>(data);
        uint32_t expected_checksum
            = (static_cast<uint32_t>(bytes[compressed_size]) << 24)
            | (static_cast<uint32_t>(bytes[compressed_size + 1]) << 16)
            | (static_cast<uint32_t>(bytes[compressed_size + 2]) << 8)
            | (static_cast<uint32_t>(bytes[compressed_size + 3]));

        if (!snappy::Uncompress(data, compressed_size, &out))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Snappy decompression failed");

        boost::crc_32_type crc;
        crc.process_bytes(out.data(), out.size());
        if (crc() != expected_checksum)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Snappy CRC32 mismatch: expected {}, got {}", expected_checksum, crc());
        return;
    }
#endif

    /// Deflate and Zstd use boost::iostreams
    boost::iostreams::filtering_istream stream;
    if (codec == avro::DEFLATE_CODEC)
    {
        boost::iostreams::zlib_params params;
        params.method = boost::iostreams::zlib::deflated;
        params.noheader = true;
        stream.push(boost::iostreams::zlib_decompressor(params));
    }
    else if (codec == avro::ZSTD_CODEC)
    {
        stream.push(boost::iostreams::zstd_decompressor());
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Avro codec");
    }

    stream.push(boost::iostreams::basic_array_source<char>(data, size));

    out.clear();
    out.assign(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
}

AvroHeaderState AvroBlockReader::parseHeader(ReadBuffer & in)
{
    /// Parse the Avro Object Container File header directly using the avro library's
    /// binary decoder. The header format is:
    ///   - 4-byte magic: 'O' 'b' 'j' 0x01
    ///   - metadata: map<string, bytes> (contains "avro.schema" and "avro.codec")
    ///   - sync marker: 16 bytes
    ReadBufferInputStream adapter(in);
    auto decoder = avro::binaryDecoder();
    decoder->init(adapter);

    /// Read and verify magic bytes
    std::array<uint8_t, 4> magic{};
    avro::decode(*decoder, magic);
    const std::array<uint8_t, 4> expected_magic = {{'O', 'b', 'j', '\x01'}};
    if (magic != expected_magic)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Avro file: bad magic bytes");

    /// Read metadata map
    std::map<std::string, std::vector<uint8_t>> metadata;
    avro::decode(*decoder, metadata);

    /// Extract schema
    auto schema_it = metadata.find("avro.schema");
    if (schema_it == metadata.end())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Avro file missing schema in metadata");

    std::string schema_json(schema_it->second.begin(), schema_it->second.end());
    avro::ValidSchema schema = avro::compileJsonSchemaFromString(schema_json);

    /// Extract codec
    avro::Codec codec = avro::NULL_CODEC;
    auto codec_it = metadata.find("avro.codec");
    if (codec_it != metadata.end())
    {
        std::string codec_name(codec_it->second.begin(), codec_it->second.end());
        if (codec_name == "deflate")
            codec = avro::DEFLATE_CODEC;
        else if (codec_name == "zstandard")
            codec = avro::ZSTD_CODEC;
#ifdef SNAPPY_CODEC_AVAILABLE
        else if (codec_name == "snappy")
            codec = avro::SNAPPY_CODEC;
#endif
        else if (codec_name != "null")
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Avro codec: {}", codec_name);
    }

    /// Read sync marker
    avro::DataFileSync sync_marker{};
    avro::decode(*decoder, sync_marker);

    /// The decoder may have buffered more data than the header.
    /// We need to position the ReadBuffer right after the sync marker.
    /// Since the decoder consumed data through our adapter, we need to flush
    /// any remaining buffered data in the decoder back to the ReadBuffer.
    decoder->init(adapter);  /// flush decoder state

    /// Now the adapter's byteCount() reflects exactly how many bytes were consumed.
    /// But the adapter may have over-read from the ReadBuffer.
    /// We don't need to adjust - the adapter's next/backup already handle ReadBuffer positioning.

    return AvroHeaderState{
        .schema = std::move(schema),
        .sync_marker = sync_marker,
        .codec = codec
    };
}

}

#endif
