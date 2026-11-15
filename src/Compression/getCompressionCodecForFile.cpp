#include <Compression/getCompressionCodecForFile.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <base/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int TOO_LARGE_SIZE_COMPRESSED;
}

using Checksum = CityHash_v1_0_2::uint128;

CompressionCodecPtr
getCompressionCodecForFile(ReadBuffer & read_buffer, UInt32 & size_compressed, UInt32 & size_decompressed, bool skip_to_next_block)
{
    read_buffer.ignore(sizeof(Checksum));

    UInt8 header_size = ICompressionCodec::getHeaderSize();
    PODArray<char> compressed_buffer;
    compressed_buffer.resize(header_size);
    read_buffer.readStrict(compressed_buffer.data(), header_size);
    uint8_t method = ICompressionCodec::readMethod(compressed_buffer.data());
    size_compressed = unalignedLoad<UInt32>(&compressed_buffer[1]);
    size_decompressed = unalignedLoad<UInt32>(&compressed_buffer[5]);

    if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
        throw Exception(
            ErrorCodes::TOO_LARGE_SIZE_COMPRESSED,
            "Compressed block header reports compressed size {} which is above the {} limit. Most likely corrupted data.",
            size_compressed,
            DBMS_MAX_COMPRESSED_SIZE);

    if (size_compressed < header_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Compressed block header reports compressed size {} which is less than the {}-byte header",
            size_compressed,
            static_cast<UInt32>(header_size));

    if (size_decompressed == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Compressed block header reports decompressed size 0. Most likely corrupted data.");

    if (method == static_cast<uint8_t>(CompressionMethodByte::Multiple))
    {
        if (size_compressed < static_cast<UInt32>(header_size) + 1)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Compressed block header reports compressed size {}, too small to hold the Multiple codec count byte (needs at least {} "
                "bytes)",
                size_compressed,
                static_cast<UInt32>(header_size) + 1);

        compressed_buffer.resize(1);
        read_buffer.readStrict(compressed_buffer.data(), 1);
        const size_t codecs_count = static_cast<UInt8>(compressed_buffer[0]);
        /// Multiple block layout: [9-byte header][1-byte codec count N][N method bytes][nested compressed block].
        const size_t nested_block_offset = static_cast<UInt32>(header_size) + 1 + codecs_count;

        if (size_compressed < nested_block_offset)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Compressed block header reports compressed size {}, too small for its declared {}-codec chain (needs {} bytes)",
                size_compressed,
                codecs_count,
                nested_block_offset);

        compressed_buffer.resize(1 + codecs_count);
        read_buffer.readStrict(compressed_buffer.data() + 1, codecs_count);
        auto codecs_bytes = CompressionCodecMultiple::getCodecsBytesFromData(compressed_buffer.data());
        Codecs codecs;
        for (auto byte : codecs_bytes)
            codecs.push_back(CompressionCodecFactory::instance().get(byte));

        if (skip_to_next_block)
            read_buffer.ignore(size_compressed - nested_block_offset);

        return std::make_shared<CompressionCodecMultiple>(codecs);
    }

    if (skip_to_next_block)
        read_buffer.ignore(size_compressed - header_size);

    return CompressionCodecFactory::instance().get(method);
}

CompressionCodecPtr getCompressionCodecForFile(const IDataPartStorage & data_part_storage, const String & relative_path)
{
    auto read_buffer = data_part_storage.readFile(relative_path, {}, std::nullopt);
    UInt32 size_compressed = 0;
    UInt32 size_decompressed = 0;
    return getCompressionCodecForFile(*read_buffer, size_compressed, size_decompressed, false);
}

}
