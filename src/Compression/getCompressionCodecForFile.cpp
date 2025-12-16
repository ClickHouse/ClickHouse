#include <Compression/getCompressionCodecForFile.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Common/PODArray.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/IDataPartStorage.h>

namespace DB
{

using Checksum = CityHash_v1_0_2::uint128;

CompressionCodecPtr
getCompressionCodecForFile(ReadBuffer & read_buffer, UInt32 & size_compressed, UInt32 & size_decompressed, bool skip_to_next_block)
{
    read_buffer.ignore(sizeof(Checksum));

    UInt8 header_size = ICompressionCodec::getHeaderSize();
    size_t starting_bytes = read_buffer.count();
    PODArray<char> compressed_buffer;
    compressed_buffer.resize(header_size);
    read_buffer.readStrict(compressed_buffer.data(), header_size);
    uint8_t method = ICompressionCodec::readMethod(compressed_buffer.data());
    size_compressed = unalignedLoad<UInt32>(&compressed_buffer[1]);
    size_decompressed = unalignedLoad<UInt32>(&compressed_buffer[5]);
    if (method == static_cast<uint8_t>(CompressionMethodByte::Multiple))
    {
        compressed_buffer.resize(1);
        read_buffer.readStrict(compressed_buffer.data(), 1);
        compressed_buffer.resize(1 + compressed_buffer[0]);
        read_buffer.readStrict(compressed_buffer.data() + 1, compressed_buffer[0]);
        auto codecs_bytes = CompressionCodecMultiple::getCodecsBytesFromData(compressed_buffer.data());
        Codecs codecs;
        for (auto byte : codecs_bytes)
            codecs.push_back(CompressionCodecFactory::instance().get(byte));

        if (skip_to_next_block)
            read_buffer.ignore(size_compressed - (read_buffer.count() - starting_bytes));

        return std::make_shared<CompressionCodecMultiple>(codecs);
    }

    if (skip_to_next_block)
        read_buffer.ignore(size_compressed - (read_buffer.count() - starting_bytes));

    return CompressionCodecFactory::instance().get(method);
}

CompressionCodecPtr getCompressionCodecForFile(const IDataPartStorage & data_part_storage, const String & relative_path)
{
    auto read_buffer = data_part_storage.readFile(relative_path, {}, std::nullopt);
    UInt32 size_compressed;
    UInt32 size_decompressed;
    return getCompressionCodecForFile(*read_buffer, size_compressed, size_decompressed, false);
}

}
