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

CompressionCodecPtr getCompressionCodecForFile(const DataPartStoragePtr & data_part_storage, const String & relative_path)
{
    auto read_buffer = data_part_storage->readFile(relative_path, {}, std::nullopt, std::nullopt);
    read_buffer->ignore(sizeof(Checksum));

    UInt8 header_size = ICompressionCodec::getHeaderSize();
    PODArray<char> compressed_buffer;
    compressed_buffer.resize(header_size);
    read_buffer->readStrict(compressed_buffer.data(), header_size);
    uint8_t method = ICompressionCodec::readMethod(compressed_buffer.data());
    if (method == static_cast<uint8_t>(CompressionMethodByte::Multiple))
    {
        compressed_buffer.resize(1);
        read_buffer->readStrict(compressed_buffer.data(), 1);
        compressed_buffer.resize(1 + compressed_buffer[0]);
        read_buffer->readStrict(compressed_buffer.data() + 1, compressed_buffer[0]);
        auto codecs_bytes = CompressionCodecMultiple::getCodecsBytesFromData(compressed_buffer.data());
        Codecs codecs;
        for (auto byte : codecs_bytes)
            codecs.push_back(CompressionCodecFactory::instance().get(byte));

        return std::make_shared<CompressionCodecMultiple>(codecs);
    }
    return CompressionCodecFactory::instance().get(method);
}

}
