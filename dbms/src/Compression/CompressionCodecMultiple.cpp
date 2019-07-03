#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionInfo.h>
#include <common/unaligned.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/hex.h>
#include <sstream>


namespace DB
{


namespace ErrorCodes
{
extern const int UNKNOWN_CODEC;
extern const int CORRUPTED_DATA;
}

CompressionCodecMultiple::CompressionCodecMultiple(Codecs codecs)
    : codecs(codecs)
{
}

UInt8 CompressionCodecMultiple::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Multiple);
}

String CompressionCodecMultiple::getCodecDesc() const
{
    std::ostringstream ss;
    for (size_t idx = 0; idx < codecs.size(); idx++)
    {
        if (idx != 0)
            ss << ',' << ' ';

        const auto codec = codecs[idx];
        ss << codec->getCodecDesc();
    }
    return ss.str();
}

UInt32 CompressionCodecMultiple::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 compressed_size = uncompressed_size;
    for (auto & codec : codecs)
        compressed_size = codec->getCompressedReserveSize(compressed_size);

    ///    TotalCodecs  ByteForEachCodec       data
    return sizeof(UInt8) + codecs.size() + compressed_size;
}

UInt32 CompressionCodecMultiple::doCompressData(const char * source, UInt32 source_size, char * dest) const
{

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source, source + source_size);

    dest[0] = static_cast<UInt8>(codecs.size());

    size_t codecs_byte_pos = 1;
    for (size_t idx = 0; idx < codecs.size(); ++idx, ++codecs_byte_pos)
    {
        const auto codec = codecs[idx];
        dest[codecs_byte_pos] = codec->getMethodByte();
        compressed_buf.resize(codec->getCompressedReserveSize(source_size));

        UInt32 size_compressed = codec->compress(uncompressed_buf.data(), source_size, compressed_buf.data());

        uncompressed_buf.swap(compressed_buf);
        source_size = size_compressed;
    }

    memcpy(&dest[1 + codecs.size()], uncompressed_buf.data(), source_size);

    return 1 + codecs.size() + source_size;
}

void CompressionCodecMultiple::useInfoAboutType(DataTypePtr data_type)
{
    for (auto & codec : codecs)
    {
        codec->useInfoAboutType(data_type);
    }
}

void CompressionCodecMultiple::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 decompressed_size) const
{
    UInt8 compression_methods_size = source[0];

    PODArray<char> compressed_buf(&source[compression_methods_size + 1], &source[source_size]);
    PODArray<char> uncompressed_buf;
    /// Insert all data into compressed buf
    source_size -= (compression_methods_size + 1);

    for (long idx = compression_methods_size - 1; idx >= 0; --idx)
    {
        UInt8 compression_method = source[idx + 1];
        const auto codec = CompressionCodecFactory::instance().get(compression_method);
        compressed_buf.resize(compressed_buf.size() + codec->getAdditionalSizeAtTheEndOfBuffer());
        UInt32 uncompressed_size = ICompressionCodec::readDecompressedBlockSize(compressed_buf.data());

        if (idx == 0 && uncompressed_size != decompressed_size)
            throw Exception("Wrong final decompressed size in codec Multiple, got " + toString(uncompressed_size) + ", expected " + toString(decompressed_size), ErrorCodes::CORRUPTED_DATA);

        uncompressed_buf.resize(uncompressed_size + codec->getAdditionalSizeAtTheEndOfBuffer());
        codec->decompress(compressed_buf.data(), source_size, uncompressed_buf.data());
        uncompressed_buf.swap(compressed_buf);
        source_size = uncompressed_size;
    }

    memcpy(dest, compressed_buf.data(), decompressed_size);
}

void registerCodecMultiple(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("Multiple", static_cast<UInt8>(CompressionMethodByte::Multiple), [&] ()
    {
        return std::make_shared<CompressionCodecMultiple>();
    });
}

}
