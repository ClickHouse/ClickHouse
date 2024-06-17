#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionInfo.h>
#include <Common/PODArray.h>
#include <Compression/CompressionFactory.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

CompressionCodecMultiple::CompressionCodecMultiple(Codecs codecs_)
    : codecs(codecs_)
{
    ASTs arguments;
    for (const auto & codec : codecs)
        arguments.push_back(codec->getCodecDesc());
    /// Special case, codec doesn't have name and contain list of codecs.
    setCodecDescription("", arguments);
}

uint8_t CompressionCodecMultiple::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Multiple);
}

void CompressionCodecMultiple::updateHash(SipHash & hash) const
{
    for (const auto & codec : codecs)
        codec->updateHash(hash);
}

UInt32 CompressionCodecMultiple::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 compressed_size = uncompressed_size;
    for (const auto & codec : codecs)
        compressed_size = codec->getCompressedReserveSize(compressed_size);

    ///    TotalCodecs  ByteForEachCodec       data
    return static_cast<UInt32>(sizeof(UInt8) + codecs.size() + compressed_size);
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

    return static_cast<UInt32>(1 + codecs.size() + source_size);
}

void CompressionCodecMultiple::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 decompressed_size) const
{
    if (source_size < 1 || !source[0])
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Wrong compression methods list");

    UInt8 compression_methods_size = source[0];

    PODArray<char> compressed_buf(&source[compression_methods_size + 1], &source[source_size]);
    PODArray<char> uncompressed_buf;
    /// Insert all data into compressed buf
    source_size -= (compression_methods_size + 1);

    for (int idx = compression_methods_size - 1; idx >= 0; --idx)
    {
        UInt8 compression_method = source[idx + 1];
        const auto codec = CompressionCodecFactory::instance().get(compression_method);
        auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

        if (compressed_buf.size() >= 1_GiB)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Too large compressed size: {}", compressed_buf.size());

        {
            UInt32 bytes_to_resize;
            if (common::addOverflow(static_cast<UInt32>(compressed_buf.size()), additional_size_at_the_end_of_buffer, bytes_to_resize))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Too large compressed size: {}", compressed_buf.size());

            compressed_buf.resize(compressed_buf.size() + additional_size_at_the_end_of_buffer);
        }

        UInt32 uncompressed_size = readDecompressedBlockSize(compressed_buf.data());

        if (uncompressed_size >= 1_GiB)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Too large uncompressed size: {}", uncompressed_size);

        if (idx == 0 && uncompressed_size != decompressed_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Wrong final decompressed size in codec Multiple, got {}, expected {}",
                uncompressed_size, decompressed_size);

        {
            UInt32 bytes_to_resize;
            if (common::addOverflow(uncompressed_size, additional_size_at_the_end_of_buffer, bytes_to_resize))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Too large uncompressed size: {}", uncompressed_size);

            uncompressed_buf.resize(bytes_to_resize);
        }

        codec->decompress(compressed_buf.data(), source_size, uncompressed_buf.data());
        uncompressed_buf.swap(compressed_buf);
        source_size = uncompressed_size;
    }

    memcpy(dest, compressed_buf.data(), decompressed_size);
}

std::vector<uint8_t> CompressionCodecMultiple::getCodecsBytesFromData(const char * source)
{
    std::vector<uint8_t> result;
    uint8_t compression_methods_size = source[0];
    for (size_t i = 0; i < compression_methods_size; ++i)
        result.push_back(source[1 + i]);
    return result;
}

bool CompressionCodecMultiple::isCompression() const
{
    for (const auto & codec : codecs)
        if (codec->isCompression())
            return true;
    return false;
}


void registerCodecMultiple(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("Multiple", static_cast<UInt8>(CompressionMethodByte::Multiple), [&] ()
    {
        return std::make_shared<CompressionCodecMultiple>();
    });
}

}
