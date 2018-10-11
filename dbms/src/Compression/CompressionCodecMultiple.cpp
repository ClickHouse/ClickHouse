#include <Compression/CompressionCodecMultiple.h>
#include <IO/CompressedStream.h>
#include <common/unaligned.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

CompressionCodecMultiple::CompressionCodecMultiple(Codecs codecs)
    : codecs(codecs)
{
    for (size_t idx = 0; idx < codecs.size(); idx++)
    {
        if (idx != 0)
            codec_desc = codec_desc + ',';

        const auto codec = codecs[idx];
        String inner_codec_desc;
        codec->getCodecDesc(inner_codec_desc);
        codec_desc = codec_desc + inner_codec_desc;
    }
}

char CompressionCodecMultiple::getMethodByte()
{
    return static_cast<char>(CompressionMethodByte::Multiple);
}

void CompressionCodecMultiple::getCodecDesc(String & codec_desc_)
{
    codec_desc_ = codec_desc;
}

size_t CompressionCodecMultiple::getCompressedReserveSize(size_t uncompressed_size)
{
    for (auto & codec : codecs)
        uncompressed_size = codec->getCompressedReserveSize(uncompressed_size);

    return sizeof(UInt8) + codecs.size() + uncompressed_size;
}

size_t CompressionCodecMultiple::compress(char * source, size_t source_size, char * dest)
{
    static constexpr size_t without_method_header_size = sizeof(UInt32) + sizeof(UInt32);

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source_size);
    uncompressed_buf.insert(source, source + source_size);

    dest[0] = static_cast<char>(codecs.size());
    for (size_t idx = 0; idx < codecs.size(); ++idx)
    {
        const auto codec = codecs[idx];
        dest[idx + 1] = codec->getMethodByte();
        compressed_buf.resize(without_method_header_size + codec->getCompressedReserveSize(source_size));

        size_t size_compressed = without_method_header_size;
        size_compressed += codec->compress(&uncompressed_buf[0], source_size, &compressed_buf[without_method_header_size]);

        UInt32 compressed_size_32 = size_compressed;
        UInt32 uncompressed_size_32 = source_size;
        unalignedStore(&compressed_buf[0], compressed_size_32);
        unalignedStore(&compressed_buf[4], uncompressed_size_32);
        uncompressed_buf.swap(compressed_buf);
        source_size = size_compressed;
    }

    memcpy(&dest[codecs.size() + 1], &compressed_buf[0], source_size);
    return source_size;
}

size_t CompressionCodecMultiple::decompress(char * source, size_t source_size, char * dest, size_t decompressed_size)
{
    UInt8 compression_methods_size = source[0];

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf;
    compressed_buf.insert(&source[compression_methods_size + 1], source_size - (compression_methods_size + 1));

    for (size_t idx = 0; idx < compression_methods_size; ++idx)
    {
        UInt8 compression_method = source[idx + 1];
        const auto codec = CompressionCodecFactory::instance().get(compression_method);
        codec->decompress(&compressed_buf[8], 0, uncompressed_buf.data(), 0);
        uncompressed_buf.swap(compressed_buf);
    }

    memcpy(dest, uncompressed_buf.data(), decompressed_size);
    return decompressed_size;
}

}