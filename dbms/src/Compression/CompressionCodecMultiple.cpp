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
        uncompressed_size += codec->getCompressedReserveSize(uncompressed_size);

    ///  MultipleCodecByte  TotalCodecs  ByteForEachCodec       data
    return sizeof(UInt8) + sizeof(UInt8) + codecs.size() + uncompressed_size;
}

size_t CompressionCodecMultiple::compress(char * source, size_t source_size, char * dest)
{
    static constexpr size_t without_method_header_size = sizeof(UInt32) + sizeof(UInt32);

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source_size);
    uncompressed_buf.insert(source, source + source_size);

    dest[0] = static_cast<char>(getMethodByte());
    dest[1] = static_cast<char>(codecs.size());
    std::cerr << "(compress) codecs size:" << codecs.size() << std::endl;
    std::cerr << "(compress) desc:" << codec_desc << std::endl;
    size_t codecs_byte_pos = 2;
    for (size_t idx = 0; idx < codecs.size(); ++idx, ++codecs_byte_pos)
    {
        const auto codec = codecs[idx];
        dest[codecs_byte_pos] = codec->getMethodByte();
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

    memcpy(&dest[2 + codecs.size()], &compressed_buf[0], source_size);
    return source_size;
}

size_t CompressionCodecMultiple::decompress(char * source, size_t source_size, char * dest, size_t decompressed_size)
{
    UInt8 compression_methods_size = source[1];
    std::cerr << "(decompress) Methods size:" << +compression_methods_size << std::endl;

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf;
    compressed_buf.insert(&source[compression_methods_size + 2], &source[source_size - (compression_methods_size + 2)]);

    for (size_t idx = 0; idx < compression_methods_size; ++idx)
    {
        UInt8 compression_method = source[idx + 2];
        std::cerr << std::hex;
        std::cerr << "(decompress) Compression method byte:" << +compression_method << std::endl;
        const auto codec = CompressionCodecFactory::instance().get(compression_method);
        codec->decompress(&compressed_buf[8], 0, uncompressed_buf.data(), 0);
        uncompressed_buf.swap(compressed_buf);
    }

    memcpy(dest, uncompressed_buf.data(), decompressed_size);
    return decompressed_size;
}

void registerCodecMultiple(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("Multiple", static_cast<char>(CompressionMethodByte::Multiple), [&](){
        return std::make_shared<CompressionCodecMultiple>();
    });
}

}
