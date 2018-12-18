#include <Compression/CompressionCodecMultiple.h>
#include <IO/CompressedStream.h>
#include <common/unaligned.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


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
    static constexpr size_t header_for_size_store = sizeof(UInt32) + sizeof(UInt32);

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source, source + source_size);

    dest[0] = static_cast<char>(getMethodByte());
    dest[1] = static_cast<char>(codecs.size());
    //std::cerr << "(compress) codecs size:" << codecs.size() << std::endl;
    //std::cerr << "(compress) desc:" << codec_desc << std::endl;
    //std::cerr << "(compress) SOURCE WRITE:\n";
    //for (size_t i = 0 ; i < source_size; ++i) {
    //    std::cerr << std::hex << +source[i] << std::endl;
    //}
    //std::cerr << "(compress) UNCOMPRESSED BUF:\n";
    //for(size_t i =  0; i < source_size; ++i)
    //    std::cerr << std::hex << +uncompressed_buf[i] << " ";
    //std::cerr << std::dec << std::endl;

    size_t codecs_byte_pos = 2;
    for (size_t idx = 0; idx < codecs.size(); ++idx, ++codecs_byte_pos)
    {
        const auto codec = codecs[idx];
        dest[codecs_byte_pos] = codec->getMethodByte();
        //String name;
        //codec->getCodecDesc(name);
        //std::cerr << std::hex << "(compress) Compression method byte:" << +codec->getMethodByte() << " name:" << name << std::endl;
        compressed_buf.resize(header_for_size_store + codec->getCompressedReserveSize(source_size));


        size_t size_compressed = header_for_size_store;
        size_compressed += codec->compress(&uncompressed_buf[0], source_size, &compressed_buf[header_for_size_store]);

        UInt32 compressed_size_32 = size_compressed;
        UInt32 uncompressed_size_32 = source_size;
        //std::cerr << std::dec << "(compress) Compressed size:" << compressed_size_32 << std::endl;
        //std::cerr << std::dec << "(compress) Uncompressed size:" << uncompressed_size_32 << std::endl;
        unalignedStore(&compressed_buf[0], compressed_size_32);
        unalignedStore(&compressed_buf[4], uncompressed_size_32);
        //std::cerr << "(compress) COMPRESSED BUF:\n";
        //for(size_t i =  0; i < size_compressed; ++i)
        //    std::cerr << std::hex << +compressed_buf[i] << " ";
        //std::cerr << std::dec << std::endl;

        uncompressed_buf.swap(compressed_buf);
        source_size = size_compressed;
    }

    memcpy(&dest[2 + codecs.size()], &uncompressed_buf[0], source_size);

    //std::cerr << std::dec;
    //std::cerr << "(compress) WRITING BUF:" << 2 + codecs.size() + source_size << std::endl;
    //for(size_t i =  0; i < 2 + codecs.size() + source_size; ++i)
    //    std::cerr << std::hex << +dest[i] << " ";
    //std::cerr << std::endl;

    return 2 + codecs.size() + source_size;
}

size_t CompressionCodecMultiple::decompress(char * source, size_t source_size, char * dest, size_t decompressed_size)
{

    static constexpr size_t  header_for_size_store = sizeof(UInt32) + sizeof(UInt32);

    if (source[0] != getMethodByte())
        throw Exception("Incorrect compression method for codec multiple, given " + toString(source[0]) + ", expected " + toString(getMethodByte()),
            ErrorCodes::UNKNOWN_CODEC);

    UInt8 compression_methods_size = source[1];
    //std::cerr << std::dec;
    //std::cerr << "(decompress) Source size:" << source_size << std::endl;
    //std::cerr << "(decompress) decompressed size:" << decompressed_size << std::endl;
    //std::cerr << "(decompress) Methods size:" << +compression_methods_size << std::endl;

    //std::cerr << "READING BUF:" << std::dec << source_size << std::endl;
    //for(size_t i =  0; i < source_size; ++i)
    //    std::cerr << std::hex << +source[i] << " ";
    //std::cerr << std::endl;

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf;
    /// Insert all data into compressed buf
    compressed_buf.insert(&source[compression_methods_size + 2], &source[source_size]);

    for (long idx = compression_methods_size - 1; idx >= 0; --idx)
    {
        UInt8 compression_method = source[idx + 2];
        const auto codec = CompressionCodecFactory::instance().get(compression_method);
        //String name;
        //codec->getCodecDesc(name);
        //std::cerr << std::hex << "(decompress) Compression method byte:" << +compression_method << " name:" << name << std::endl;
        UInt32 compressed_size = unalignedLoad<UInt32>(&compressed_buf[0]);
        UInt32 uncompressed_size = unalignedLoad<UInt32>(&compressed_buf[4]);
        if (idx == 0 && uncompressed_size != decompressed_size)
            throw Exception("Wrong final decompressed size in codec Multiple, got " + toString(uncompressed_size) + ", expected " + toString(decompressed_size), ErrorCodes::CORRUPTED_DATA);
        uncompressed_buf.resize(uncompressed_size);
        //std::cerr << std::dec << "(decompress) expected compressed size:" << compressed_size << std::endl;
        //std::cerr << std::dec << "(decompress) expected uncompressed size:" << uncompressed_size << std::endl;
        //std::cerr << "COMPRESSED BUF:\n";
        //for(size_t i =  0; i < compressed_size; ++i)
        //    std::cerr<< std::hex << +compressed_buf[i] << " ";
        //std::cerr << std::dec << std::endl;

        codec->decompress(&compressed_buf[header_for_size_store], compressed_size - header_for_size_store, &uncompressed_buf[0], uncompressed_size);
        //std::cerr << "FINAL UNCOMPRESSED:\n";
        //for (size_t i = 0; i < uncompressed_size; ++i) {
        //    std::cerr << std::hex << +uncompressed_buf[i] << std::endl;
        //}

        uncompressed_buf.swap(compressed_buf);
    }
    //std::cerr << "FINISHED\n";


    memcpy(dest, compressed_buf.data(), decompressed_size);
    return decompressed_size;
}

void registerCodecMultiple(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("Multiple", static_cast<char>(CompressionMethodByte::Multiple), [&](){
        return std::make_shared<CompressionCodecMultiple>();
    });
}

}
