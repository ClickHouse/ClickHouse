#include <memory>
#include <utility>
#include <city.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>
#include <string.h>

#include <common/unaligned.h>
#include <Core/Types.h>

#include <IO/CompressedWriteBuffer.h>
#include <map>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int UNKNOWN_COMPRESSION_METHOD;
}

//class CompressionPipeline
//{
//private:
//
//    const std::map<std::string, uint8_t> METHOD_BYTES = {
//        {"LZ4", static_cast<uint8_t>(CompressionMethodByte::LZ4)},
//        {"LZ4HC", CompressionMethodByte::LZ4},
//        {"ZSTD", CompressionMethodByte::ZSTD},
//    };
//
//    std::vector<void (*)()> compression_pipe;
//    std::vector<void (*)()> decompression_pipe;
//    std::vector<size_t> compression_bounds;
//
//    std::vector<void (*)()> makeCompressionPipe() {
//        std::vector<void (*)()> pipe;
//        for (auto &method: methods) {
//            auto method_code = METHOD_BYTES.find(method);
//        }
//        return pipe;
//    }
//
//    std::vector<void (*)()> makeDecompressionPipe() {
//        std::vector<void (*)()> pipe;
//
//        return pipe;
//    }
//
//public:
//    static constexpr uint8_t continuation_bit = 0x01;
//    static constexpr uint8_t parametrization_bit = 0x10;
//    std::vector<std::string> methods;
//
//    explicit CompressionPipeline(std::vector<std::string> methods)
//            : methods(std::move(methods))
//    {}
//
//    size_t getCompressedSizeBound(size_t uncompressed_size)
//    {
//
//    }
//
//    int compress(const char* source, char* dest, int inputSize, int maxOutputSize, int compressionLevel) {};
//    // void* dst, size_t dstCapacity, const void* src, size_t srcSize, int compressionLevel
//
//    int decompress(void* dst, size_t maxDstSize, const void* src, size_t srcSize) {};
//};
//
//class BaseCompression
//{
//private:
//    std::string name;
//    uint8_t bytecode;
//    bool custom_settings = false; /// Used to determine if there is custom settings are specified for that particular compressor
//public:
//    virtual size_t compress();
//    virtual size_t decompress();
//};
//
//class LZ4Compression : BaseCompression
//{
//
//};
//
//class ZSTDCompression : BaseCompression
//{
//
//};

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    size_t uncompressed_size = offset();
    size_t compressed_size = 0;
    char * compressed_buffer_ptr = nullptr;

    /** The format of compressed block - see CompressedStream.h
      */

    switch (compression_settings.method)
    {
        case CompressionMethod::LZ4:
        case CompressionMethod::LZ4HC:
        {
            static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
            compressed_buffer.resize(header_size + LZ4_COMPRESSBOUND(uncompressed_size));
#pragma GCC diagnostic pop

            compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::LZ4);

            if (compression_settings.method == CompressionMethod::LZ4)
                compressed_size = header_size + LZ4_compress_default(
                    working_buffer.begin(),
                    &compressed_buffer[header_size],
                    uncompressed_size,
                    LZ4_COMPRESSBOUND(uncompressed_size));
            else
                compressed_size = header_size + LZ4_compress_HC(
                    working_buffer.begin(),
                    &compressed_buffer[header_size],
                    uncompressed_size,
                    LZ4_COMPRESSBOUND(uncompressed_size),
                    compression_settings.level);

            UInt32 compressed_size_32 = compressed_size;
            UInt32 uncompressed_size_32 = uncompressed_size;

            unalignedStore(&compressed_buffer[1], compressed_size_32);
            unalignedStore(&compressed_buffer[5], uncompressed_size_32);

            compressed_buffer_ptr = &compressed_buffer[0];
            break;
        }
        case CompressionMethod::ZSTD:
        {
            static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

            compressed_buffer.resize(header_size + ZSTD_compressBound(uncompressed_size));

            compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::ZSTD);

            size_t res = ZSTD_compress(
                &compressed_buffer[header_size],
                compressed_buffer.size() - header_size,
                working_buffer.begin(),
                uncompressed_size,
                compression_settings.level);

            if (ZSTD_isError(res))
                throw Exception("Cannot compress block with ZSTD: " + std::string(ZSTD_getErrorName(res)), ErrorCodes::CANNOT_COMPRESS);

            compressed_size = header_size + res;

            UInt32 compressed_size_32 = compressed_size;
            UInt32 uncompressed_size_32 = uncompressed_size;

            unalignedStore(&compressed_buffer[1], compressed_size_32);
            unalignedStore(&compressed_buffer[5], uncompressed_size_32);

            compressed_buffer_ptr = &compressed_buffer[0];
            break;
        }
        case CompressionMethod::NONE:
        {
            static constexpr size_t header_size = 1 + sizeof (UInt32) + sizeof (UInt32);

            compressed_size = header_size + uncompressed_size;
            UInt32 uncompressed_size_32 = uncompressed_size;
            UInt32 compressed_size_32 = compressed_size;

            compressed_buffer.resize(compressed_size);

            compressed_buffer[0] = static_cast<UInt8>(CompressionMethodByte::NONE);

            unalignedStore(&compressed_buffer[1], compressed_size_32);
            unalignedStore(&compressed_buffer[5], uncompressed_size_32);
            memcpy(&compressed_buffer[9], working_buffer.begin(), uncompressed_size);

            compressed_buffer_ptr = &compressed_buffer[0];
            break;
        }
        default:
            throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

    CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer_ptr, compressed_size);
    out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

    out.write(compressed_buffer_ptr, compressed_size);
}


CompressedWriteBuffer::CompressedWriteBuffer(
    WriteBuffer & out_,
    CompressionSettings compression_settings_,
    size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), compression_settings(compression_settings_)
{
}


CompressedWriteBuffer::~CompressedWriteBuffer()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
