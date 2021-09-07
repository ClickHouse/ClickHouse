#include <iostream>
#include <string>

#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/LZ4_decompress_faster.h>

namespace DB
{
    CompressionCodecPtr getCompressionCodecLZ4(int level);
}

struct AuxiliaryRandomData
{
    size_t level;
    size_t decompressed_size;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{

    if (size < sizeof(AuxiliaryRandomData) + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER)
        return 0;

    const auto * p = reinterpret_cast<const AuxiliaryRandomData *>(data);
    auto codec = DB::getCompressionCodecLZ4(p->level);

    size_t output_buffer_size = p->decompressed_size % 65536;
    size -= sizeof(AuxiliaryRandomData);
    size -= LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER;
    data += sizeof(AuxiliaryRandomData) / sizeof(uint8_t);

    // std::string input = std::string(reinterpret_cast<const char*>(data), size);
    // fmt::print(stderr, "Using input {} of size {}, output size is {}. \n", input, size, output_buffer_size);

    DB::Memory<> memory;
    memory.resize(output_buffer_size + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);

    codec->doDecompressData(reinterpret_cast<const char *>(data), size, memory.data(), output_buffer_size);

    return 0;
}
catch (...)
{
    return 1;
}
