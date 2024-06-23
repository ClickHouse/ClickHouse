#include <iostream>
#include <string>

#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include "base/types.h"

namespace DB
{
    CompressionCodecPtr getCompressionCodecGCD(UInt8 gcd_bytes_size);
}

struct AuxiliaryRandomData
{
    UInt8 gcd_size_bytes;
    size_t decompressed_size;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    if (size < sizeof(AuxiliaryRandomData))
        return 0;

    const auto * p = reinterpret_cast<const AuxiliaryRandomData *>(data);
    auto codec = DB::getCompressionCodecGCD(p->gcd_size_bytes);

    size_t output_buffer_size = p->decompressed_size % 65536;
    size -= sizeof(AuxiliaryRandomData);
    data += sizeof(AuxiliaryRandomData) / sizeof(uint8_t);

    // std::string input = std::string(reinterpret_cast<const char*>(data), size);
    // fmt::print(stderr, "Using input {} of size {}, output size is {}. \n", input, size, output_buffer_size);

    DB::Memory<> memory;
    memory.resize(output_buffer_size + codec->getAdditionalSizeAtTheEndOfBuffer());

    codec->doDecompressData(reinterpret_cast<const char *>(data), static_cast<UInt32>(size), memory.data(), static_cast<UInt32>(output_buffer_size));

    return 0;
}
catch (...)
{
    return 1;
}
