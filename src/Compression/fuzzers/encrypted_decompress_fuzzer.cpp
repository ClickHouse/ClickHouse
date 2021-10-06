#include <iostream>
#include <string>

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionCodecEncrypted.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
    CompressionCodecPtr getCompressionCodecEncrypted(const std::string_view & master_key);
}

constexpr size_t key_size = 20;

struct AuxiliaryRandomData
{
    char key[key_size];
    size_t decompressed_size;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    if (size < sizeof(AuxiliaryRandomData))
        return 0;

    const auto * p = reinterpret_cast<const AuxiliaryRandomData *>(data);

    std::string key = std::string(p->key, key_size);
    auto codec = DB::getCompressionCodecEncrypted(key);

    size_t output_buffer_size = p->decompressed_size % 65536;
    size -= sizeof(AuxiliaryRandomData);
    data += sizeof(AuxiliaryRandomData) / sizeof(uint8_t);

    std::string input = std::string(reinterpret_cast<const char*>(data), size);
    fmt::print(stderr, "Using input {} of size {}, output size is {}. \n", input, size, output_buffer_size);

    if (output_buffer_size < size)
        return 0;

    DB::Memory<> memory;
    memory.resize(output_buffer_size + codec->getAdditionalSizeAtTheEndOfBuffer());

    codec->doDecompressData(reinterpret_cast<const char *>(data), size, memory.data(), output_buffer_size);

    return 0;
}
catch (...)
{
    return 1;
}
