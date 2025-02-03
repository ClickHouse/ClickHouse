#include <string>

#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
    CompressionCodecPtr getCompressionCodecDoubleDelta(UInt8 data_bytes_size);
}

struct AuxiliaryRandomData
{
    UInt8 data_bytes_size;
    size_t decompressed_size;
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        if (size < sizeof(AuxiliaryRandomData))
            return 0;

        const auto * p = reinterpret_cast<const AuxiliaryRandomData *>(data);
        auto codec = DB::getCompressionCodecDoubleDelta(p->data_bytes_size);

        size_t output_buffer_size = p->decompressed_size % 65536;
        size -= sizeof(AuxiliaryRandomData);
        data += sizeof(AuxiliaryRandomData) / sizeof(uint8_t);

        // std::string input = std::string(reinterpret_cast<const char*>(data), size);
        // fmt::print(stderr, "Using input {} of size {}, output size is {}. \n", input, size, output_buffer_size);

        DB::Memory<> memory;
        memory.resize(output_buffer_size + codec->getAdditionalSizeAtTheEndOfBuffer());

        codec->doDecompressData(reinterpret_cast<const char *>(data), static_cast<UInt32>(size), memory.data(), static_cast<UInt32>(output_buffer_size));
    }
    catch (...)
    {
    }

    return 0;
}
