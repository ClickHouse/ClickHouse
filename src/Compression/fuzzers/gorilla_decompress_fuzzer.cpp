#include <string>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <base/types.h>

struct AuxiliaryRandomData
{
    size_t decompressed_size;
};

using namespace DB;
ContextMutablePtr context;
extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    total_memory_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1_GiB);
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

    if (size < sizeof(AuxiliaryRandomData))
        return 0;

    const auto * p = reinterpret_cast<const AuxiliaryRandomData *>(data);

    /// Gorilla reads data_bytes_size from source[0] during decompression,
    /// so the construction parameter does not affect decompression code paths.
    auto codec = CompressionCodecFactory::instance().get(
        static_cast<uint8_t>(CompressionMethodByte::Gorilla));

    size_t output_buffer_size = p->decompressed_size % 65536;
    size -= sizeof(AuxiliaryRandomData);
    data += sizeof(AuxiliaryRandomData) / sizeof(uint8_t);

    DB::Memory<> memory;
    memory.resize(output_buffer_size + codec->getAdditionalSizeAtTheEndOfBuffer());

    codec->doDecompressData(
        reinterpret_cast<const char *>(data),
        static_cast<UInt32>(size),
        memory.data(),
        static_cast<UInt32>(output_buffer_size));

    return 0;
}
catch (...)
{
    return 1;
}
