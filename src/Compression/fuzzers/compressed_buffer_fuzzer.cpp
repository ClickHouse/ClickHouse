#include <Common/Arena.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>

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
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        DB::ReadBufferFromMemory from(data, size);
        DB::CompressedReadBuffer in{from};

        while (!in.eof())
            in.next();
    }
    catch (...)
    {
    }

    return 0;
}
