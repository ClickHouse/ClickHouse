#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <common/getMemoryAmount.h>
#include <Common/formatReadable.h>
#include <sys/mman.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_ALLOCATE_MEMORY;
    }
}

using namespace DB;

int mainEntryClickHouseEatAllMemory(int, char **)
{
    const char * oom_score = "1000";

    WriteBufferFromFile buf("/proc/self/oom_score_adj");
    writeCString(oom_score, buf);

    size_t total_memory_amount = getMemoryAmount();

    void * buf = mmap(nullptr, total_memory_amount, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buf == MAP_FAILED)
        throwFromErrno("Cannot mmap " + formatReadableSizeWithBinarySuffix(total_memory_amount) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

    char * begin = buf;
    char * end = begin + total_memory_amount;
    size_t page_size = 4096;

    for (auto ptr = begin; ptr < end; ptr += page_size)
        *ptr = 1;

    return 0;
}
