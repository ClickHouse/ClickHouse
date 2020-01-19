#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <common/getMemoryAmount.h>
#include <Common/formatReadable.h>
#include <sys/mman.h>
#include <vector>
#include <thread>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_ALLOCATE_MEMORY;
    }
}

using namespace DB;

int mainEntryClickHouseDropCaches(int, char **)
{
    {
        const char * oom_score = "1000";
        WriteBufferFromFile oom_score_file("/proc/self/oom_score_adj");
        writeCString(oom_score, oom_score_file);
    }

    const size_t total_memory_amount = getMemoryAmount();
    const size_t step = 1ULL << 30;

    std::vector<std::thread> threads;
    for (size_t allocated = 0; allocated < total_memory_amount; allocated += step)
    {
        threads.emplace_back([=]
        {
            void * buf = mmap(nullptr, step, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (buf == MAP_FAILED)
                throwFromErrno("Cannot mmap " + formatReadableSizeWithBinarySuffix(total_memory_amount) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

            char * begin = static_cast<char *>(buf);
            char * end = begin + step;
            size_t page_size = 4096;

            for (auto ptr = begin; ptr < end; ptr += page_size)
                *ptr = 1;
        });
    }

    for (auto & thread : threads)
        thread.join();

    return 0;
}
