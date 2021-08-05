#if 0

#include <Common/memory.h>
#include <iostream>


extern "C" void * clickhouse_malloc(size_t size)
{
    void * res = malloc(size);
    if (res)
        Memory::trackMemory(size);
    return res;
}

extern "C" void clickhouse_free(void * ptr)
{
    Memory::untrackMemory(ptr);
    free(ptr);
}

extern "C" int clickhouse_posix_memalign(void ** memptr, size_t alignment, size_t size)
{
    int res = posix_memalign(memptr, alignment, size);
    if (res == 0)
        Memory::trackMemory(size);
    return res;
}

#endif
