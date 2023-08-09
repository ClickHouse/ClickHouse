#include <Common/memory.h>
#include <cstdlib>


/** These functions can be substituted instead of regular ones when memory tracking is needed.
  */

extern "C" void * clickhouse_malloc(size_t size)
{
    void * res = malloc(size);
    if (res)
        Memory::trackMemory(size);
    return res;
}

extern "C" void * clickhouse_calloc(size_t number_of_members, size_t size)
{
    void * res = calloc(number_of_members, size);
    if (res)
        Memory::trackMemory(number_of_members * size);
    return res;
}

extern "C" void * clickhouse_realloc(void * ptr, size_t size)
{
    if (ptr)
        Memory::untrackMemory(ptr);
    void * res = realloc(ptr, size);
    if (res)
        Memory::trackMemory(size);
    return res;
}

extern "C" void * clickhouse_reallocarray(void * ptr, size_t number_of_members, size_t size)
{
    size_t real_size = 0;
    if (__builtin_mul_overflow(number_of_members, size, &real_size))
        return nullptr;

    return clickhouse_realloc(ptr, real_size);
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
