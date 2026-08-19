#if defined(OS_LINUX)
#include <cstdlib>

/// Interposing these symbols explicitly. The idea works like this: malloc.cpp compiles to a
/// dedicated object (namely clickhouse_malloc.o), and it will show earlier in the link command
/// than malloc libs like libjemalloc.a. As a result, these symbols get picked in time right after.

extern "C"
{
    void *malloc(size_t size);
    void free(void *ptr);
    void *calloc(size_t nmemb, size_t size);
    void *realloc(void *ptr, size_t size);
    int posix_memalign(void **memptr, size_t alignment, size_t size);
    void *aligned_alloc(size_t alignment, size_t size);
    void *valloc(size_t size);
    void *memalign(size_t alignment, size_t size);
#if !defined(USE_MUSL)
    void *pvalloc(size_t size);
#endif
}

template<typename T>
inline void ignore(T x __attribute__((unused)))
{
}

static void dummyFunctionForInterposing() __attribute__((used));
static void dummyFunctionForInterposing()
{
    void* dummy;
    free(nullptr); // NOLINT
    ignore(malloc(0)); // NOLINT
    ignore(calloc(0, 0)); // NOLINT
    ignore(realloc(nullptr, 0)); // NOLINT
    ignore(posix_memalign(&dummy, 0, 0)); // NOLINT
    ignore(aligned_alloc(1, 0)); // NOLINT
    ignore(valloc(0)); // NOLINT
    ignore(memalign(0, 0)); // NOLINT
#if !defined(USE_MUSL)
    ignore(pvalloc(0)); // NOLINT
#endif
}
#endif
