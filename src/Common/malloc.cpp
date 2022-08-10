#if defined(OS_LINUX)
#include <stdlib.h>

/// Interposing these symbols explicitly. The idea works like this: malloc.cpp compiles to a
/// dedicated object (namely clickhouse_malloc.o), and it will show earlier in the link command
/// than malloc libs like libjemalloc.a. As a result, these symbols get picked in time right after.

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wredundant-decls"
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
#pragma GCC diagnostic pop

template<typename T>
inline void ignore(T x __attribute__((unused)))
{
}

static void dummyFunctionForInterposing() __attribute__((used));
static void dummyFunctionForInterposing()
{
    void* dummy;
    /// Suppression for PVS-Studio and clang-tidy.
    free(nullptr); // -V575 NOLINT
    ignore(malloc(0)); // -V575 NOLINT
    ignore(calloc(0, 0)); // -V575 NOLINT
    ignore(realloc(nullptr, 0)); // -V575 NOLINT
    ignore(posix_memalign(&dummy, 0, 0)); // -V575 NOLINT
    ignore(aligned_alloc(1, 0)); // -V575 NOLINT
    ignore(valloc(0)); // -V575 NOLINT
    ignore(memalign(0, 0)); // -V575 NOLINT
#if !defined(USE_MUSL)
    ignore(pvalloc(0)); // -V575 NOLINT
#endif
}
#endif
