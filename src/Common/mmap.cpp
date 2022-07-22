#if defined(OS_LINUX)
#include <cstdlib>


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wredundant-decls"
extern "C"
{
    void * mmap(void * /*addr*/, size_t /*length*/, int /*prot*/, int /*flags*/, int /*fd*/, off_t /*offset*/);
    int munmap(void * /*addr*/, size_t /*length*/);
}
#pragma GCC diagnostic pop

template<typename T>
inline void ignore(T x __attribute__((unused)))
{
}

static void dummyFunctionForInterposing() __attribute__((used));
static void dummyFunctionForInterposing()
{
    /// Suppression for PVS-Studio and clang-tidy.
    ignore(mmap(nullptr, 0, 0, 0, 0, 0)); // -V575 NOLINT
    ignore(munmap(nullptr, 0)); // -V575 NOLINT
}
#endif
