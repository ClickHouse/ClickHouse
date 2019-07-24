/// This code was developed by Fedor Korotkiy (prime@yandex-team.ru) for YT product in Yandex.

#include <link.h>
#include <dlfcn.h>
#include <array>
#include <cstddef>
#include <stdexcept>


namespace
{

// This is adapted from
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.hh
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.cc

using DLIterateFunction = int (*) (int (*callback) (dl_phdr_info * info, size_t size, void * data), void * data);

DLIterateFunction getOriginalDLIteratePHDR()
{
    void * func = dlsym(RTLD_NEXT, "dl_iterate_phdr");
    if (!func)
        throw std::runtime_error("Cannot find dl_iterate_phdr function with dlsym");
    return reinterpret_cast<DLIterateFunction>(func);
}


constexpr size_t phdr_cache_max_size = 128;
size_t phdr_cache_size = 0;
using PHDRCache = std::array<dl_phdr_info, phdr_cache_max_size>;
PHDRCache phdr_cache;

}


extern "C"
#ifndef __clang__
[[gnu::visibility("default")]]
[[gnu::externally_visible]]
#endif
int dl_iterate_phdr(int (*callback) (dl_phdr_info * info, size_t size, void * data), void * data)
{
    if (0 == phdr_cache_size)
    {
        // Cache is not yet populated, pass through to the original function.
        return getOriginalDLIteratePHDR()(callback, data);
    }

    int result = 0;
    for (size_t i = 0; i < phdr_cache_size; ++i)
    {
        result = callback(&phdr_cache[i], offsetof(dl_phdr_info, dlpi_adds), data);
        if (result != 0)
            break;
    }
    return result;
}


void enablePHDRCache()
{
#if defined(__linux__)
    // Fill out ELF header cache for access without locking.
    // This assumes no dynamic object loading/unloading after this point
    getOriginalDLIteratePHDR()([] (dl_phdr_info * info, size_t /*size*/, void * /*data*/)
    {
        if (phdr_cache_size >= phdr_cache_max_size)
            throw std::runtime_error("phdr_cache_max_size is not enough to accomodate the result of dl_iterate_phdr. You must recompile the code.");
        phdr_cache[phdr_cache_size] = *info;
        ++phdr_cache_size;
        return 0;
    }, nullptr);
#endif
}
