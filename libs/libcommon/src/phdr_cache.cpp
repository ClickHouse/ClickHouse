/// This code was developed by Fedor Korotkiy (prime@yandex-team.ru) for YT product in Yandex.

#include <link.h>
#include <dlfcn.h>
#include <assert.h>
#include <vector>
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

// Never destroyed to avoid races with static destructors.
using PHDRCache = std::vector<dl_phdr_info>;
PHDRCache * phdr_cache;

}


extern "C"
#ifndef __clang__
[[gnu::visibility("default")]]
[[gnu::externally_visible]]
#endif
int dl_iterate_phdr(int (*callback) (dl_phdr_info * info, size_t size, void * data), void * data)
{
    if (!phdr_cache)
    {
        // Cache is not yet populated, pass through to the original function.
        return getOriginalDLIteratePHDR()(callback, data);
    }

    int result = 0;
    for (auto & info : *phdr_cache)
    {
        result = callback(&info, offsetof(dl_phdr_info, dlpi_adds), data);
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
    phdr_cache = new PHDRCache;
    getOriginalDLIteratePHDR()([] (dl_phdr_info * info, size_t /*size*/, void * /*data*/)
    {
        phdr_cache->push_back(*info);
        return 0;
    }, nullptr);
#endif
}
