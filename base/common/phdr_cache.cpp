/// This code was based on the code by Fedor Korotkiy (prime@yandex-team.ru) for YT product in Yandex.
#include "phdr_cache.h"

#include "defines.h"

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <vector>

#include <dlfcn.h>
#include <link.h>

#if defined(OS_LINUX) && !defined(THREAD_SANITIZER)
#    define USE_PHDR_CACHE 1
#endif

#define __msan_unpoison(X, Y)

#if defined(MEMORY_SANITIZER)
#    undef __msan_unpoison
#    include <sanitizer/msan_interface.h>
#endif

/// Thread Sanitizer uses dl_iterate_phdr function on initialization and fails if we provide our own.
#if defined(USE_PHDR_CACHE)

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


using PHDRCache = std::vector<dl_phdr_info>;
std::atomic<PHDRCache *> phdr_cache {};

}


extern "C"
#    if !defined(__clang__)
    [[gnu::visibility("default")]] [[gnu::externally_visible]]
#    endif
    int
    dl_iterate_phdr(int (*callback)(dl_phdr_info * info, size_t size, void * data), void * data)
{
    auto current_phdr_cache = phdr_cache.load();
    if (!current_phdr_cache)
    {
        // Cache is not yet populated, pass through to the original function.
        return getOriginalDLIteratePHDR()(callback, data);
    }

    int result = 0;
    for (auto & entry : *current_phdr_cache)
    {
        result = callback(&entry, offsetof(dl_phdr_info, dlpi_adds), data);
        if (result != 0)
            break;
    }
    return result;
}


extern "C"
{
#    if defined(ADDRESS_SANITIZER)
void __lsan_ignore_object(const void *);
#    else
void __lsan_ignore_object(const void *)
{
}
#    endif
}


void updatePHDRCache()
{
    // Fill out ELF header cache for access without locking.
    // This assumes no dynamic object loading/unloading after this point

    PHDRCache * new_phdr_cache = new PHDRCache;
    getOriginalDLIteratePHDR()([] (dl_phdr_info * info, size_t /*size*/, void * data)
    {
        // `info` is created by dl_iterate_phdr, which is a non-instrumented
        // libc function, so we have to unpoison it manually.
        __msan_unpoison(info, sizeof(*info));

        reinterpret_cast<PHDRCache *>(data)->push_back(*info);
        return 0;
    }, new_phdr_cache);
    phdr_cache.store(new_phdr_cache);

    /// Memory is intentionally leaked.
    __lsan_ignore_object(new_phdr_cache);
}


bool hasPHDRCache()
{
    return phdr_cache.load() != nullptr;
}

#else

void updatePHDRCache() {}
bool hasPHDRCache() { return false; }

#endif
