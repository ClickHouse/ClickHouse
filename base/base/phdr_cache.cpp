#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

/// This code was based on the code by Fedor Korotkiy https://www.linkedin.com/in/fedor-korotkiy-659a1838/

#include <base/defines.h>

#if defined(OS_LINUX) && !defined(THREAD_SANITIZER) && !defined(USE_MUSL)
    #define USE_PHDR_CACHE 1
#endif

/// Thread Sanitizer uses dl_iterate_phdr function on initialization and fails if we provide our own.
#ifdef USE_PHDR_CACHE

#if defined(__clang__)
#   pragma clang diagnostic ignored "-Wreserved-id-macro"
#   pragma clang diagnostic ignored "-Wunused-macros"
#endif

#define __msan_unpoison(X, Y) // NOLINT
#if defined(ch_has_feature)
#    if ch_has_feature(memory_sanitizer)
#        undef __msan_unpoison
#        include <sanitizer/msan_interface.h>
#    endif
#endif

#include <link.h>
#include <dlfcn.h>
#include <vector>
#include <atomic>
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


using PHDRCache = std::vector<dl_phdr_info>;
std::atomic<PHDRCache *> phdr_cache {};

}


extern "C"
#ifndef __clang__
[[gnu::visibility("default")]]
[[gnu::externally_visible]]
#endif
int dl_iterate_phdr(int (*callback) (dl_phdr_info * info, size_t size, void * data), void * data)
{
    auto * current_phdr_cache = phdr_cache.load();
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
#ifdef ADDRESS_SANITIZER
void __lsan_ignore_object(const void *);
#else
void __lsan_ignore_object(const void *) {} // NOLINT
#endif
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

#if defined(USE_MUSL)
    /// With statically linked with musl, dl_iterate_phdr is immutable.
    bool hasPHDRCache() { return true; }
#else
    bool hasPHDRCache() { return false; }
#endif

#endif
