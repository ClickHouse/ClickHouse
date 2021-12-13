#pragma once

#include <cstddef>
#include <sys/types.h>
#if !defined(_MSC_VER)
#include <sys/mman.h>
#endif


#ifdef MREMAP_MAYMOVE
    #define HAS_MREMAP 1
#else
    #define HAS_MREMAP 0
#endif


/// You can forcely disable mremap by defining DISABLE_MREMAP to 1 before including this file.
#ifndef DISABLE_MREMAP
    #if HAS_MREMAP
        #define DISABLE_MREMAP 0
    #else
        #define DISABLE_MREMAP 1
    #endif
#endif


/// Implement mremap with mmap/memcpy/munmap.
void * mremap_fallback(
    void * old_address,
    size_t old_size,
    size_t new_size,
    int flags,
    int mmap_prot,
    int mmap_flags,
    int mmap_fd,
    off_t mmap_offset);


#if !HAS_MREMAP
    #define MREMAP_MAYMOVE 1

    inline void * mremap(
        void * old_address,
        size_t old_size,
        size_t new_size,
        int flags = 0,
        int mmap_prot = 0,
        int mmap_flags = 0,
        int mmap_fd = -1,
        off_t mmap_offset = 0)
    {
        return mremap_fallback(old_address, old_size, new_size, flags, mmap_prot, mmap_flags, mmap_fd, mmap_offset);
    }
#endif


inline void * clickhouse_mremap(
    void * old_address,
    size_t old_size,
    size_t new_size,
    int flags = 0,
    [[maybe_unused]] int mmap_prot = 0,
    [[maybe_unused]] int mmap_flags = 0,
    [[maybe_unused]] int mmap_fd = -1,
    [[maybe_unused]] off_t mmap_offset = 0)
{
#if DISABLE_MREMAP
    return mremap_fallback(old_address, old_size, new_size, flags, mmap_prot, mmap_flags, mmap_fd, mmap_offset);
#else

    return mremap(
        old_address,
        old_size,
        new_size,
        flags
#if !defined(MREMAP_FIXED)
        ,
        mmap_prot,
        mmap_flags,
        mmap_fd,
        mmap_offset
#endif
        );
#endif
}
