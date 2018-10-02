#pragma once

#include <cstddef>
#include <sys/types.h>
#if !_MSC_VER
#include <sys/mman.h>
#endif

#if defined(MREMAP_MAYMOVE)
// we already have implementation (linux)
#else

#define MREMAP_MAYMOVE 1

void * mremap(void * old_address,
    size_t old_size,
    size_t new_size,
    int flags = 0,
    int mmap_prot = 0,
    int mmap_flags = 0,
    int mmap_fd = -1,
    off_t mmap_offset = 0);

#endif

inline void * clickhouse_mremap(void * old_address,
    size_t old_size,
    size_t new_size,
    int flags = 0,
    [[maybe_unused]] int mmap_prot = 0,
    [[maybe_unused]] int mmap_flags = 0,
    [[maybe_unused]] int mmap_fd = -1,
    [[maybe_unused]] off_t mmap_offset = 0)
{
    return mremap(old_address,
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
}
