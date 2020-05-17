#pragma once

#include <Common/MemoryTracker.h>
#include <Common/thread_local_rng.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>

#include <sys/mman.h>

#include <pcg_random.hpp>

/// Old Darwin builds lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#   define MAP_ANONYMOUS MAP_ANON
#endif

/**
 * @anchor mmap_populate_desc
 *
 * Freshly mmapped pages are copy-on-write references to a global zero page. On the first write, a page fault occurs,
 * and an actual writable page is allocated. If we are going to use this memory soon, such as when resizing hash
 * tables, it makes sense to pre-fault the pages by passing MAP_POPULATE to mmap(). This takes some time, but should be
 * faster overall than having a hot loop interrupted by page faults.
 *
 * @warning It is only supported on Linux.
 */
#ifdef MAP_POPULATE
//#   pragma message("Allocators: using MMAP_POPULATE")
    constexpr const int PossibleMmapPopulate = MAP_POPULATE;
#else
//#   pragma message("Allocators: no MMAP_POPULATE")
    constexpr const int PossibleMmapPopulate = 0;
#endif

/// Thread and memory sanitizers do not intercept mremap. The usage of mremap will lead to false positives.
#if defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER)
//#   pragma message("Allocators: no mremap")
#   define DISABLE_MREMAP 1
#else
//#   pragma message("Allocators: using mremap")
#endif

/// In debug builds, request mmap() at random addresses (a kind of ASLR) to reproduce more memory stomping bugs.
/// @note Linux doesn't do it by default as it may lead to worse TLB performance.
#ifdef NDEBUG
//#   pragma message("Allocators: no ASLR")
#   define ALLOCATOR_ASLR 0
#else
//#   pragma message("Allocators: using ASLR")
#   define ALLOCATOR_ASLR 1
#endif

/**
 * @anchor mremap_desc
 *
 * Many modern allocators (for example, @e tcmalloc) do not do a mremap for realloc, even in case large chunks of
 * memory (although this allows you to increase performance and reduce memory consumption during realloc). To fix this,
 * we do mremap manually if the chunk of memory is large enough.
 *
 * @note This is also required, because tcmalloc can't allocate a chunk of memory greater than 16 GB.
 *
 * @note MMAP_THRESHOLD symbol is intentionally made weak. It allows to override it during linkage when using
 *       ClickHouse as a library in third-party applications which may already use own allocator doing mmaps in the
 *       implementation of alloc/realloc.
 *
 * @note This parameter can be used not only for mremap, e.g. IGrabberAllocator uses it to determine minimum chunk size.
 */
#ifdef NDEBUG
//#   pragma message("Allocators: using 64MB MMAP_THRESHOLD")
    /**
     * The threshold (64 MB) is chosen quite large, since changing the address
     * space is very slow, especially in the case of a large number of threads. We
     * expect that the set of operations mmap/something to do/mremap can only be
     * performed about 1000 times per second.
     */
    [[gnu::weak]] extern constexpr const size_t MMAP_THRESHOLD = 64 * (1ULL << 20);
#else
//#   pragma message("Allocators: using 4KB MMAP_THRESHOLD")
    /**
     * In debug build, use small mmap threshold to reproduce more memory
     * stomping bugs. Along with ASLR it will hopefully detect more issues than
     * ASan. The program may fail due to the limit on number of memory mappings.
     */
    [[gnu::weak]] extern constexpr const size_t MMAP_THRESHOLD = 4096;

#endif

/**
 * If on, provides hints to mmap (first parameter).
 */
struct AllocatorsASLR
{
#if ALLOCATOR_ASLR == 1
    [[nodiscard, gnu::const]] void * operator()(pcg64& rng = thread_local_rng) const noexcept
    {
        return reinterpret_cast<void *>(
                std::uniform_int_distribution<size_t>(
                    0x100000000000UL,
                    0x700000000000UL)(rng));
    }
#else
    [[nodiscard, gnu::const]] constexpr void * operator()(pcg64&) const noexcept
    {
        return nullptr;
    }

    [[nodiscard, gnu::const]] constexpr void * operator()() const noexcept
    {
        return nullptr;
    }

#endif
};

