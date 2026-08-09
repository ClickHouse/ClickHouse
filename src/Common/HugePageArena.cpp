#include <Common/HugePageArena.h>

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <vector>

#if USE_JEMALLOC

#include <sys/mman.h>
#include <base/defines.h>
#include <base/getPageSize.h>
#include <Common/logger_useful.h>
#include <jemalloc/jemalloc.h>

namespace DB::HugePageArena
{

namespace
{

constexpr size_t huge_page_size = 2 * 1024 * 1024;
/// Address space is cheap and only faulted in on touch, so reserve coarsely: this is the unit that
/// costs one `madvise` and one VMA.
constexpr size_t chunk_size = 1024 * 1024 * 1024;

size_t roundUp(size_t value, size_t multiple)
{
    return ((value + multiple - 1) / multiple) * multiple;
}

/** Reserves address space in large chunks, each marked with a single `madvise(MADV_HUGEPAGE)`, and
  * hands out aligned pieces of it. Only ever bumps: extents handed to jemalloc are never given
  * back, because jemalloc recycles them itself and the dalloc hook retains them.
  *
  * Allocates nothing on the heap. This runs inside jemalloc's extent hooks, which are called with
  * arena locks held, so anything that reached back into the allocator - a `std::vector` growing, a
  * log message formatting - risks recursing into jemalloc or deadlocking. Hence the fixed array and
  * the absence of logging here. 4096 chunks is 4 TiB of reservable address space.
  */
class ChunkPool
{
public:
    void * carve(size_t size, size_t alignment)
    {
        /// Always at least huge-page aligned, otherwise the kernel cannot back the range with huge
        /// pages however the chunk is marked.
        alignment = std::max(alignment, huge_page_size);

        std::lock_guard lock(mutex);

        if (void * result = carveFromChunks(size, alignment))
            return result;

        const size_t needed = roundUp(size + alignment, huge_page_size);
        if (!addChunk(std::max(needed, chunk_size)))
            return nullptr;

        return carveFromChunks(size, alignment);
    }

    Stats getStats() const
    {
        std::lock_guard lock(mutex);
        Stats stats;
        stats.chunks = chunk_count;
        stats.failed_madvise = failed_madvise;
        for (size_t i = 0; i < chunk_count; ++i)
        {
            stats.reserved_bytes += chunks[i].size;
            stats.carved_bytes += chunks[i].used;
        }
        return stats;
    }

private:
    struct Chunk
    {
        char * base = nullptr;
        size_t size = 0;
        size_t used = 0;
    };

    void * carveFromChunks(size_t size, size_t alignment)
    {
        for (size_t i = 0; i < chunk_count; ++i)
        {
            Chunk & chunk = chunks[i];
            const size_t offset = roundUp(reinterpret_cast<uintptr_t>(chunk.base) + chunk.used, alignment)
                - reinterpret_cast<uintptr_t>(chunk.base);
            if (offset + size > chunk.size)
                continue;

            chunk.used = offset + size;
            return chunk.base + offset;
        }
        return nullptr;
    }

    bool addChunk(size_t size)
    {
        if (chunk_count >= max_chunks)
            return false;

        size = roundUp(size, huge_page_size);

        /// `MAP_NORESERVE` because most of a chunk stays untouched; pages are faulted in on use.
        void * base = ::mmap(nullptr, size, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (base == MAP_FAILED)
            return false;

        /// The one call that makes the whole chunk eligible. Doing this per allocation instead is
        /// what splits the VMA, so it happens exactly once per chunk and never again.
        /// Not fatal - the arena still works, it just will not get huge pages. Counted rather than
        /// logged, because logging here would allocate.
        if (::madvise(base, size, MADV_HUGEPAGE) != 0)
            ++failed_madvise;

        chunks[chunk_count++] = {static_cast<char *>(base), size, 0};
        return true;
    }

    static constexpr size_t max_chunks = 4096;

    mutable std::mutex mutex;
    Chunk chunks[max_chunks];
    size_t chunk_count = 0;
    size_t failed_madvise = 0;
};

ChunkPool & chunkPool()
{
    static ChunkPool pool;
    return pool;
}

/// jemalloc asks for new address space only when the arena cannot satisfy a request from extents it
/// already holds, so this is called rarely - once per chunk's worth of growth, not per allocation.
void * hookAlloc(
    extent_hooks_t *, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned)
{
    /// A fixed-address request cannot be served from a bump-allocated pool.
    if (new_addr != nullptr)
        return nullptr;

    void * result = chunkPool().carve(size, alignment);
    if (result == nullptr)
        return nullptr;

    /// Freshly reserved address space is never handed out twice, so it is still zero-filled, and it
    /// is mapped read-write up front.
    *zero = true;
    *commit = true;
    return result;
}

/// Returning true means "not deallocated": jemalloc keeps the extent and reuses it inside this
/// arena. That is what confines huge-page-marked memory to hash tables.
bool hookDalloc(extent_hooks_t *, void *, size_t, bool, unsigned)
{
    return true;
}

void hookDestroy(extent_hooks_t *, void *, size_t, bool, unsigned)
{
    /// The arena lives for the process lifetime; the address space stays reserved and reusable.
}

bool hookCommit(extent_hooks_t *, void *, size_t, size_t, size_t, unsigned)
{
    /// Already mapped read-write. false means success.
    return false;
}

bool hookDecommit(extent_hooks_t *, void *, size_t, size_t, size_t, unsigned)
{
    /// Unsupported: decommitting would mean unmapping, which would give the address space back.
    return true;
}

bool hookPurgeForced(extent_hooks_t *, void * addr, size_t, size_t offset, size_t length, unsigned)
{
    /// Safe with respect to the marking: `MADV_DONTNEED` does not touch `vm_flags`, so it frees the
    /// physical pages without splitting the VMA or clearing `VM_HUGEPAGE`, and the range faults
    /// back in as huge pages.
    return ::madvise(static_cast<char *>(addr) + offset, length, MADV_DONTNEED) != 0;
}

bool hookPurgeLazy(extent_hooks_t *, void *, size_t, size_t, size_t, unsigned)
{
    /// Unsupported, so jemalloc falls back to the forced variant above. `MADV_FREE` would leave the
    /// pages resident until reclaim, which hides the memory cost we want to be able to measure.
    return true;
}

bool hookSplit(extent_hooks_t *, void *, size_t, size_t, size_t, bool, unsigned)
{
    /// Splitting is bookkeeping inside jemalloc; it does not touch the mapping. false means success.
    return false;
}

bool hookMerge(extent_hooks_t *, void *, size_t, void *, size_t, bool, unsigned)
{
    return false;
}

extent_hooks_t custom_hooks = {
    .alloc = &hookAlloc,
    .dalloc = &hookDalloc,
    .destroy = &hookDestroy,
    .commit = &hookCommit,
    .decommit = &hookDecommit,
    .purge_lazy = &hookPurgeLazy,
    .purge_forced = &hookPurgeForced,
    .split = &hookSplit,
    .merge = &hookMerge,
};

struct ArenaHolder
{
    unsigned index = 0;
    bool enabled = false;

    ArenaHolder()
    {
        const char * env = std::getenv("CH_HASHTABLE_HUGE_PAGES"); /// NOLINT(concurrency-mt-unsafe)
        if (env == nullptr || env[0] == '0' || env[0] == '\0')
            return;

        extent_hooks_t * hooks = &custom_hooks;
        size_t size = sizeof(index);
        if (je_mallctl("arenas.create", &index, &size, &hooks, sizeof(hooks)) != 0)
        {
            LOG_WARNING(getLogger("HugePageArena"), "Could not create a jemalloc arena, huge pages for hash tables are disabled");
            return;
        }

        enabled = true;
        LOG_INFO(getLogger("HugePageArena"), "Hash-table huge pages enabled, jemalloc arena {}", index);
    }
};

const ArenaHolder & arena()
{
    static const ArenaHolder holder;
    return holder;
}

int flagsFor(size_t alignment, bool zeroed)
{
    /// `MALLOCX_TCACHE_NONE` keeps these allocations from being cached per thread, which would let
    /// them escape the arena's accounting; they are large and rare, so the cache is no use anyway.
    int flags = MALLOCX_ARENA(arena().index) | MALLOCX_TCACHE_NONE;
    if (alignment > 1)
        flags |= MALLOCX_ALIGN(std::max(alignment, huge_page_size));
    else
        flags |= MALLOCX_ALIGN(huge_page_size);
    if (zeroed)
        flags |= MALLOCX_ZERO;
    return flags;
}

}

bool isEnabled()
{
    return arena().enabled;
}

void * allocate(size_t size, size_t alignment, bool zeroed)
{
    return je_mallocx(size, flagsFor(alignment, zeroed));
}

void * reallocate(void * buf, size_t new_size, size_t alignment, bool zeroed)
{
    /// Keeps the buffer in this arena across a resize. Plain `realloc` would move it to the calling
    /// thread's arena instead, quietly losing the huge pages after the first growth.
    return je_rallocx(buf, new_size, flagsFor(alignment, zeroed));
}

void deallocate(void * buf)
{
    je_dallocx(buf, MALLOCX_ARENA(arena().index) | MALLOCX_TCACHE_NONE);
}

Stats getStats()
{
    return chunkPool().getStats();
}

}

#else

namespace DB::HugePageArena
{

bool isEnabled() { return false; }
void * allocate(size_t, size_t, bool) { return nullptr; }
void * reallocate(void *, size_t, size_t, bool) { return nullptr; }
void deallocate(void *) { }
Stats getStats() { return {}; }

}

#endif
