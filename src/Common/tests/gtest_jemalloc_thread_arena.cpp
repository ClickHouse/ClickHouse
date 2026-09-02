#include "config.h"

#if USE_JEMALLOC

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include <Common/Jemalloc.h>

using namespace DB;

/// Owning arena of a pointer, from jemalloc's per-extent metadata.
static unsigned owningArena(void * ptr)
{
    unsigned arena = 0;
    size_t arena_size = sizeof(arena);
    EXPECT_EQ(je_mallctl("arenas.lookup", &arena, &arena_size, &ptr, sizeof(ptr)), 0);
    return arena;
}

/// Both tests create a dedicated arena and force the thread cache on, and can bail out on a
/// failed assertion at any point, with dedicated-arena chunks still parked in the thread cache.
class ScopedJemallocThreadArenaTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        tcache_was_enabled = Jemalloc::getValue<bool>("thread.tcache.enabled");
        /// The tests rely on the thread cache.
        Jemalloc::setValue("thread.tcache.enabled", true);

        /// Sampled small allocations are promoted to large and would skew the stats below.
        bool prof_off = false;
        size_t prof_size = sizeof(prof_was_active);
        prof_active_saved = je_mallctl("thread.prof.active", &prof_was_active, &prof_size, &prof_off, sizeof(prof_off)) == 0;

        size_t arena_size = sizeof(arena);
        ASSERT_EQ(je_mallctl("arenas.create", &arena, &arena_size, nullptr, 0), 0);
        arena_created = true;
    }

    void TearDown() override
    {
        /// Disabling the cache also flushes it, so no dedicated-arena chunks leak to later tests.
        Jemalloc::setValue("thread.tcache.enabled", false);
        Jemalloc::setValue("thread.tcache.enabled", tcache_was_enabled);

        if (prof_active_saved && prof_was_active)
            Jemalloc::setValue("thread.prof.active", true);

        /// A failure message may hold a cached chunk of this arena; destroy would dangle it.
        if (arena_created && !HasFailure())
            EXPECT_EQ(je_mallctl(fmt::format("arena.{}.destroy", arena).c_str(), nullptr, nullptr, nullptr, 0), 0);
    }

    unsigned arena = 0;
    bool arena_created = false;
    bool tcache_was_enabled = true;
    bool prof_was_active = false;
    bool prof_active_saved = false;
};

/// The scope must bypass a warm thread cache: cached chunks belong to other arenas.
/// Warm the cache, allocate inside the scope, check the arena grew by the full batch.
TEST_F(ScopedJemallocThreadArenaTest, BypassesThreadCache)
{
    if (!Jemalloc::getValue<bool>("config.stats"))
        GTEST_SKIP() << "jemalloc built without stats";

    /// An exact small size class, so stats grow by exactly this amount per allocation.
    constexpr size_t alloc_size = 128;
    constexpr size_t count = 100;

    /// Warm the cache from the default arena; with an empty cache the bug is invisible.
    std::vector<void *> ptrs(count);
    for (auto & ptr : ptrs)
        ptr = je_malloc(alloc_size);
    for (auto * ptr : ptrs)
        je_free(ptr);

    const auto small_allocated = [this]
    {
        uint64_t epoch = 1;
        je_mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch));
        return Jemalloc::getValue<size_t>(fmt::format("stats.arenas.{}.small.allocated", arena).c_str());
    };

    const size_t before = small_allocated();
    {
        ScopedJemallocThreadArena scope(arena);
        for (auto & ptr : ptrs)
            ptr = je_malloc(alloc_size);
    }
    const size_t after = small_allocated();

    /// Bypass the cache: a failure message below could pick up a parked arena chunk.
    for (auto * ptr : ptrs)
        je_sdallocx(ptr, alloc_size, MALLOCX_TCACHE_NONE);

    EXPECT_GE(after, before + count * alloc_size);
    /// The scope must restore the cache state it captured on entry.
    EXPECT_TRUE(Jemalloc::getValue<bool>("thread.tcache.enabled"));
}

/// Chunks freed after the scope park in the thread cache and would be reused by unrelated
/// code outside any scope. The next scope entry must flush them back to the arena.
TEST_F(ScopedJemallocThreadArenaTest, EntryEvictsArenaChunksFromThreadCache)
{
    constexpr size_t alloc_size = 128;
    constexpr size_t count = 100;

    std::vector<void *> ptrs(count);
    {
        ScopedJemallocThreadArena scope(arena);
        for (auto & ptr : ptrs)
            ptr = je_malloc(alloc_size);
    }
    ASSERT_EQ(owningArena(ptrs.front()), arena);

    /// Freed outside the scope: the chunks land in this thread's cache, not back in the arena.
    for (auto * ptr : ptrs)
        je_free(ptr);

    /// An allocation outside any scope now reuses the most recently freed chunk.
    void * unscoped = je_malloc(alloc_size);
    const unsigned unscoped_arena = owningArena(unscoped);
    je_free(unscoped);

    {
        ScopedJemallocThreadArena scope(arena);
    }

    /// The thread-cache GC may evict the parked chunks first. Checked only after the flush
    /// above: a message allocated earlier could itself hold a parked arena chunk.
    if (unscoped_arena != arena)
        GTEST_SKIP() << "thread cache GC evicted the parked chunks first";

    /// The scope entry above flushed the cache, so this refills from the thread's own arena.
    void * clean = je_malloc(alloc_size);
    EXPECT_NE(owningArena(clean), arena);
    je_free(clean);
}

#endif
