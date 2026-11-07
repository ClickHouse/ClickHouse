/// The `experimental.*` leaves through the public API of the fully initialized allocator (the test links API.cpp,
/// whose constructor initializes the allocator with `MALLOC_CONF` from the environment; the test is registered twice:
/// without and with heap profiling). Ports of jemalloc's `test/unit/inspect.c`, `test/unit/batch_alloc.c` (and
/// `batch_alloc_prof.c`), the public API part of `test/unit/safety_check.c` (the redzone checks themselves only exist
/// with `config_opt_safety_checks`, which is never enabled), plus `experimental.arenas.<i>.pactivep`,
/// `experimental.arenas_create_ext` and `experimental.thread.activity_callback`.

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/Frontend.h>
#include <allocator/Options.h>
#include <allocator/Sanitizer.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>

extern "C" {
#include <jemalloc/jemalloc_defs.h>
#include <jemalloc/jemalloc_macros.h>
#include <jemalloc/jemalloc_protos.h>
}

using namespace jemalloc;

namespace
{

/// --- inspect.c ------------------------------------------------------------------------------------------------------

constexpr size_t TEST_MAX_SIZE = 1 << 20;

/// `TEST_UTIL_EINVAL`: the call fails with `EINVAL` and does not touch the output (size and content).
void checkCommonEINVAL(
    const char * node,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length,
    const size_t & out_size,
    size_t out_size_ref,
    const void * out,
    const void * out_ref)
{
    CHECK_EQ(je_mallctl(node, old_value, old_length_ptr, new_value, new_length), EINVAL);
    CHECK_EQ(out_size, out_size_ref);
    CHECK_EQ(memcmp(out, out_ref, out_size_ref), 0);
}

/// `TEST_UTIL_VALID`
void checkCommonValid(
    const char * node, void * out, size_t * out_size, void * in, size_t in_size, size_t out_size_ref, const void * out_ref)
{
    REQUIRE(je_mallctl(node, out, out_size, in, in_size) == 0);
    CHECK_EQ(*out_size, out_size_ref);
    CHECK_NE(memcmp(out, out_ref, out_size_ref), 0);
}

}

/// jemalloc: test/unit/inspect.c test_query
TEST(MallctlExperimentalAPI, UtilizationQuery)
{
    /// jemalloc runs `test/unit/inspect.c` with `profiling:false`: sampled small allocations are not slabs.
    if (options.profiling)
        return;
    const char * node = "experimental.utilization.query";
    /// Select some sizes that can span both small and large sizes, and are numerically unrelated to any size
    /// boundaries.
    for (size_t size_value = 7; size_value <= TEST_MAX_SIZE && size_value <= SIZE_CLASS_LARGE_MAX_CLASS;
         size_value += (size_value <= SIZE_CLASS_SMALL_MAX_CLASS ? 1009 : 99989))
    {
        void * p = je_mallocx(size_value, 0);
        void ** in = &p;
        size_t in_size = sizeof(const void *);
        size_t out_size = sizeof(void *) + sizeof(size_t) * 5;
        void * out = je_mallocx(out_size, 0);
        void * out_ref = je_mallocx(out_size, 0);
        size_t out_size_ref = out_size;
        REQUIRE(p != nullptr && out != nullptr && out_ref != nullptr);

        auto current_slab = [](void * o) -> void *& { return *static_cast<void **>(o); };
        auto counts = [](void * o) { return reinterpret_cast<size_t *>(static_cast<void **>(o) + 1); };

        current_slab(out) = nullptr;
        counts(out)[0] = counts(out)[1] = counts(out)[2] = size_t(-1);
        counts(out)[3] = counts(out)[4] = size_t(-1);
        memcpy(out_ref, out, out_size);

        /// Invalid argument(s) errors.
        checkCommonEINVAL(node, nullptr, &out_size, in, in_size, out_size, out_size_ref, out, out_ref);
        checkCommonEINVAL(node, out, nullptr, in, in_size, out_size, out_size_ref, out, out_ref);
        checkCommonEINVAL(node, out, &out_size, nullptr, in_size, out_size, out_size_ref, out, out_ref);
        checkCommonEINVAL(node, out, &out_size, in, 0, out_size, out_size_ref, out, out_ref);
        in_size -= 1;
        checkCommonEINVAL(node, out, &out_size, in, in_size, out_size, out_size_ref, out, out_ref);
        in_size += 1;
        out_size_ref = out_size -= 2 * sizeof(size_t);
        checkCommonEINVAL(node, out, &out_size, in, in_size, out_size, out_size_ref, out, out_ref);
        out_size_ref = out_size += 2 * sizeof(size_t);

        /// The output of a valid call.
        checkCommonValid(node, out, &out_size, in, in_size, out_size_ref, out_ref);
        size_t num_free = counts(out)[0];
        size_t num_regions = counts(out)[1];
        size_t size = counts(out)[2];
        size_t bin_num_free = counts(out)[3];
        size_t bin_num_regions = counts(out)[4];
        CHECK_LE(size_value, size);
        CHECK_EQ(size & (PAGE - 1), size_t(0));

        /// We don't do much bin checking if prof is on, since profiling can produce extents that are for small size
        /// classes but not slabs, which interferes with things like region counts.
        if (!options.profiling && size_value <= SIZE_CLASS_SMALL_MAX_CLASS)
        {
            CHECK_LE(num_free, num_regions);
            CHECK_LE(num_regions, size);
            CHECK_NE(num_regions, size_t(0));
            /// Allocation should follow first fit principle.
            CHECK(num_free == 0 || (current_slab(out) != nullptr && current_slab(out) <= p));
            CHECK_LE(bin_num_free, bin_num_regions);
            CHECK_NE(bin_num_regions, size_t(0));
            CHECK_LE(num_free, bin_num_free);
            CHECK_LE(num_regions, bin_num_regions);
            CHECK_EQ(bin_num_regions % num_regions, size_t(0));
            CHECK_LE(bin_num_free - num_free, bin_num_regions - num_regions);
            CHECK_LE(num_regions - num_free, bin_num_regions - bin_num_free);

            /// Exact values: the slab of `p` (the extent of the pointer, its size class).
            ThreadState & thread_state = ThreadState::fetch();
            const Extent * extent = arena_extent_map_global.extentLookup(&thread_state, p);
            REQUIRE(extent != nullptr);
            CHECK_EQ(size, extent->size());
            CHECK_EQ(num_regions, size_t(bin_infos[extent->sizeClassIdx()].num_regions));
        }
        else if (size_value > SIZE_CLASS_SMALL_MAX_CLASS)
        {
            CHECK_EQ(num_free, size_t(0));
            CHECK_EQ(num_regions, size_t(1));
            CHECK(current_slab(out) == nullptr);
            CHECK_EQ(bin_num_free, size_t(0));
            CHECK_EQ(bin_num_regions, size_t(0));
        }

        je_free(out_ref);
        je_free(out);
        je_free(p);
    }
}

/// jemalloc: test/unit/inspect.c test_batch
TEST(MallctlExperimentalAPI, UtilizationBatchQuery)
{
    /// jemalloc runs `test/unit/inspect.c` with `profiling:false`: sampled small allocations are not slabs.
    if (options.profiling)
        return;
    const char * node = "experimental.utilization.batch_query";
    for (size_t size_value = 17; size_value <= TEST_MAX_SIZE && size_value <= SIZE_CLASS_LARGE_MAX_CLASS;
         size_value += (size_value <= SIZE_CLASS_SMALL_MAX_CLASS ? 1019 : 99991))
    {
        void * p = je_mallocx(size_value, 0);
        void * q = je_mallocx(size_value, 0);
        REQUIRE(p != nullptr && q != nullptr);
        void * in[] = {p, q};
        size_t in_size = sizeof(const void *) * 2;
        size_t out[] = {size_t(-1), size_t(-1), size_t(-1), size_t(-1), size_t(-1), size_t(-1)};
        size_t out_size = sizeof(size_t) * 6;
        size_t out_ref[] = {size_t(-1), size_t(-1), size_t(-1), size_t(-1), size_t(-1), size_t(-1)};
        size_t out_size_ref = out_size;

        /// Invalid argument(s) errors.
        checkCommonEINVAL(node, nullptr, &out_size, in, in_size, out_size, out_size_ref, out, out_ref);
        checkCommonEINVAL(node, out, nullptr, in, in_size, out_size, out_size_ref, out, out_ref);
        checkCommonEINVAL(node, out, &out_size, nullptr, in_size, out_size, out_size_ref, out, out_ref);
        checkCommonEINVAL(node, out, &out_size, in, 0, out_size, out_size_ref, out, out_ref);
        in_size -= 1;
        checkCommonEINVAL(node, out, &out_size, in, in_size, out_size, out_size_ref, out, out_ref);
        in_size += 1;
        out_size_ref = out_size -= 2 * sizeof(size_t);
        checkCommonEINVAL(node, out, &out_size, in, in_size, out_size, out_size_ref, out, out_ref);
        out_size_ref = out_size += 2 * sizeof(size_t);
        in_size -= sizeof(const void *);
        checkCommonEINVAL(node, out, &out_size, in, in_size, out_size, out_size_ref, out, out_ref);
        in_size += sizeof(const void *);

        auto num_free = [&](size_t i) { return out[i * 3]; };
        auto num_regions = [&](size_t i) { return out[i * 3 + 1]; };
        auto size = [&](size_t i) { return out[i * 3 + 2]; };

        /// The output of valid calls.
        out_size_ref = out_size /= 2;
        in_size /= 2;
        checkCommonValid(node, out, &out_size, in, in_size, out_size_ref, out_ref);
        CHECK_LE(size_value, size(0));
        CHECK_EQ(size(0) & (PAGE - 1), size_t(0));
        /// See the corresponding comment in `UtilizationQuery`; profiling breaks our slab count expectations.
        if (size_value <= SIZE_CLASS_SMALL_MAX_CLASS && !options.profiling)
        {
            CHECK_LE(num_free(0), num_regions(0));
            CHECK_LE(num_regions(0), size(0));
            CHECK_NE(num_regions(0), size_t(0));
        }
        else if (size_value > SIZE_CLASS_SMALL_MAX_CLASS)
        {
            CHECK_EQ(num_free(0), size_t(0));
            CHECK_EQ(num_regions(0), size_t(1));
        }
        /// Should not overwrite content beyond what's needed (jemalloc compares 3 bytes).
        CHECK_EQ(memcmp(out + 3, out_ref + 3, 3), 0);
        in_size *= 2;
        out_size_ref = out_size *= 2;

        memcpy(out_ref, out, 3 * sizeof(size_t));
        checkCommonValid(node, out, &out_size, in, in_size, out_size_ref, out_ref);
        /// Statistics should be stable across calls.
        CHECK_EQ(memcmp(out, out_ref, 3), 0);
        if (size_value <= SIZE_CLASS_SMALL_MAX_CLASS)
            CHECK_LE(num_free(1), num_regions(1));
        else
            CHECK_EQ(num_free(0), size_t(0));
        CHECK_EQ(num_regions(0), num_regions(1));
        CHECK_EQ(size(0), size(1));

        je_free(q);
        je_free(p);
    }
}

/// --- batch_alloc.c --------------------------------------------------------------------------------------------------

namespace
{

constexpr size_t BATCH_MAX = (1U << 16) + 1024;
void * global_ptrs[BATCH_MAX];

bool pageAligned(const void * ptr)
{
    return (reinterpret_cast<uintptr_t>(ptr) & PAGE_MASK) == 0;
}

void verifyBatchBasic(ThreadState & thread_state, void ** ptrs, size_t batch, size_t usable_size, bool zero)
{
    for (size_t i = 0; i < batch; ++i)
    {
        void * p = ptrs[i];
        CHECK_EQ(allocationSize(&thread_state, p), usable_size);
        if (zero)
        {
            for (size_t k = 0; k < usable_size; ++k)
                CHECK_EQ(static_cast<unsigned char *>(p)[k], 0);
        }
    }
}

void verifyBatchLocality(ThreadState & thread_state, void ** ptrs, size_t batch, size_t usable_size, Arena * arena, unsigned num_regions)
{
    /// Checking batch locality when prof is on is feasible but complicated, while checking the non-prof case
    /// suffices for unit-test purpose.
    if (config::profiling && options.profiling)
        return;
    for (size_t i = 0, j = 0; i < batch; ++i, ++j)
    {
        if (j == num_regions)
            j = 0;
        if (j == 0 && batch - i < num_regions)
            break;
        void * p = ptrs[i];
        CHECK(allocationArena(&thread_state, p) == arena);
        if (j == 0)
        {
            CHECK(pageAligned(p));
            continue;
        }
        REQUIRE(i > 0);
        void * q = ptrs[i - 1];
        CHECK(
            reinterpret_cast<uintptr_t>(p) > reinterpret_cast<uintptr_t>(q)
            && size_t(reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(q)) == usable_size);
    }
}

void releaseBatch(void ** ptrs, size_t batch, size_t size)
{
    for (size_t i = 0; i < batch; ++i)
        je_sdallocx(ptrs[i], size, 0);
}

/// jemalloc: batch_alloc_packet_t
struct BatchAllocPacket
{
    void ** ptrs;
    size_t num;
    size_t size;
    int flags;
};

size_t batchAllocWrapper(void ** ptrs, size_t num, size_t size, int flags)
{
    BatchAllocPacket packet = {ptrs, num, size, flags};
    size_t filled;
    size_t len = sizeof(size_t);
    REQUIRE(je_mallctl("experimental.batch_alloc", &filled, &len, &packet, sizeof(packet)) == 0);
    return filled;
}

void testWrapper(size_t size, size_t alignment, bool zero, unsigned arena_flag)
{
    ThreadState & thread_state = ThreadState::fetch();
    const size_t usable_size
        = alignment != 0 ? size_classes::alignedSizeToUsableSize(size, alignment) : size_classes::sizeToUsableSize(size);
    const SizeClassIdx idx = size_classes::sizeToIndex(usable_size);
    const unsigned num_regions = bin_infos[idx].num_regions;
    REQUIRE(num_regions > 0);
    Arena * arena;
    if (arena_flag != 0)
        arena = arenaGet(&thread_state, unsigned(arena_flag >> 20) - 1, false);
    else
        arena = arenaChoose(thread_state, nullptr);
    REQUIRE(arena != nullptr);
    int flags = int(arena_flag);
    if (alignment != 0)
        flags |= MALLOCX_ALIGN(alignment);
    if (zero)
        flags |= MALLOCX_ZERO;

    /// Allocate for the purpose of bootstrapping `arena_tdata`, so that the change in bin stats won't contaminate the
    /// stats to be verified below.
    void * p = je_mallocx(size, flags | MALLOCX_TCACHE_NONE);

    for (size_t i = 0; i < 4; ++i)
    {
        size_t base = 0;
        if (i == 1)
            base = num_regions;
        else if (i == 2)
            base = num_regions * 2;
        else if (i == 3)
            base = (1 << 16);
        for (int j = -1; j <= 1; ++j)
        {
            if (base == 0 && j == -1)
                continue;
            size_t batch = base + size_t(j);
            REQUIRE(batch < BATCH_MAX);
            size_t filled = batchAllocWrapper(global_ptrs, batch, size, flags);
            REQUIRE(filled == batch);
            verifyBatchBasic(thread_state, global_ptrs, batch, usable_size, zero);
            verifyBatchLocality(thread_state, global_ptrs, batch, usable_size, arena, num_regions);
            releaseBatch(global_ptrs, batch, usable_size);
        }
    }

    je_free(p);
}

}

/// jemalloc: test/unit/batch_alloc.c test_batch_alloc
TEST(MallctlExperimentalAPI, BatchAlloc)
{
    testWrapper(11, 0, false, 0);
}

/// jemalloc: test/unit/batch_alloc.c test_batch_alloc_zero
TEST(MallctlExperimentalAPI, BatchAllocZero)
{
    testWrapper(11, 0, true, 0);
}

/// jemalloc: test/unit/batch_alloc.c test_batch_alloc_aligned
TEST(MallctlExperimentalAPI, BatchAllocAligned)
{
    testWrapper(7, 16, false, 0);
}

/// jemalloc: test/unit/batch_alloc.c test_batch_alloc_manual_arena
TEST(MallctlExperimentalAPI, BatchAllocManualArena)
{
    unsigned arena_idx;
    size_t len_unsigned = sizeof(unsigned);
    REQUIRE(je_mallctl("arenas.create", &arena_idx, &len_unsigned, nullptr, 0) == 0);
    testWrapper(11, 0, false, MALLOCX_ARENA(arena_idx));
}

/// jemalloc: test/unit/batch_alloc.c test_batch_alloc_large
TEST(MallctlExperimentalAPI, BatchAllocLarge)
{
    size_t size = SIZE_CLASS_LARGE_MIN_CLASS;
    for (size_t batch = 0; batch < 4; ++batch)
    {
        size_t filled = batchAllocWrapper(global_ptrs, batch, size, 0);
        CHECK_EQ(filled, batch);
        releaseBatch(global_ptrs, batch, size);
    }
    size = global_do_not_change_thread_cache_max_class + 1;
    for (size_t batch = 0; batch < 4; ++batch)
    {
        size_t filled = batchAllocWrapper(global_ptrs, batch, size, 0);
        CHECK_EQ(filled, batch);
        releaseBatch(global_ptrs, batch, size);
    }
}

/// The access checks of `experimental.batch_alloc` (jemalloc: `VERIFY_READ(size_t)`, `ASSURED_WRITE`) and the
/// failure of an invalid size.
TEST(MallctlExperimentalAPI, BatchAllocErrors)
{
    BatchAllocPacket packet = {global_ptrs, 1, 8, 0};
    size_t filled = 12345;
    size_t len = sizeof(unsigned);
    CHECK_EQ(je_mallctl("experimental.batch_alloc", &filled, &len, &packet, sizeof(packet)), EINVAL);
    CHECK_EQ(len, size_t(0));
    len = sizeof(size_t);
    CHECK_EQ(je_mallctl("experimental.batch_alloc", &filled, &len, nullptr, 0), EINVAL);
    CHECK_EQ(je_mallctl("experimental.batch_alloc", &filled, &len, &packet, sizeof(packet) - 1), EINVAL);
    CHECK_EQ(filled, size_t(12345));
    /// A size beyond the largest size class: nothing is allocated.
    packet.size = SIZE_CLASS_LARGE_MAX_CLASS + 1;
    REQUIRE(je_mallctl("experimental.batch_alloc", &filled, &len, &packet, sizeof(packet)) == 0);
    CHECK_EQ(filled, size_t(0));
}

/// --- safety_check.c (public API) ------------------------------------------------------------------------------------

namespace
{

bool fake_abort_called = false;
char fake_abort_message[256];

void fakeAbort(const char * message)
{
    fake_abort_called = true;
    snprintf(fake_abort_message, sizeof(fake_abort_message), "%s", message);
}

}

/// jemalloc: experimental_hooks_safety_check_abort_ctl. The redzone tests of `test/unit/safety_check.c` are skipped
/// in jemalloc without `config_opt_safety_checks`; here the hook is installed through `mallctl` and the failure path
/// (`safety_check_fail`) is called directly.
TEST(MallctlExperimentalAPI, SafetyCheckAbortHook)
{
    const char * node = "experimental.hooks.safety_check_abort";
    SafetyCheckAbortHook hook = fakeAbort;
    SafetyCheckAbortHook old_hook = nullptr;
    size_t old_len = sizeof(old_hook);

    /// Write-only.
    CHECK_EQ(je_mallctl(node, &old_hook, &old_len, nullptr, 0), EPERM);
    CHECK_EQ(je_mallctl(node, &old_hook, &old_len, &hook, sizeof(hook)), EPERM);
    CHECK_EQ(je_mallctl(node, nullptr, nullptr, &hook, sizeof(hook) - 1), EINVAL);
    /// Neither read nor write: no-op.
    CHECK_EQ(je_mallctl(node, nullptr, nullptr, nullptr, 0), 0);

    REQUIRE(je_mallctl(node, nullptr, nullptr, &hook, sizeof(hook)) == 0);
    safetyCheckFail("<jemalloc>: test failure %d\n", 42);
    CHECK(fake_abort_called);
    CHECK_STREQ(static_cast<const char *>(fake_abort_message), "<jemalloc>: test failure 42\n");
    fake_abort_called = false;

    SafetyCheckAbortHook null_hook = nullptr;
    REQUIRE(je_mallctl(node, nullptr, nullptr, &null_hook, sizeof(null_hook)) == 0);
}

/// --- experimental.arenas.<i>.pactivep, experimental.arenas_create_ext -----------------------------------------------

TEST(MallctlExperimentalAPI, ArenasActivePagesPtr)
{
    unsigned arena_idx;
    size_t len = sizeof(arena_idx);
    REQUIRE(je_mallctl("arenas.create", &arena_idx, &len, nullptr, 0) == 0);
    /// `experimental.arenas.<i>` exists once the ctl snapshot knows the arena (`ctl_arenas_i_verify`).
    uint64_t epoch = 1;
    REQUIRE(je_mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch)) == 0);

    size_t numeric_path[4];
    size_t numeric_path_length = 4;
    REQUIRE(je_mallctl_name_to_numeric_path("experimental.arenas.0.pactivep", numeric_path, &numeric_path_length) == 0);
    numeric_path[2] = arena_idx;
    size_t * active_pages_ptr = nullptr;
    size_t size_value = sizeof(active_pages_ptr);
    REQUIRE(je_mallctl_by_numeric_path(numeric_path, numeric_path_length, &active_pages_ptr, &size_value, nullptr, 0) == 0);
    REQUIRE(active_pages_ptr != nullptr);

    ThreadState & thread_state = ThreadState::fetch();
    Arena * arena = arenaGet(&thread_state, arena_idx, false);
    REQUIRE(arena != nullptr);
    CHECK_EQ(*active_pages_ptr, arena->page_allocator_shard.numActiveGet());

    /// A large allocation in the arena is visible through the pointer immediately.
    size_t before = *active_pages_ptr;
    void * p = je_mallocx(1 << 20, MALLOCX_ARENA(arena_idx) | MALLOCX_TCACHE_NONE);
    REQUIRE(p != nullptr);
    CHECK_EQ(*active_pages_ptr - before, ((size_t(1) << 20) + large_pad) >> LOG2_PAGE);
    je_dallocx(p, MALLOCX_TCACHE_NONE);
    CHECK_EQ(*active_pages_ptr, before);

    /// Errors: wrong sizes (checked before anything else), writes, nonexistent arenas.
    size_t bad_size = sizeof(unsigned);
    CHECK_EQ(je_mallctl_by_numeric_path(numeric_path, numeric_path_length, &active_pages_ptr, &bad_size, nullptr, 0), EINVAL);
    CHECK_EQ(je_mallctl_by_numeric_path(numeric_path, numeric_path_length, nullptr, &size_value, nullptr, 0), EINVAL);
    CHECK_EQ(je_mallctl_by_numeric_path(numeric_path, numeric_path_length, &active_pages_ptr, &size_value, &active_pages_ptr, sizeof(active_pages_ptr)), EPERM);
    /// The index function rejects arenas that do not exist (`ctl_arenas_i_verify`).
    numeric_path[2] = MALLOCX_ARENA_LIMIT - 1;
    CHECK_EQ(je_mallctl_by_numeric_path(numeric_path, numeric_path_length, &active_pages_ptr, &size_value, nullptr, 0), ENOENT);
    /// `MALLCTL_ARENAS_ALL` passes the index function but is not an arena.
    numeric_path[2] = MALLCTL_ARENAS_ALL;
    CHECK_EQ(je_mallctl_by_numeric_path(numeric_path, numeric_path_length, &active_pages_ptr, &size_value, nullptr, 0), EFAULT);
}

namespace
{

/// jemalloc: arena_config_t
struct TestArenaConfig
{
    extent_hooks_t * extent_hooks_ptr;
    bool metadata_use_hooks;
};

static_assert(sizeof(TestArenaConfig) == sizeof(ArenaConfig));

}

TEST(MallctlExperimentalAPI, ArenasCreateExtended)
{
    const char * node = "experimental.arenas_create_ext";
    unsigned num_arenas_before;
    size_t len = sizeof(unsigned);
    REQUIRE(je_mallctl("arenas.narenas", &num_arenas_before, &len, nullptr, 0) == 0);

    /// Without a config: the default one.
    unsigned arena_idx = 0;
    len = sizeof(arena_idx);
    REQUIRE(je_mallctl(node, &arena_idx, &len, nullptr, 0) == 0);
    CHECK_EQ(arena_idx, num_arenas_before);

    /// The default hooks without metadata hooks.
    TestArenaConfig config = {const_cast<extent_hooks_t *>(&extent_hooks_default_extent_hooks), false};
    unsigned arena_idx2 = 0;
    REQUIRE(je_mallctl(node, &arena_idx2, &len, &config, sizeof(config)) == 0);
    CHECK_EQ(arena_idx2, arena_idx + 1);

    /// The arena works and uses the default hooks.
    extent_hooks_t * hooks = nullptr;
    size_t hooks_len = sizeof(hooks);
    char name[64];
    snprintf(name, sizeof(name), "arena.%u.extent_hooks", arena_idx2);
    REQUIRE(je_mallctl(name, &hooks, &hooks_len, nullptr, 0) == 0);
    CHECK(hooks == &extent_hooks_default_extent_hooks);
    void * p = je_mallocx(100, MALLOCX_ARENA(arena_idx2) | MALLOCX_TCACHE_NONE);
    REQUIRE(p != nullptr);
    unsigned lookup_idx = 0;
    len = sizeof(lookup_idx);
    REQUIRE(je_mallctl("arenas.lookup", &lookup_idx, &len, &p, sizeof(p)) == 0);
    CHECK_EQ(lookup_idx, arena_idx2);
    je_dallocx(p, MALLOCX_TCACHE_NONE);

    /// Errors.
    len = sizeof(size_t);
    CHECK_EQ(je_mallctl(node, &arena_idx, &len, nullptr, 0), EINVAL);
    CHECK_EQ(len, size_t(0));
    len = sizeof(unsigned);
    CHECK_EQ(je_mallctl(node, &arena_idx, &len, &config, sizeof(config) - 1), EINVAL);
    /// Custom extent hooks are not supported.
    extent_hooks_t custom_hooks = extent_hooks_default_extent_hooks;
    config.extent_hooks_ptr = &custom_hooks;
    CHECK_EQ(je_mallctl(node, &arena_idx, &len, &config, sizeof(config)), EINVAL);

    unsigned num_arenas_after;
    len = sizeof(unsigned);
    REQUIRE(je_mallctl("arenas.narenas", &num_arenas_after, &len, nullptr, 0) == 0);
    CHECK_EQ(num_arenas_after, num_arenas_before + 2);
}

/// --- experimental.thread.activity_callback ---------------------------------------------------------------------------

namespace
{

struct ActivityRecord
{
    unsigned calls = 0;
    uint64_t allocated = 0;
    uint64_t deallocated = 0;
};

void activityCallback(void * user_context, uint64_t allocated, uint64_t deallocated)
{
    auto * record = static_cast<ActivityRecord *>(user_context);
    ++record->calls;
    record->allocated = allocated;
    record->deallocated = deallocated;
}

/// jemalloc: activity_callback_thunk_t
struct TestThunk
{
    void (*callback)(void *, uint64_t, uint64_t);
    void * user_context;
};

}

TEST(MallctlExperimentalAPI, ThreadActivityCallback)
{
    const char * node = "experimental.thread.activity_callback";
    ActivityRecord record;
    TestThunk old_thunk = {activityCallback, &record};
    size_t len = sizeof(old_thunk);
    REQUIRE(je_mallctl(node, &old_thunk, &len, nullptr, 0) == 0);
    CHECK(old_thunk.callback == nullptr);
    CHECK(old_thunk.user_context == nullptr);

    TestThunk thunk = {activityCallback, &record};
    REQUIRE(je_mallctl(node, nullptr, nullptr, &thunk, sizeof(thunk)) == 0);
    CHECK_EQ(je_mallctl(node, nullptr, nullptr, &thunk, sizeof(thunk) - 1), EINVAL);

    /// The callback is called by the peak event (every `PEAK_EVENT_WAIT` = 64 KiB of allocated + deallocated
    /// bytes) with the thread's counters at that moment.
    for (int i = 0; i < 64; ++i)
        je_free(je_mallocx(4096, 0));
    CHECK_GT(record.calls, 0u);
    uint64_t allocated;
    uint64_t deallocated;
    size_t u64_len = sizeof(uint64_t);
    REQUIRE(je_mallctl("thread.allocated", &allocated, &u64_len, nullptr, 0) == 0);
    REQUIRE(je_mallctl("thread.deallocated", &deallocated, &u64_len, nullptr, 0) == 0);
    CHECK_GT(record.allocated, uint64_t(0));
    CHECK_LE(record.allocated, allocated);
    CHECK_LE(record.deallocated, deallocated);

    /// Reading returns the installed thunk; writing a null callback turns it off.
    TestThunk read_thunk = {nullptr, nullptr};
    TestThunk null_thunk = {nullptr, nullptr};
    len = sizeof(read_thunk);
    REQUIRE(je_mallctl(node, &read_thunk, &len, &null_thunk, sizeof(null_thunk)) == 0);
    CHECK(read_thunk.callback == activityCallback);
    CHECK(read_thunk.user_context == &record);
    unsigned calls = record.calls;
    for (int i = 0; i < 64; ++i)
        je_free(je_mallocx(4096, 0));
    CHECK_EQ(record.calls, calls);
}
