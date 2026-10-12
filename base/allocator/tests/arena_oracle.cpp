/// Compares the arena (`Arena.cpp`, `ArenaLarge.cpp`, `Bin.cpp` and everything below them) with jemalloc's `arena.c`,
/// `large.c`, `bin.c` linked from the reference `lib_jemalloc.a`. Two manual arenas are created on each side (C:
/// `arena_new` with the reference library's own tsd; C++: `arenaNew` with a `ThreadState` object), at the same
/// indices, and receive identical randomized sequences of operations: tcache-bypass small and large allocation
/// (`arena_malloc_hard`, `arena_palloc` with alignments), deallocation (`arena_dalloc_no_tcache`,
/// `arena_sdalloc_no_tcache`), tcache fill (`arena_ptr_array_fill_small`), flush of mixed-arena batches
/// (`arena_ptr_array_flush`, small and large), `arena_fill_small_fresh`, in-place and moving reallocation, decay,
/// sampled-allocation promotion/demotion, and finally reset, purge and destroy.
///
/// Addresses come from mmap, so they are normalized to (region, offset) by interposing `mmap`/`munmap` (see the
/// reference file). The randomized decisions of the arena (cache-oblivious offsets and the decay ticker) come from
/// the thread's PRNG state, which is synchronized once at the start and compared after every step. The clock is a fake
/// one that never advances (the decay epoch never advances, so purging happens only through `decay(all)`).
///
/// After every step: every returned pointer, the usable sizes, the PRNG/ticker state, the slabcur / nonfull / full
/// state of the touched bins, the large lists, and all values of `arena_stats_merge` (except the uptime) - including
/// the per-bin, per-large-class and per-page-size extent stats and the lock counters of all arena mutexes - must be
/// identical.
///
/// Limits: the reference library is initialized with ClickHouse's configuration by its constructor, so global state
/// (`num_arenas_auto`, `manual_arena_base`, `oversize_threshold`, the decay defaults, `opt_prof`) is copied from it to the
/// C++ side; the reference's background threads are disabled through their state flag. Our side does not create arena
/// 0 / the auto arenas (only the arena table entries of the test arenas exist). The extent map is global on both
/// sides, so rtree metadata is not compared (`metadata_radix_tree` is 0 for a non-zero arena's base on both sides).

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/Base.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include "Test.h"
#include "arena_oracle_ref.h"

#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>

extern "C" {
/// The reference pulls in the libunwind-based profiler backtrace, which is never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

using namespace jemalloc;

namespace
{

constexpr int REF_SIDE = 0;
constexpr int OUR_SIDE = 1;

constinit ThreadState our_thread_state;

RefGlobals ref_globals;

struct SideScope
{
    explicit SideScope(int side) { ref_trace_set_side(side); }
    ~SideScope() { ref_trace_set_side(-1); }
};

void bootOnce()
{
    static bool booted = false;
    if (booted)
        return;
    booted = true;

    ref_boot(&ref_globals);

    REQUIRE(ref_globals.sizeof_arena == sizeof(Arena));
    REQUIRE(ref_globals.sizeof_bin == sizeof(Bin));

    /// Copy the reference's configuration.
    options.profiling = ref_globals.option_profiling;
    options.retain = ref_globals.option_retain;
    options.cache_oblivious = ref_globals.option_cache_oblivious;
    options.calloc_madvise_threshold = ref_globals.calloc_madvise_threshold;
    options.log2_extent_max_active_fit = ref_globals.log2_extent_max_active_fit;
    options.dirty_decay_ms = ref_globals.dirty_decay_ms_default;
    options.muzzy_decay_ms = ref_globals.muzzy_decay_ms_default;

    REQUIRE(!pages::boot());
    sizeBoot(default_size_class_data, options.cache_oblivious);
    REQUIRE(large_pad == ref_globals.large_pad);
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_extent_map_global.init(base0Get(), /* zeroed */ true));
    REQUIRE(!arenaBoot(&default_size_class_data, base0Get(), false));
    REQUIRE(arena_num_bins_total == ref_globals.num_bins_total);
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));

    oversize_threshold = ref_globals.oversize_threshold;
    num_arenas_auto = ref_globals.num_arenas_auto;
    manual_arena_base = ref_globals.manual_arena_base;
    numArenasTotalSet(ref_globals.num_arenas_total);

    /// A nominal (slow) state, as for a thread that has initialized its tsd (`arenaNew` enters and leaves reentrancy,
    /// which recomputes the state). The object is not in the nominal list.
    our_thread_state.state.store(thread_state_nominal_slow, std::memory_order_relaxed);

    /// The same PRNG / ticker state on both sides.
    uint64_t prng;
    int32_t tick;
    int32_t num_ticks;
    ref_thread_state_rng_get(&prng, &tick, &num_ticks);
    our_thread_state.prngState() = prng;
    our_thread_state.arena_decay_ticker.tick = tick;
    our_thread_state.arena_decay_ticker.num_ticks = num_ticks;
}

struct NormalizedAddress
{
    bool ok = false;
    size_t rank = 0;
    size_t offset = 0;

    bool operator==(const NormalizedAddress & other) const = default;
};

NormalizedAddress normalize(int side, const void * p)
{
    NormalizedAddress n;
    n.ok = ref_normalize(side, reinterpret_cast<uintptr_t>(p), &n.rank, &n.offset);
    return n;
}

bool samePtr(const void * ref, const void * our, int step, const char * what)
{
    if (ref == nullptr || our == nullptr)
    {
        if (ref != our)
            std::fprintf(stderr, "step %d: %s: ref %p our %p\n", step, what, ref, our);
        return ref == our;
    }
    NormalizedAddress r = normalize(REF_SIDE, ref);
    NormalizedAddress o = normalize(OUR_SIDE, our);
    bool ok = r.ok && o.ok && r == o;
    if (!ok)
        std::fprintf(
            stderr,
            "step %d: %s: ref (region %zu + %zu ok %d) our (region %zu + %zu ok %d)\n",
            step,
            what,
            r.rank,
            r.offset,
            int(r.ok),
            o.rank,
            o.offset,
            int(o.ok));
    return ok;
}

void put(std::vector<uint64_t> & out, uint64_t v)
{
    out.push_back(v);
}

void putMutex(std::vector<uint64_t> & out, const MutexProfilingData & d)
{
    put(out, d.num_lock_ops);
    put(out, d.num_owner_switches);
    put(out, d.num_wait_times);
    put(out, d.num_spin_acquired);
    put(out, d.max_num_threads);
}

/// The same order as `ref_stats`.
std::vector<uint64_t> ourStats(Arena * arena)
{
    static ArenaStats arena_stats;
    static BinStatsData bin_stats[SIZE_CLASS_NUM_BINS];
    static ArenaStatsLarge large_stats[SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS];
    static PageAllocatorExtentStats extent_stats[SIZE_CLASS_NUM_PAGE_SIZES];
    std::memset(static_cast<void *>(&arena_stats), 0, sizeof(arena_stats));
    std::memset(static_cast<void *>(bin_stats), 0, sizeof(bin_stats));
    std::memset(static_cast<void *>(large_stats), 0, sizeof(large_stats));
    std::memset(static_cast<void *>(extent_stats), 0, sizeof(extent_stats));

    unsigned num_threads = 0;
    const char * sbrk = nullptr;
    ssize_t dirty_decay_ms = 0;
    ssize_t muzzy_decay_ms = 0;
    size_t num_active = 0;
    size_t num_dirty = 0;
    size_t num_muzzy = 0;
    arenaStatsMerge(
        &our_thread_state,
        arena,
        &num_threads,
        &sbrk,
        &dirty_decay_ms,
        &muzzy_decay_ms,
        &num_active,
        &num_dirty,
        &num_muzzy,
        &arena_stats,
        bin_stats,
        large_stats,
        extent_stats);

    std::vector<uint64_t> out;
    put(out, num_threads);
    uint64_t sbrk_idx = 99;
    for (unsigned i = 0; i < unsigned(SbrkPrecedence::Limit); ++i)
        if (std::strcmp(sbrk, sbrk_precedence_names[i]) == 0)
            sbrk_idx = i;
    put(out, sbrk_idx);
    put(out, uint64_t(dirty_decay_ms));
    put(out, uint64_t(muzzy_decay_ms));
    put(out, num_active);
    put(out, num_dirty);
    put(out, num_muzzy);

    put(out, arena_stats.base);
    put(out, arena_stats.metadata_extent);
    put(out, arena_stats.metadata_radix_tree);
    put(out, arena_stats.resident);
    put(out, arena_stats.metadata_transparent_huge_pages);
    put(out, arena_stats.mapped);
    put(out, arena_stats.internal.load());
    put(out, arena_stats.allocated_large);
    put(out, arena_stats.num_allocations_large);
    put(out, arena_stats.num_deallocations_large);
    put(out, arena_stats.num_fills_large);
    put(out, arena_stats.num_flushes_large);
    put(out, arena_stats.num_requests_large);
    put(out, arena_stats.page_allocator_shard_stats.extent_available);
    const PageAllocatorStats & page_allocator = arena_stats.page_allocator_shard_stats.page_allocator_stats;
    put(out, page_allocator.decay_dirty.num_purge.readUnsynchronized());
    put(out, page_allocator.decay_dirty.num_madvises.readUnsynchronized());
    put(out, page_allocator.decay_dirty.purged.readUnsynchronized());
    put(out, page_allocator.decay_muzzy.num_purge.readUnsynchronized());
    put(out, page_allocator.decay_muzzy.num_madvises.readUnsynchronized());
    put(out, page_allocator.decay_muzzy.purged.readUnsynchronized());
    put(out, page_allocator.retained);
    put(out, page_allocator.page_allocator_mapped.load());
    put(out, page_allocator.abandoned_vm.load());
    put(out, arena_stats.thread_cache_bytes);
    put(out, arena_stats.thread_cache_stashed_bytes);
    for (unsigned i = 0; i < mutex_profiling_num_arena_mutexes; ++i)
        putMutex(out, arena_stats.mutex_profiling_data[i]);

    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        const BinStats & b = bin_stats[i].stats_data;
        put(out, b.num_allocations);
        put(out, b.num_deallocations);
        put(out, b.num_requests);
        put(out, b.current_regions);
        put(out, b.num_fills);
        put(out, b.num_flushes);
        put(out, b.num_slabs);
        put(out, b.slab_changes);
        put(out, b.current_slabs);
        put(out, b.non_full_slabs);
        putMutex(out, bin_stats[i].mutex_data);
    }
    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS; ++i)
    {
        const ArenaStatsLarge & l = large_stats[i];
        put(out, l.num_allocations.readUnsynchronized());
        put(out, l.num_deallocations.readUnsynchronized());
        put(out, l.active_bytes.readUnsynchronized());
        put(out, l.num_requests.readUnsynchronized());
        put(out, l.num_fills.readUnsynchronized());
        put(out, l.num_flushes.readUnsynchronized());
        put(out, l.current_large_extents);
    }
    for (unsigned i = 0; i < SIZE_CLASS_NUM_PAGE_SIZES; ++i)
    {
        const PageAllocatorExtentStats & e = extent_stats[i];
        put(out, e.num_dirty);
        put(out, e.dirty_bytes);
        put(out, e.num_muzzy);
        put(out, e.muzzy_bytes);
        put(out, e.num_retained);
        put(out, e.retained_bytes);
    }
    return out;
}

constexpr size_t STATS_MAX = 16384;

bool sameStats(void * ref_arena, Arena * our_arena, int step)
{
    static uint64_t ref_out[STATS_MAX];
    size_t n;
    {
        SideScope scope(REF_SIDE);
        n = ref_stats(ref_arena, ref_out, STATS_MAX);
    }
    std::vector<uint64_t> our;
    {
        SideScope scope(OUR_SIDE);
        our = ourStats(our_arena);
    }
    REQUIRE(n <= STATS_MAX);
    if (n != our.size())
    {
        std::fprintf(stderr, "step %d: stats count %zu vs %zu\n", step, n, our.size());
        return false;
    }
    bool ok = true;
    int reported = 0;
    for (size_t i = 0; i < n; ++i)
    {
        if (ref_out[i] != our[i])
        {
            ok = false;
            if (reported++ < 10)
                std::fprintf(
                    stderr,
                    "step %d: stat #%zu: ref %llu our %llu\n",
                    step,
                    i,
                    static_cast<unsigned long long>(ref_out[i]),
                    static_cast<unsigned long long>(our[i]));
        }
    }
    return ok;
}

bool sameRng(int step)
{
    uint64_t prng;
    int32_t tick;
    int32_t num_ticks;
    ref_thread_state_rng_get(&prng, &tick, &num_ticks);
    bool ok = prng == our_thread_state.prngState() && tick == our_thread_state.arena_decay_ticker.tick
        && num_ticks == our_thread_state.arena_decay_ticker.num_ticks;
    if (!ok)
        std::fprintf(
            stderr,
            "step %d: rng ref (%llx, %d, %d) our (%llx, %d, %d)\n",
            step,
            static_cast<unsigned long long>(prng),
            tick,
            num_ticks,
            static_cast<unsigned long long>(our_thread_state.prngState()),
            our_thread_state.arena_decay_ticker.tick,
            our_thread_state.arena_decay_ticker.num_ticks);
    return ok;
}

bool sameBin(void * ref_arena, Arena * our_arena, unsigned bin_idx, int step)
{
    uintptr_t ref_current_slab;
    unsigned ref_num_free;
    uintptr_t ref_first;
    size_t ref_num_full;
    ref_bin_state(ref_arena, bin_idx, &ref_current_slab, &ref_num_free, &ref_first, &ref_num_full);

    Bin * bin = arenaGetBin(our_arena, bin_idx, 0);
    void * our_current_slab = bin->current_slab ? bin->current_slab->addr() : nullptr;
    unsigned our_num_free = bin->current_slab ? bin->current_slab->numFree() : 0;
    Extent * first = bin->slabs_non_full.first();
    void * our_first = first ? first->addr() : nullptr;
    size_t our_num_full = 0;
    for (Extent * e = bin->slabs_full.first(); e != nullptr; e = bin->slabs_full.next(e))
        ++our_num_full;

    bool ok = samePtr(reinterpret_cast<void *>(ref_current_slab), our_current_slab, step, "slabcur") && ref_num_free == our_num_free
        && samePtr(reinterpret_cast<void *>(ref_first), our_first, step, "nonfull first") && ref_num_full == our_num_full;
    if (!ok)
        std::fprintf(
            stderr, "step %d: bin %u: nfree %u/%u nfull %zu/%zu\n", step, bin_idx, ref_num_free, our_num_free, ref_num_full, our_num_full);
    return ok;
}

bool sameLargeList(void * ref_arena, Arena * our_arena, int step)
{
    static uintptr_t ref_list[65536];
    size_t n = ref_large_list(ref_arena, ref_list, 65536);
    size_t i = 0;
    bool ok = true;
    for (Extent * e = our_arena->large.first(); e != nullptr; e = our_arena->large.next(e), ++i)
    {
        if (i >= n || !samePtr(reinterpret_cast<void *>(ref_list[i]), e->addr(), step, "large list"))
        {
            ok = false;
            break;
        }
    }
    if (ok && i != n)
    {
        std::fprintf(stderr, "step %d: large list length %zu vs %zu\n", step, n, i);
        ok = false;
    }
    return ok;
}

struct Live
{
    void * ref;
    void * our;
    size_t usable_size;
    SizeClassIdx size_class_idx;
    bool small;
    unsigned arena;
};

struct Oracle
{
    void * ref_arenas[2] = {};
    Arena * our_arenas[2] = {};
    std::vector<Live> live;
    std::mt19937_64 rng;
    int step = 0;

    explicit Oracle(uint64_t seed)
        : rng(seed)
    {
        bootOnce();
        for (unsigned a = 0; a < 2; ++a)
        {
            unsigned idx = numArenasTotalGet();
            {
                SideScope scope(REF_SIDE);
                ref_arenas[a] = ref_arena_new(idx);
            }
            {
                SideScope scope(OUR_SIDE);
                our_arenas[a] = arenaNew(&our_thread_state, idx, &arena_config_default);
            }
            REQUIRE(ref_arenas[a] != nullptr);
            REQUIRE(our_arenas[a] != nullptr);
            REQUIRE(ref_arena_idx(ref_arenas[a]) == idx);
            REQUIRE(arenaIdxGet(our_arenas[a]) == idx);
            /// The reference does not count arenas created by `arena_new` directly; advance our counter in the same way
            /// as `arena_init_locked` would for the next index, and keep the reference's view consistent: both sides
            /// use `idx + 1` for the second arena.
            numArenasTotalSet(idx + 1);
            char ref_name[ARENA_NAME_LEN];
            char our_name[ARENA_NAME_LEN];
            ref_arena_name(ref_arenas[a], ref_name);
            arenaNameGet(our_arenas[a], our_name);
            CHECK_EQ(std::strcmp(ref_name, our_name), 0);
        }
    }

    size_t random(size_t n) { return size_t(rng() % n); }

    void checkAll(std::initializer_list<unsigned> bins = {})
    {
        ++checks;
        for (unsigned a = 0; a < 2; ++a)
        {
            CHECK(sameStats(ref_arenas[a], our_arenas[a], step));
            CHECK(sameLargeList(ref_arenas[a], our_arenas[a], step));
            for (unsigned b : bins)
                CHECK(sameBin(ref_arenas[a], our_arenas[a], b, step));
        }
        CHECK(sameRng(step));
    }

    void addLive(void * ref, void * our, unsigned a, const char * what)
    {
        CHECK(samePtr(ref, our, step, what));
        if (ref == nullptr || our == nullptr)
            return;
        size_t ref_usable_size = ref_allocation_size(ref);
        size_t our_usable_size = arenaAllocationSize(&our_thread_state, our);
        CHECK_EQ(ref_usable_size, our_usable_size);
        SizeClassIdx idx = size_classes::sizeToIndex(our_usable_size);
        live.push_back({ref, our, our_usable_size, idx, idx < SIZE_CLASS_NUM_BINS, a});
        /// `prof_malloc` of an allocation that is not sampled (`opt.prof` is on): the extent's tctx of a large
        /// allocation is reset (otherwise it may hold garbage from a previous slab use of the extent).
        if (options.profiling)
        {
            ref_profiling_thread_context_reset(ref);
            arenaProfilingThreadContextReset(our_thread_state, our, nullptr);
        }
        /// Touch the memory (the same pattern on both sides) so that later zeroing decisions matter.
        std::memset(ref, 0x5a, minOf<size_t>(our_usable_size, 64));
        std::memset(our, 0x5a, minOf<size_t>(our_usable_size, 64));
    }

    size_t randomSmallSize()
    {
        SizeClassIdx idx = SizeClassIdx(random(4) == 0 ? random(SIZE_CLASS_NUM_BINS) : random(12));
        size_t usable_size = size_classes::indexToSize(idx);
        size_t low = idx == 0 ? 1 : size_classes::indexToSize(idx - 1) + 1;
        return low + random(usable_size - low + 1);
    }

    size_t randomLargeSize()
    {
        size_t log2_size = SIZE_CLASS_LOG2_LARGE_MIN_CLASS + random(6);
        return (size_t(1) << log2_size) + random(size_t(1) << log2_size);
    }

    void opMallocSmall()
    {
        unsigned a = unsigned(random(2));
        size_t size = randomSmallSize();
        SizeClassIdx idx = size_classes::sizeToIndex(size);
        bool zero = random(4) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_malloc_hard(ref_arenas[a], size, idx, zero, true);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaMallocHard(&our_thread_state, our_arenas[a], size, idx, zero, true);
        }
        if (zero && our != nullptr)
            CHECK_EQ(static_cast<unsigned char *>(our)[0], 0);
        addLive(ref, our, a, "malloc small");
        checkAll({idx});
    }

    void opMallocLarge()
    {
        unsigned a = unsigned(random(2));
        size_t size = randomLargeSize();
        SizeClassIdx idx = size_classes::sizeToIndex(size);
        bool zero = random(3) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_malloc_hard(ref_arenas[a], size, idx, zero, false);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaMallocHard(&our_thread_state, our_arenas[a], size, idx, zero, false);
        }
        if (zero && our != nullptr)
            CHECK_EQ(static_cast<unsigned char *>(our)[size_classes::sizeToUsableSize(size) - 1], 0);
        addLive(ref, our, a, "malloc large");
        checkAll();
    }

    void opAllocateAligned()
    {
        unsigned a = unsigned(random(2));
        size_t alignment = size_t(1) << (4 + random(LOG2_PAGE + 2 - 4));
        size_t size = random(2) ? randomSmallSize() : randomLargeSize();
        size_t usable_size = size_classes::alignedSizeToUsableSize(size, alignment);
        if (usable_size == 0 || usable_size > SIZE_CLASS_LARGE_MAX_CLASS)
            return;
        bool slab = size_classes::canUseSlab(usable_size) && alignment <= PAGE;
        bool zero = random(4) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_allocate_aligned(ref_arenas[a], usable_size, alignment, zero, slab);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaAllocateAligned(&our_thread_state, our_arenas[a], usable_size, alignment, zero, slab, nullptr);
        }
        if (our != nullptr)
            CHECK_EQ(reinterpret_cast<uintptr_t>(our) % alignment, 0u);
        addLive(ref, our, a, "palloc");
        checkAll({slab ? size_classes::sizeToIndex(usable_size) : 0u});
    }

    void opDeallocate()
    {
        if (live.empty())
            return;
        size_t i = random(live.size());
        Live l = live[i];
        live[i] = live.back();
        live.pop_back();
        bool sized = random(2) == 0;
        {
            SideScope scope(REF_SIDE);
            if (sized)
                ref_sized_deallocate_no_thread_cache(l.ref, l.usable_size);
            else
                ref_deallocate_no_thread_cache(l.ref);
        }
        {
            SideScope scope(OUR_SIDE);
            if (sized)
                arenaSizedDeallocateNoThreadCache(&our_thread_state, l.our, l.usable_size);
            else
                arenaDeallocateNoThreadCache(&our_thread_state, l.our);
        }
        if (l.small)
            checkAll({l.size_class_idx});
        else
            checkAll();
    }

    void opFill()
    {
        unsigned a = unsigned(random(2));
        unsigned bin_idx = unsigned(random(4) == 0 ? random(SIZE_CLASS_NUM_BINS) : random(12));
        unsigned num_fill_min = 1 + unsigned(random(64));
        unsigned num_fill_max = num_fill_min + unsigned(random(200));
        uint64_t num_requests = random(1000);
        std::vector<void *> ref_ptrs(num_fill_max);
        std::vector<void *> our_ptrs(num_fill_max);
        unsigned ref_n;
        unsigned our_n;
        {
            SideScope scope(REF_SIDE);
            ref_n = ref_fill_small(ref_arenas[a], bin_idx, ref_ptrs.data(), num_fill_min, num_fill_max, num_requests);
        }
        {
            SideScope scope(OUR_SIDE);
            CacheBinPtrArray array{CacheBinSize(num_fill_max)};
            array.ptr = our_ptrs.data();
            CacheBinStats stats;
            stats.num_requests = num_requests;
            our_n = arenaPtrArrayFillSmall(
                &our_thread_state, our_arenas[a], bin_idx, &array, CacheBinSize(num_fill_min), CacheBinSize(num_fill_max), stats);
        }
        CHECK_EQ(ref_n, our_n);
        for (unsigned i = 0; i < minOf(ref_n, our_n); ++i)
            addLive(ref_ptrs[i], our_ptrs[i], a, "fill");
        checkAll({bin_idx});
    }

    void opFillFresh()
    {
        unsigned a = unsigned(random(2));
        unsigned bin_idx = unsigned(random(12));
        size_t num_fill = 1 + random(300);
        bool zero = random(2) == 0;
        std::vector<void *> ref_ptrs(num_fill);
        std::vector<void *> our_ptrs(num_fill);
        size_t ref_n;
        size_t our_n;
        {
            SideScope scope(REF_SIDE);
            ref_n = ref_fill_small_fresh(ref_arenas[a], bin_idx, ref_ptrs.data(), num_fill, zero);
        }
        {
            SideScope scope(OUR_SIDE);
            our_n = arenaFillSmallFresh(&our_thread_state, our_arenas[a], bin_idx, our_ptrs.data(), num_fill, zero);
        }
        CHECK_EQ(ref_n, our_n);
        for (size_t i = 0; i < minOf(ref_n, our_n); ++i)
            addLive(ref_ptrs[i], our_ptrs[i], a, "fill fresh");
        checkAll({bin_idx});
    }

    void opFlush()
    {
        if (live.empty())
            return;
        /// Pick a size class of a random live object and flush a random subset of the objects of that class (from both
        /// arenas: the flush partitions by arena).
        const Live & sample = live[random(live.size())];
        SizeClassIdx size_class_idx = sample.size_class_idx;
        bool small = size_class_idx < SIZE_CLASS_NUM_BINS;
        /// The tcache only caches (and so only flushes) large size classes up to `THREAD_CACHE_MAX_CLASS_LIMIT`.
        if (!small && sample.usable_size > THREAD_CACHE_MAX_CLASS_LIMIT)
            return;
        std::vector<size_t> picked;
        for (size_t i = 0; i < live.size(); ++i)
            if (live[i].size_class_idx == size_class_idx && random(3) != 0)
                picked.push_back(i);
        if (picked.empty())
            return;
        std::vector<void *> ref_ptrs;
        std::vector<void *> our_ptrs;
        for (size_t i : picked)
        {
            ref_ptrs.push_back(live[i].ref);
            our_ptrs.push_back(live[i].our);
        }
        /// Remove the picked objects (from the back, to keep the indices valid).
        for (size_t k = picked.size(); k-- > 0;)
        {
            live[picked[k]] = live.back();
            live.pop_back();
        }
        unsigned stats_a = unsigned(random(2));
        uint64_t num_requests = random(500);
        unsigned n = unsigned(ref_ptrs.size());
        {
            SideScope scope(REF_SIDE);
            ref_flush(size_class_idx, ref_ptrs.data(), n, small, ref_arenas[stats_a], num_requests);
        }
        {
            SideScope scope(OUR_SIDE);
            CacheBinPtrArray array{CacheBinSize(n)};
            array.ptr = our_ptrs.data();
            CacheBinStats stats;
            stats.num_requests = num_requests;
            arenaPtrArrayFlush(our_thread_state, size_class_idx, &array, n, small, our_arenas[stats_a], stats);
        }
        if (small)
            checkAll({size_class_idx});
        else
            checkAll();
    }

    void opReallocateNoMove()
    {
        if (live.empty())
            return;
        Live & l = live[random(live.size())];
        size_t size = random(2) ? randomSmallSize() : randomLargeSize();
        size_t extra = random(2) ? 0 : random(size_t(1) << (LOG2_PAGE + 2));
        if (size + extra > SIZE_CLASS_LARGE_MAX_CLASS)
            extra = 0;
        bool zero = random(3) == 0;
        size_t ref_new_size;
        size_t our_new_size;
        bool ref_result;
        bool our_result;
        {
            SideScope scope(REF_SIDE);
            ref_result = ref_reallocate_no_move(l.ref, l.usable_size, size, extra, zero, &ref_new_size);
        }
        {
            SideScope scope(OUR_SIDE);
            our_result = arenaReallocateNoMove(&our_thread_state, l.our, l.usable_size, size, extra, zero, &our_new_size);
        }
        CHECK_EQ(ref_result, our_result);
        CHECK_EQ(ref_new_size, our_new_size);
        CHECK_EQ(ref_allocation_size(l.ref), arenaAllocationSize(&our_thread_state, l.our));
        l.usable_size = arenaAllocationSize(&our_thread_state, l.our);
        l.size_class_idx = size_classes::sizeToIndex(l.usable_size);
        l.small = l.size_class_idx < SIZE_CLASS_NUM_BINS;
        checkAll();
    }

    void opReallocate()
    {
        if (live.empty())
            return;
        size_t i = random(live.size());
        Live l = live[i];
        unsigned a = unsigned(random(2));
        size_t size = random(2) ? randomSmallSize() : randomLargeSize();
        size_t alignment = random(3) == 0 ? (size_t(1) << (4 + random(LOG2_PAGE + 2 - 4))) : 0;
        size_t usable_size = alignment == 0 ? size_classes::sizeToUsableSize(size) : size_classes::alignedSizeToUsableSize(size, alignment);
        if (usable_size == 0 || usable_size > SIZE_CLASS_LARGE_MAX_CLASS)
            return;
        bool slab = size_classes::canUseSlab(usable_size) && alignment <= PAGE;
        bool zero = random(4) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_reallocate(ref_arenas[a], l.ref, l.usable_size, size, alignment, zero, slab);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaReallocate(&our_thread_state, our_arenas[a], l.our, l.usable_size, size, alignment, zero, slab, nullptr);
        }
        CHECK(samePtr(ref, our, step, "ralloc"));
        if (ref != nullptr && our != nullptr)
        {
            live[i] = live.back();
            live.pop_back();
            /// The arena of the result: the original one if it was not moved.
            unsigned result_arena = (our == l.our) ? l.arena : a;
            addLive(ref, our, result_arena, "ralloc result");
        }
        checkAll();
    }

    void opDecay()
    {
        unsigned a = unsigned(random(2));
        bool all = random(3) == 0;
        {
            SideScope scope(REF_SIDE);
            ref_decay(ref_arenas[a], all);
        }
        {
            SideScope scope(OUR_SIDE);
            arenaDecay(&our_thread_state, our_arenas[a], false, all);
        }
        checkAll();
    }

    void opPromote()
    {
        /// A sampled small allocation: a large extent of `bumped_usable_size` (PAGE-aligned, not a slab) that reports the
        /// small usize.
        unsigned a = unsigned(random(2));
        size_t usable_size = size_classes::indexToSize(SizeClassIdx(random(SIZE_CLASS_NUM_BINS)));
        size_t bumped_usable_size = size_classes::alignedSizeToUsableSize(usable_size, PROFILING_SAMPLE_ALIGNMENT);
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_allocate_aligned(ref_arenas[a], bumped_usable_size, PROFILING_SAMPLE_ALIGNMENT, false, false);
            if (ref != nullptr)
                ref_profiling_promote(ref, usable_size, bumped_usable_size);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaAllocateAligned(
                &our_thread_state, our_arenas[a], bumped_usable_size, PROFILING_SAMPLE_ALIGNMENT, false, false, nullptr);
            if (our != nullptr)
                arenaProfilingPromote(&our_thread_state, our, usable_size, bumped_usable_size);
        }
        CHECK(samePtr(ref, our, step, "promoted"));
        if (ref != nullptr && our != nullptr)
        {
            CHECK_EQ(ref_allocation_size(ref), usable_size);
            CHECK_EQ(arenaAllocationSize(&our_thread_state, our), usable_size);
            CHECK_EQ(ref_allocation_size_if_owned(ref), arenaAllocationSizeIfOwned(&our_thread_state, our));
            checkAll();
            {
                SideScope scope(REF_SIDE);
                ref_deallocate_no_thread_cache(ref);
            }
            {
                SideScope scope(OUR_SIDE);
                arenaDeallocateNoThreadCache(&our_thread_state, our);
            }
        }
        checkAll();
    }

    void opAllocationSize()
    {
        if (live.empty())
            return;
        const Live & l = live[random(live.size())];
        CHECK_EQ(ref_allocation_size(l.ref), arenaAllocationSize(&our_thread_state, l.our));
        CHECK_EQ(ref_allocation_size_if_owned(l.ref), arenaAllocationSizeIfOwned(&our_thread_state, l.our));
        /// An interior pointer of a slab and a pointer that is not managed.
        if (l.small)
            CHECK_EQ(
                ref_allocation_size_if_owned(static_cast<char *>(l.ref) + 1),
                arenaAllocationSizeIfOwned(&our_thread_state, static_cast<char *>(l.our) + 1));
    }

    size_t op_counts[14] = {};
    size_t max_live = 0;
    size_t checks = 0;

    void run(int num_steps)
    {
        for (step = 0; step < num_steps && allocator_test::failureCount() == 0; ++step)
        {
            size_t op = random(14);
            ++op_counts[op];
            max_live = maxOf(max_live, live.size());
            switch (op)
            {
                case 0:
                case 1:
                case 2: opMallocSmall(); break;
                case 3: opMallocLarge(); break;
                case 4: opAllocateAligned(); break;
                case 5:
                case 6: opDeallocate(); break;
                case 7: opFill(); break;
                case 8: opFlush(); break;
                case 9: opReallocateNoMove(); break;
                case 10: opReallocate(); break;
                case 11:
                    if (random(4) == 0)
                        opDecay();
                    else
                        opFillFresh();
                    break;
                case 12:
                    /// Only sampled allocations are promoted (`profiling` is off with an empty compiled-in conf, e.g. s390x).
                    if (options.profiling)
                        opPromote();
                    break;
                case 13: opAllocationSize(); break;
            }
        }
    }

    /// Reset (frees everything), purge and destroy both arenas.
    void finish()
    {
        for (unsigned a = 0; a < 2; ++a)
        {
            {
                SideScope scope(REF_SIDE);
                ref_reset(ref_arenas[a]);
            }
            {
                SideScope scope(OUR_SIDE);
                arenaReset(our_thread_state, our_arenas[a]);
            }
            checkAll();
            {
                SideScope scope(REF_SIDE);
                ref_decay(ref_arenas[a], true);
            }
            {
                SideScope scope(OUR_SIDE);
                arenaDecay(&our_thread_state, our_arenas[a], false, true);
            }
            checkAll();
        }
        live.clear();
        for (unsigned a = 0; a < 2; ++a)
        {
            {
                SideScope scope(REF_SIDE);
                ref_destroy(ref_arenas[a]);
            }
            {
                SideScope scope(OUR_SIDE);
                arenaDestroy(our_thread_state, our_arenas[a]);
            }
            CHECK(arenaGet(&our_thread_state, arenaIdxGet(our_arenas[a]), false) == nullptr);
        }
        CHECK(sameRng(step));
    }
};

}

TEST(ArenaOracle, RandomScripts)
{
    for (uint64_t seed = 1; seed <= 6 && allocator_test::failureCount() == 0; ++seed)
    {
        Oracle oracle(seed);
        oracle.run(1500);
        std::fprintf(
            stderr, "seed %llu: %zu checks, max live %zu, ops:", static_cast<unsigned long long>(seed), oracle.checks, oracle.max_live);
        for (size_t c : oracle.op_counts)
            std::fprintf(stderr, " %zu", c);
        std::fprintf(stderr, "\n");
        oracle.finish();
    }
}

TEST(ArenaOracle, DirtyDecayImmediately)
{
    /// With `dirty_decay_ms` = 0, every deallocation that generates dirty pages purges immediately
    /// (`arena_handle_deferred_work`).
    Oracle oracle(100);
    for (unsigned a = 0; a < 2; ++a)
    {
        {
            SideScope scope(REF_SIDE);
            CHECK(!ref_decay_ms_set(oracle.ref_arenas[a], 0, 0));
        }
        {
            SideScope scope(OUR_SIDE);
            CHECK(!arenaDecayMsSet(&our_thread_state, oracle.our_arenas[a], extent_state_dirty, 0));
        }
    }
    oracle.checkAll();
    oracle.run(800);
    oracle.finish();
}
