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
/// (`narenas_auto`, `manual_arena_base`, `oversize_threshold`, the decay defaults, `opt_prof`) is copied from it to the
/// C++ side; the reference's background threads are disabled through their state flag. Our side does not create arena
/// 0 / the auto arenas (only the arena table entries of the test arenas exist). The extent map is global on both
/// sides, so rtree metadata is not compared (`metadata_rtree` is 0 for a non-zero arena's base on both sides).

#include <allocator/ArenaInlines.h>
#include <allocator/Arena.h>
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

extern "C"
{
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

constinit ThreadState our_tsd;

ref_globals_t ref_globals;

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
    opt.prof = ref_globals.opt_prof;
    opt.retain = ref_globals.opt_retain;
    opt.cache_oblivious = ref_globals.opt_cache_oblivious;
    opt.calloc_madvise_threshold = ref_globals.calloc_madvise_threshold;
    opt.lg_extent_max_active_fit = ref_globals.lg_extent_max_active_fit;
    opt.dirty_decay_ms = ref_globals.dirty_decay_ms_default;
    opt.muzzy_decay_ms = ref_globals.muzzy_decay_ms_default;

    REQUIRE(!pages::boot());
    szBoot(default_sc_data, opt.cache_oblivious);
    REQUIRE(sz_large_pad == ref_globals.sz_large_pad);
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_emap_global.init(b0get(), /* zeroed */ true));
    REQUIRE(!arenaBoot(&default_sc_data, b0get(), false));
    REQUIRE(arena_nbins_total == ref_globals.nbins_total);
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));

    oversize_threshold = ref_globals.oversize_threshold;
    narenas_auto = ref_globals.narenas_auto;
    manual_arena_base = ref_globals.manual_arena_base;
    narenasTotalSet(ref_globals.narenas_total);

    /// A nominal (slow) state, as for a thread that has initialized its tsd (`arenaNew` enters and leaves reentrancy,
    /// which recomputes the state). The object is not in the nominal list.
    our_tsd.state.store(tsd_state_nominal_slow, std::memory_order_relaxed);

    /// The same PRNG / ticker state on both sides.
    uint64_t prng;
    int32_t tick;
    int32_t nticks;
    ref_tsd_rng_get(&prng, &tick, &nticks);
    our_tsd.prngState() = prng;
    our_tsd.arena_decay_ticker.tick = tick;
    our_tsd.arena_decay_ticker.nticks = nticks;
}

struct Norm
{
    bool ok = false;
    size_t rank = 0;
    size_t offset = 0;

    bool operator==(const Norm & other) const = default;
};

Norm normalize(int side, const void * p)
{
    Norm n;
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
    Norm r = normalize(REF_SIDE, ref);
    Norm o = normalize(OUR_SIDE, our);
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

void putMutex(std::vector<uint64_t> & out, const MutexProfData & d)
{
    put(out, d.n_lock_ops);
    put(out, d.n_owner_switches);
    put(out, d.n_wait_times);
    put(out, d.n_spin_acquired);
    put(out, d.max_n_thds);
}

/// The same order as `ref_stats`.
std::vector<uint64_t> ourStats(Arena * arena)
{
    static ArenaStats astats;
    static BinStatsData bstats[SC_NBINS];
    static ArenaStatsLarge lstats[SC_NSIZES - SC_NBINS];
    static PacExtentStats estats[SC_NPSIZES];
    std::memset(static_cast<void *>(&astats), 0, sizeof(astats));
    std::memset(static_cast<void *>(bstats), 0, sizeof(bstats));
    std::memset(static_cast<void *>(lstats), 0, sizeof(lstats));
    std::memset(static_cast<void *>(estats), 0, sizeof(estats));

    unsigned nthreads = 0;
    const char * dss = nullptr;
    ssize_t dirty_decay_ms = 0;
    ssize_t muzzy_decay_ms = 0;
    size_t nactive = 0;
    size_t ndirty = 0;
    size_t nmuzzy = 0;
    arenaStatsMerge(
        &our_tsd, arena, &nthreads, &dss, &dirty_decay_ms, &muzzy_decay_ms, &nactive, &ndirty, &nmuzzy, &astats, bstats, lstats, estats);

    std::vector<uint64_t> out;
    put(out, nthreads);
    uint64_t dss_ind = 99;
    for (unsigned i = 0; i < unsigned(DssPrec::Limit); ++i)
        if (std::strcmp(dss, dss_prec_names[i]) == 0)
            dss_ind = i;
    put(out, dss_ind);
    put(out, uint64_t(dirty_decay_ms));
    put(out, uint64_t(muzzy_decay_ms));
    put(out, nactive);
    put(out, ndirty);
    put(out, nmuzzy);

    put(out, astats.base);
    put(out, astats.metadata_edata);
    put(out, astats.metadata_rtree);
    put(out, astats.resident);
    put(out, astats.metadata_thp);
    put(out, astats.mapped);
    put(out, astats.internal.load());
    put(out, astats.allocated_large);
    put(out, astats.nmalloc_large);
    put(out, astats.ndalloc_large);
    put(out, astats.nfills_large);
    put(out, astats.nflushes_large);
    put(out, astats.nrequests_large);
    put(out, astats.pa_shard_stats.edata_avail);
    const PacStats & pac = astats.pa_shard_stats.pac_stats;
    put(out, pac.decay_dirty.npurge.readUnsynchronized());
    put(out, pac.decay_dirty.nmadvise.readUnsynchronized());
    put(out, pac.decay_dirty.purged.readUnsynchronized());
    put(out, pac.decay_muzzy.npurge.readUnsynchronized());
    put(out, pac.decay_muzzy.nmadvise.readUnsynchronized());
    put(out, pac.decay_muzzy.purged.readUnsynchronized());
    put(out, pac.retained);
    put(out, pac.pac_mapped.load());
    put(out, pac.abandoned_vm.load());
    put(out, astats.tcache_bytes);
    put(out, astats.tcache_stashed_bytes);
    for (unsigned i = 0; i < mutex_prof_num_arena_mutexes; ++i)
        putMutex(out, astats.mutex_prof_data[i]);

    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        const BinStats & b = bstats[i].stats_data;
        put(out, b.nmalloc);
        put(out, b.ndalloc);
        put(out, b.nrequests);
        put(out, b.curregs);
        put(out, b.nfills);
        put(out, b.nflushes);
        put(out, b.nslabs);
        put(out, b.reslabs);
        put(out, b.curslabs);
        put(out, b.nonfull_slabs);
        putMutex(out, bstats[i].mutex_data);
    }
    for (unsigned i = 0; i < SC_NSIZES - SC_NBINS; ++i)
    {
        const ArenaStatsLarge & l = lstats[i];
        put(out, l.nmalloc.readUnsynchronized());
        put(out, l.ndalloc.readUnsynchronized());
        put(out, l.active_bytes.readUnsynchronized());
        put(out, l.nrequests.readUnsynchronized());
        put(out, l.nfills.readUnsynchronized());
        put(out, l.nflushes.readUnsynchronized());
        put(out, l.curlextents);
    }
    for (unsigned i = 0; i < SC_NPSIZES; ++i)
    {
        const PacExtentStats & e = estats[i];
        put(out, e.ndirty);
        put(out, e.dirty_bytes);
        put(out, e.nmuzzy);
        put(out, e.muzzy_bytes);
        put(out, e.nretained);
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
    int32_t nticks;
    ref_tsd_rng_get(&prng, &tick, &nticks);
    bool ok = prng == our_tsd.prngState() && tick == our_tsd.arena_decay_ticker.tick && nticks == our_tsd.arena_decay_ticker.nticks;
    if (!ok)
        std::fprintf(
            stderr,
            "step %d: rng ref (%llx, %d, %d) our (%llx, %d, %d)\n",
            step,
            static_cast<unsigned long long>(prng),
            tick,
            nticks,
            static_cast<unsigned long long>(our_tsd.prngState()),
            our_tsd.arena_decay_ticker.tick,
            our_tsd.arena_decay_ticker.nticks);
    return ok;
}

bool sameBin(void * ref_arena, Arena * our_arena, unsigned binind, int step)
{
    uintptr_t ref_slabcur;
    unsigned ref_nfree;
    uintptr_t ref_first;
    size_t ref_nfull;
    ref_bin_state(ref_arena, binind, &ref_slabcur, &ref_nfree, &ref_first, &ref_nfull);

    Bin * bin = arenaGetBin(our_arena, binind, 0);
    void * our_slabcur = bin->slabcur ? bin->slabcur->addr() : nullptr;
    unsigned our_nfree = bin->slabcur ? bin->slabcur->nfree() : 0;
    Extent * first = bin->slabs_nonfull.first();
    void * our_first = first ? first->addr() : nullptr;
    size_t our_nfull = 0;
    for (Extent * e = bin->slabs_full.first(); e != nullptr; e = bin->slabs_full.next(e))
        ++our_nfull;

    bool ok = samePtr(reinterpret_cast<void *>(ref_slabcur), our_slabcur, step, "slabcur") && ref_nfree == our_nfree
        && samePtr(reinterpret_cast<void *>(ref_first), our_first, step, "nonfull first") && ref_nfull == our_nfull;
    if (!ok)
        std::fprintf(stderr, "step %d: bin %u: nfree %u/%u nfull %zu/%zu\n", step, binind, ref_nfree, our_nfree, ref_nfull, our_nfull);
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
    size_t usize;
    szind_t szind;
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
            unsigned ind = narenasTotalGet();
            {
                SideScope scope(REF_SIDE);
                ref_arenas[a] = ref_arena_new(ind);
            }
            {
                SideScope scope(OUR_SIDE);
                our_arenas[a] = arenaNew(&our_tsd, ind, &arena_config_default);
            }
            REQUIRE(ref_arenas[a] != nullptr);
            REQUIRE(our_arenas[a] != nullptr);
            REQUIRE(ref_arena_ind(ref_arenas[a]) == ind);
            REQUIRE(arenaIndGet(our_arenas[a]) == ind);
            /// The reference does not count arenas created by `arena_new` directly; advance our counter in the same way
            /// as `arena_init_locked` would for the next index, and keep the reference's view consistent: both sides
            /// use `ind + 1` for the second arena.
            narenasTotalSet(ind + 1);
            char ref_name[ARENA_NAME_LEN];
            char our_name[ARENA_NAME_LEN];
            ref_arena_name(ref_arenas[a], ref_name);
            arenaNameGet(our_arenas[a], our_name);
            CHECK_EQ(std::strcmp(ref_name, our_name), 0);
        }
    }

    size_t rnd(size_t n) { return size_t(rng() % n); }

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
        size_t ref_usize = ref_salloc(ref);
        size_t our_usize = arenaSalloc(&our_tsd, our);
        CHECK_EQ(ref_usize, our_usize);
        szind_t ind = sz::sizeToIndex(our_usize);
        live.push_back({ref, our, our_usize, ind, ind < SC_NBINS, a});
        /// `prof_malloc` of an allocation that is not sampled (`opt.prof` is on): the extent's tctx of a large
        /// allocation is reset (otherwise it may hold garbage from a previous slab use of the extent).
        if (opt.prof)
        {
            ref_prof_tctx_reset(ref);
            arenaProfTctxReset(our_tsd, our, nullptr);
        }
        /// Touch the memory (the same pattern on both sides) so that later zeroing decisions matter.
        std::memset(ref, 0x5a, minOf<size_t>(our_usize, 64));
        std::memset(our, 0x5a, minOf<size_t>(our_usize, 64));
    }

    size_t randomSmallSize()
    {
        szind_t ind = szind_t(rnd(4) == 0 ? rnd(SC_NBINS) : rnd(12));
        size_t usize = sz::indexToSize(ind);
        size_t lo = ind == 0 ? 1 : sz::indexToSize(ind - 1) + 1;
        return lo + rnd(usize - lo + 1);
    }

    size_t randomLargeSize()
    {
        size_t lg = SC_LG_LARGE_MINCLASS + rnd(6);
        return (size_t(1) << lg) + rnd(size_t(1) << lg);
    }

    void opMallocSmall()
    {
        unsigned a = unsigned(rnd(2));
        size_t size = randomSmallSize();
        szind_t ind = sz::sizeToIndex(size);
        bool zero = rnd(4) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_malloc_hard(ref_arenas[a], size, ind, zero, true);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaMallocHard(&our_tsd, our_arenas[a], size, ind, zero, true);
        }
        if (zero && our != nullptr)
            CHECK_EQ(static_cast<unsigned char *>(our)[0], 0);
        addLive(ref, our, a, "malloc small");
        checkAll({ind});
    }

    void opMallocLarge()
    {
        unsigned a = unsigned(rnd(2));
        size_t size = randomLargeSize();
        szind_t ind = sz::sizeToIndex(size);
        bool zero = rnd(3) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_malloc_hard(ref_arenas[a], size, ind, zero, false);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaMallocHard(&our_tsd, our_arenas[a], size, ind, zero, false);
        }
        if (zero && our != nullptr)
            CHECK_EQ(static_cast<unsigned char *>(our)[sz::s2u(size) - 1], 0);
        addLive(ref, our, a, "malloc large");
        checkAll();
    }

    void opPalloc()
    {
        unsigned a = unsigned(rnd(2));
        size_t alignment = size_t(1) << (4 + rnd(LG_PAGE + 2 - 4));
        size_t size = rnd(2) ? randomSmallSize() : randomLargeSize();
        size_t usize = sz::sa2u(size, alignment);
        if (usize == 0 || usize > SC_LARGE_MAXCLASS)
            return;
        bool slab = sz::canUseSlab(usize) && alignment <= PAGE;
        bool zero = rnd(4) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_palloc(ref_arenas[a], usize, alignment, zero, slab);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaPalloc(&our_tsd, our_arenas[a], usize, alignment, zero, slab, nullptr);
        }
        if (our != nullptr)
            CHECK_EQ(reinterpret_cast<uintptr_t>(our) % alignment, 0u);
        addLive(ref, our, a, "palloc");
        checkAll({slab ? sz::sizeToIndex(usize) : 0u});
    }

    void opDalloc()
    {
        if (live.empty())
            return;
        size_t i = rnd(live.size());
        Live l = live[i];
        live[i] = live.back();
        live.pop_back();
        bool sized = rnd(2) == 0;
        {
            SideScope scope(REF_SIDE);
            if (sized)
                ref_sdalloc_no_tcache(l.ref, l.usize);
            else
                ref_dalloc_no_tcache(l.ref);
        }
        {
            SideScope scope(OUR_SIDE);
            if (sized)
                arenaSdallocNoTcache(&our_tsd, l.our, l.usize);
            else
                arenaDallocNoTcache(&our_tsd, l.our);
        }
        if (l.small)
            checkAll({l.szind});
        else
            checkAll();
    }

    void opFill()
    {
        unsigned a = unsigned(rnd(2));
        unsigned binind = unsigned(rnd(4) == 0 ? rnd(SC_NBINS) : rnd(12));
        unsigned nfill_min = 1 + unsigned(rnd(64));
        unsigned nfill_max = nfill_min + unsigned(rnd(200));
        uint64_t nrequests = rnd(1000);
        std::vector<void *> ref_ptrs(nfill_max);
        std::vector<void *> our_ptrs(nfill_max);
        unsigned ref_n;
        unsigned our_n;
        {
            SideScope scope(REF_SIDE);
            ref_n = ref_fill_small(ref_arenas[a], binind, ref_ptrs.data(), nfill_min, nfill_max, nrequests);
        }
        {
            SideScope scope(OUR_SIDE);
            CacheBinPtrArray arr{cache_bin_sz_t(nfill_max)};
            arr.ptr = our_ptrs.data();
            CacheBinStats stats;
            stats.nrequests = nrequests;
            our_n = arenaPtrArrayFillSmall(
                &our_tsd, our_arenas[a], binind, &arr, cache_bin_sz_t(nfill_min), cache_bin_sz_t(nfill_max), stats);
        }
        CHECK_EQ(ref_n, our_n);
        for (unsigned i = 0; i < minOf(ref_n, our_n); ++i)
            addLive(ref_ptrs[i], our_ptrs[i], a, "fill");
        checkAll({binind});
    }

    void opFillFresh()
    {
        unsigned a = unsigned(rnd(2));
        unsigned binind = unsigned(rnd(12));
        size_t nfill = 1 + rnd(300);
        bool zero = rnd(2) == 0;
        std::vector<void *> ref_ptrs(nfill);
        std::vector<void *> our_ptrs(nfill);
        size_t ref_n;
        size_t our_n;
        {
            SideScope scope(REF_SIDE);
            ref_n = ref_fill_small_fresh(ref_arenas[a], binind, ref_ptrs.data(), nfill, zero);
        }
        {
            SideScope scope(OUR_SIDE);
            our_n = arenaFillSmallFresh(&our_tsd, our_arenas[a], binind, our_ptrs.data(), nfill, zero);
        }
        CHECK_EQ(ref_n, our_n);
        for (size_t i = 0; i < minOf(ref_n, our_n); ++i)
            addLive(ref_ptrs[i], our_ptrs[i], a, "fill fresh");
        checkAll({binind});
    }

    void opFlush()
    {
        if (live.empty())
            return;
        /// Pick a size class of a random live object and flush a random subset of the objects of that class (from both
        /// arenas: the flush partitions by arena).
        const Live & sample = live[rnd(live.size())];
        szind_t szind = sample.szind;
        bool small = szind < SC_NBINS;
        /// The tcache only caches (and so only flushes) large size classes up to `TCACHE_MAXCLASS_LIMIT`.
        if (!small && sample.usize > TCACHE_MAXCLASS_LIMIT)
            return;
        std::vector<size_t> picked;
        for (size_t i = 0; i < live.size(); ++i)
            if (live[i].szind == szind && rnd(3) != 0)
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
        unsigned stats_a = unsigned(rnd(2));
        uint64_t nrequests = rnd(500);
        unsigned n = unsigned(ref_ptrs.size());
        {
            SideScope scope(REF_SIDE);
            ref_flush(szind, ref_ptrs.data(), n, small, ref_arenas[stats_a], nrequests);
        }
        {
            SideScope scope(OUR_SIDE);
            CacheBinPtrArray arr{cache_bin_sz_t(n)};
            arr.ptr = our_ptrs.data();
            CacheBinStats stats;
            stats.nrequests = nrequests;
            arenaPtrArrayFlush(our_tsd, szind, &arr, n, small, our_arenas[stats_a], stats);
        }
        if (small)
            checkAll({szind});
        else
            checkAll();
    }

    void opRallocNoMove()
    {
        if (live.empty())
            return;
        Live & l = live[rnd(live.size())];
        size_t size = rnd(2) ? randomSmallSize() : randomLargeSize();
        size_t extra = rnd(2) ? 0 : rnd(size_t(1) << (LG_PAGE + 2));
        if (size + extra > SC_LARGE_MAXCLASS)
            extra = 0;
        bool zero = rnd(3) == 0;
        size_t ref_newsize;
        size_t our_newsize;
        bool ref_ret;
        bool our_ret;
        {
            SideScope scope(REF_SIDE);
            ref_ret = ref_ralloc_no_move(l.ref, l.usize, size, extra, zero, &ref_newsize);
        }
        {
            SideScope scope(OUR_SIDE);
            our_ret = arenaRallocNoMove(&our_tsd, l.our, l.usize, size, extra, zero, &our_newsize);
        }
        CHECK_EQ(ref_ret, our_ret);
        CHECK_EQ(ref_newsize, our_newsize);
        CHECK_EQ(ref_salloc(l.ref), arenaSalloc(&our_tsd, l.our));
        l.usize = arenaSalloc(&our_tsd, l.our);
        l.szind = sz::sizeToIndex(l.usize);
        l.small = l.szind < SC_NBINS;
        checkAll();
    }

    void opRalloc()
    {
        if (live.empty())
            return;
        size_t i = rnd(live.size());
        Live l = live[i];
        unsigned a = unsigned(rnd(2));
        size_t size = rnd(2) ? randomSmallSize() : randomLargeSize();
        size_t alignment = rnd(3) == 0 ? (size_t(1) << (4 + rnd(LG_PAGE + 2 - 4))) : 0;
        size_t usize = alignment == 0 ? sz::s2u(size) : sz::sa2u(size, alignment);
        if (usize == 0 || usize > SC_LARGE_MAXCLASS)
            return;
        bool slab = sz::canUseSlab(usize) && alignment <= PAGE;
        bool zero = rnd(4) == 0;
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_ralloc(ref_arenas[a], l.ref, l.usize, size, alignment, zero, slab);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaRalloc(&our_tsd, our_arenas[a], l.our, l.usize, size, alignment, zero, slab, nullptr);
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
        unsigned a = unsigned(rnd(2));
        bool all = rnd(3) == 0;
        {
            SideScope scope(REF_SIDE);
            ref_decay(ref_arenas[a], all);
        }
        {
            SideScope scope(OUR_SIDE);
            arenaDecay(&our_tsd, our_arenas[a], false, all);
        }
        checkAll();
    }

    void opPromote()
    {
        /// A sampled small allocation: a large extent of `bumped_usize` (PAGE-aligned, not a slab) that reports the
        /// small usize.
        unsigned a = unsigned(rnd(2));
        size_t usize = sz::indexToSize(szind_t(rnd(SC_NBINS)));
        size_t bumped_usize = sz::sa2u(usize, PROF_SAMPLE_ALIGNMENT);
        void * ref;
        void * our;
        {
            SideScope scope(REF_SIDE);
            ref = ref_palloc(ref_arenas[a], bumped_usize, PROF_SAMPLE_ALIGNMENT, false, false);
            if (ref != nullptr)
                ref_prof_promote(ref, usize, bumped_usize);
        }
        {
            SideScope scope(OUR_SIDE);
            our = arenaPalloc(&our_tsd, our_arenas[a], bumped_usize, PROF_SAMPLE_ALIGNMENT, false, false, nullptr);
            if (our != nullptr)
                arenaProfPromote(&our_tsd, our, usize, bumped_usize);
        }
        CHECK(samePtr(ref, our, step, "promoted"));
        if (ref != nullptr && our != nullptr)
        {
            CHECK_EQ(ref_salloc(ref), usize);
            CHECK_EQ(arenaSalloc(&our_tsd, our), usize);
            CHECK_EQ(ref_vsalloc(ref), arenaVsalloc(&our_tsd, our));
            checkAll();
            {
                SideScope scope(REF_SIDE);
                ref_dalloc_no_tcache(ref);
            }
            {
                SideScope scope(OUR_SIDE);
                arenaDallocNoTcache(&our_tsd, our);
            }
        }
        checkAll();
    }

    void opSalloc()
    {
        if (live.empty())
            return;
        const Live & l = live[rnd(live.size())];
        CHECK_EQ(ref_salloc(l.ref), arenaSalloc(&our_tsd, l.our));
        CHECK_EQ(ref_vsalloc(l.ref), arenaVsalloc(&our_tsd, l.our));
        /// An interior pointer of a slab and a pointer that is not managed.
        if (l.small)
            CHECK_EQ(ref_vsalloc(static_cast<char *>(l.ref) + 1), arenaVsalloc(&our_tsd, static_cast<char *>(l.our) + 1));
    }

    size_t op_counts[14] = {};
    size_t max_live = 0;
    size_t checks = 0;

    void run(int nsteps)
    {
        for (step = 0; step < nsteps && allocator_test::failureCount() == 0; ++step)
        {
            size_t op = rnd(14);
            ++op_counts[op];
            max_live = maxOf(max_live, live.size());
            switch (op)
            {
                case 0:
                case 1:
                case 2:
                    opMallocSmall();
                    break;
                case 3:
                    opMallocLarge();
                    break;
                case 4:
                    opPalloc();
                    break;
                case 5:
                case 6:
                    opDalloc();
                    break;
                case 7:
                    opFill();
                    break;
                case 8:
                    opFlush();
                    break;
                case 9:
                    opRallocNoMove();
                    break;
                case 10:
                    opRalloc();
                    break;
                case 11:
                    if (rnd(4) == 0)
                        opDecay();
                    else
                        opFillFresh();
                    break;
                case 12:
                    /// Only sampled allocations are promoted (`prof` is off with an empty compiled-in conf, e.g. s390x).
                    if (opt.prof)
                        opPromote();
                    break;
                case 13:
                    opSalloc();
                    break;
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
                arenaReset(our_tsd, our_arenas[a]);
            }
            checkAll();
            {
                SideScope scope(REF_SIDE);
                ref_decay(ref_arenas[a], true);
            }
            {
                SideScope scope(OUR_SIDE);
                arenaDecay(&our_tsd, our_arenas[a], false, true);
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
                arenaDestroy(our_tsd, our_arenas[a]);
            }
            CHECK(arenaGet(&our_tsd, arenaIndGet(our_arenas[a]), false) == nullptr);
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
        std::fprintf(stderr, "seed %llu: %zu checks, max live %zu, ops:", static_cast<unsigned long long>(seed), oracle.checks, oracle.max_live);
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
            CHECK(!arenaDecayMsSet(&our_tsd, oracle.our_arenas[a], extent_state_dirty, 0));
        }
    }
    oracle.checkAll();
    oracle.run(800);
    oracle.finish();
}
