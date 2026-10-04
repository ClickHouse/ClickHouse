/// Compares `PaShard` / `PageAllocator` (and everything below: `ExtentOps`, `ExtentCache`, `ExtentSet`, `Sanitizer`
/// guard pages and the bump allocator) with jemalloc's `pa.c` / `pac.c` / `extent.c` linked from the reference
/// `lib_jemalloc.a`. A C shard (with its own base and emap) and a C++ shard receive identical randomized sequences of
/// `pa_alloc` / `pa_dalloc` / `pa_expand` / `pa_shrink` / decay / settings calls. Addresses come from mmap, so they are
/// normalized to (region, offset) by interposing `mmap`/`munmap` (see the reference file); the clock is a fake one
/// (interposed `clock_gettime`) driven explicitly, in steps that never fall into the window where the randomized decay
/// deadline (seeded from the address of the decay structure, which differs) could decide the epoch advance.
/// After every step, the returned extents, the full contents (LRU order) of all six extent sets, every stat of
/// `pa_shard_stats_merge` / `pa_shard_basic_stats_merge` (including the per-size extent stats), the decay state, the
/// serial number and grow state must be identical.

#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/ExtentOps.h>
#include <allocator/Options.h>
#include <allocator/PageAllocator.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include "Test.h"
#include "page_allocator_oracle_ref.h"

#include <cstdlib>
#include <cstring>
#include <new>
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

void bootOnce()
{
    static bool booted = false;
    if (!booted)
    {
        REQUIRE(ref_boot() == 0);
        REQUIRE(!pages::boot());
        booted = true;
    }
}

struct NormalizedAddr
{
    bool ok;
    size_t rank;
    size_t offset;

    bool operator==(const NormalizedAddr & other) const = default;
};

NormalizedAddr normalize(int side, uintptr_t addr)
{
    NormalizedAddr result{};
    result.ok = ref_normalize(side, addr, &result.rank, &result.offset);
    return result;
}

ref_extent_info_t ourInfo(const Extent * e)
{
    ref_extent_info_t info{};
    info.addr = reinterpret_cast<uintptr_t>(e->e_addr);
    info.size = e->size();
    info.sn = e->sn();
    info.state = e->state();
    info.szind = e->szindMaybeInvalid();
    info.zeroed = e->zeroed();
    info.committed = e->committed();
    info.guarded = e->guarded();
    info.slab = e->slab();
    info.is_head = e->isHead();
    info.arena_ind = unsigned((e->e_bits & extent_bits::arena.mask()) >> extent_bits::arena.shift);
    return info;
}

bool sameInfo(const ref_extent_info_t & r, const ref_extent_info_t & o, const char * what, int step)
{
    NormalizedAddr ra = normalize(REF_SIDE, r.addr);
    NormalizedAddr oa = normalize(OUR_SIDE, o.addr);
    bool ok = ra.ok && oa.ok && ra == oa && r.size == o.size && r.sn == o.sn && r.state == o.state && r.szind == o.szind
        && r.zeroed == o.zeroed && r.committed == o.committed && r.guarded == o.guarded && r.slab == o.slab && r.is_head == o.is_head
        && r.arena_ind == o.arena_ind;
    if (!ok)
        std::fprintf(
            stderr,
            "step %d: %s: ref (region %zu + %zu, ok %d, size %zu, sn %llu, state %u, szind %u, z%d c%d g%d s%d h%d a%u) vs "
            "our (region %zu + %zu, ok %d, size %zu, sn %llu, state %u, szind %u, z%d c%d g%d s%d h%d a%u)\n",
            step,
            what,
            ra.rank,
            ra.offset,
            int(ra.ok),
            r.size,
            static_cast<unsigned long long>(r.sn),
            r.state,
            r.szind,
            r.zeroed,
            r.committed,
            r.guarded,
            r.slab,
            r.is_head,
            r.arena_ind,
            oa.rank,
            oa.offset,
            int(oa.ok),
            o.size,
            static_cast<unsigned long long>(o.sn),
            o.state,
            o.szind,
            o.zeroed,
            o.committed,
            o.guarded,
            o.slab,
            o.is_head,
            o.arena_ind);
    return ok;
}

struct Live
{
    void * ref;
    Extent * our;
    size_t size;
    bool slab;
    bool guarded;
};

struct Harness
{
    void * ref = nullptr;
    PaShard * our = nullptr;
    PaShardStats * our_stats = nullptr;
    ExtentMap * our_emap = nullptr;
    Base * our_base = nullptr;
    std::vector<Live> live;

    Harness(unsigned ind, ssize_t dirty_ms, ssize_t muzzy_ms, size_t oversize_threshold)
    {
        ref_trace_set_side(REF_SIDE);
        ref = ref_shard_new(ind, dirty_ms, muzzy_ms, oversize_threshold);
        REQUIRE(ref != nullptr);

        ref_trace_set_side(OUR_SIDE);
        our_base = Base::create(nullptr, ind, &ehooks_default_extent_hooks, true);
        REQUIRE(our_base != nullptr);
        void * emap_memory = std::aligned_alloc(64, (sizeof(ExtentMap) + 63) / 64 * 64);
        std::memset(emap_memory, 0, sizeof(ExtentMap));
        our_emap = new (emap_memory) ExtentMap();
        REQUIRE(!our_emap->init(our_base, /* zeroed */ true));
        our_stats = new PaShardStats();
        void * shard_memory = std::aligned_alloc(64, (sizeof(PaShard) + 63) / 64 * 64);
        std::memset(shard_memory, 0, sizeof(PaShard));
        our = new (shard_memory) PaShard();
        NsTime cur;
        cur.initUpdate();
        REQUIRE(!our->init(nullptr, our_emap, our_base, ind, our_stats, nullptr, cur, oversize_threshold, dirty_ms, muzzy_ms));
        ref_trace_set_side(-1);
    }

    template <typename F>
    static void onSide(int side, F && f)
    {
        ref_trace_set_side(side);
        f();
        ref_trace_set_side(-1);
    }

    bool compareExtentSets(int step)
    {
        static ref_extent_info_t ref_list[20000];
        bool ok = true;
        for (int which = 0; which < 3; ++which)
        {
            ExtentCache * ecache = which == 0 ? &our->pac.ecache_dirty : (which == 1 ? &our->pac.ecache_muzzy : &our->pac.ecache_retained);
            for (int guarded = 0; guarded < 2; ++guarded)
            {
                size_t n = ref_ecache_list(ref, which, guarded, ref_list, 20000);
                ExtentSet & eset = guarded ? ecache->guarded_eset : ecache->eset;
                size_t k = 0;
                for (Extent * e = eset.lru.first(); e != nullptr; e = eset.lru.next(e), ++k)
                {
                    if (k >= n)
                    {
                        std::fprintf(stderr, "step %d: ecache %d/%d: more extents than the reference\n", step, which, guarded);
                        return false;
                    }
                    char what[64];
                    std::snprintf(what, sizeof(what), "ecache %d/%d LRU #%zu", which, guarded, k);
                    if (!sameInfo(ref_list[k], ourInfo(e), what, step))
                        return false;
                }
                if (k != n)
                {
                    std::fprintf(stderr, "step %d: ecache %d/%d: %zu extents vs %zu\n", step, which, guarded, n, k);
                    ok = false;
                }
            }
        }
        return ok;
    }

    void ourStats(uint64_t * out)
    {
        size_t nactive = 0;
        size_t ndirty = 0;
        size_t nmuzzy = 0;
        our->basicStatsMerge(&nactive, &ndirty, &nmuzzy);

        static PaShardStats stats;
        static PacExtentStats estats[SC_NPSIZES];
        stats.~PaShardStats();
        new (&stats) PaShardStats();
        std::memset(estats, 0, sizeof(estats));
        size_t resident = 0;
        our->statsMerge(nullptr, &stats, estats, &resident);

        size_t i = 0;
        out[i++] = nactive;
        out[i++] = ndirty;
        out[i++] = nmuzzy;
        out[i++] = resident;
        out[i++] = stats.edata_avail;
        out[i++] = stats.pac_stats.retained;
        out[i++] = stats.pac_stats.pac_mapped.load();
        out[i++] = stats.pac_stats.abandoned_vm.load();
        out[i++] = our->pac.mapped();
        out[i++] = stats.pac_stats.decay_dirty.npurge.read();
        out[i++] = stats.pac_stats.decay_dirty.nmadvise.read();
        out[i++] = stats.pac_stats.decay_dirty.purged.read();
        out[i++] = stats.pac_stats.decay_muzzy.npurge.read();
        out[i++] = stats.pac_stats.decay_muzzy.nmadvise.read();
        out[i++] = stats.pac_stats.decay_muzzy.purged.read();
        out[i++] = our->pac.extent_sn_next.load();
        out[i++] = our->pac.exp_grow.next;
        out[i++] = our->pac.exp_grow.limit;
        out[i++] = our->edata_cache.count();
        for (unsigned j = 0; j < SC_NPSIZES; ++j)
        {
            out[i++] = estats[j].ndirty;
            out[i++] = estats[j].dirty_bytes;
            out[i++] = estats[j].nmuzzy;
            out[i++] = estats[j].muzzy_bytes;
            out[i++] = estats[j].nretained;
            out[i++] = estats[j].retained_bytes;
        }
    }

    bool compareStats(int step)
    {
        static uint64_t ref_out[4096];
        static uint64_t our_out[4096];
        size_t n = ref_nstats();
        REQUIRE(n <= 4096);
        ref_stats(ref, ref_out);
        ourStats(our_out);
        bool ok = true;
        for (size_t i = 0; i < n; ++i)
        {
            if (ref_out[i] != our_out[i])
            {
                std::fprintf(
                    stderr,
                    "step %d: stat #%zu: %llu vs %llu\n",
                    step,
                    i,
                    static_cast<unsigned long long>(ref_out[i]),
                    static_cast<unsigned long long>(our_out[i]));
                ok = false;
            }
        }
        for (int which = 0; which < 2; ++which)
        {
            uint64_t epoch;
            size_t npages_limit;
            size_t nunpurged;
            size_t backlog_last;
            ref_decay_state(ref, which, &epoch, &npages_limit, &nunpurged, &backlog_last);
            Decay & d = which == 0 ? our->pac.decay_dirty : our->pac.decay_muzzy;
            if (epoch != d.epoch.ns() || npages_limit != d.npages_limit || nunpurged != d.nunpurged
                || backlog_last != d.backlog[SMOOTHSTEP_NSTEPS - 1] || ref_decay_ms_get(ref, which) != d.msRead())
            {
                std::fprintf(stderr, "step %d: decay %d state mismatch\n", step, which);
                ok = false;
            }
        }
        if (ref_nregions_of(REF_SIDE) != ref_nregions_of(OUR_SIDE))
        {
            std::fprintf(stderr, "step %d: %zu regions vs %zu\n", step, ref_nregions_of(REF_SIDE), ref_nregions_of(OUR_SIDE));
            ok = false;
        }
        return ok;
    }

    /// The live extents, and their emap entries.
    bool compareLive(int step)
    {
        for (size_t i = 0; i < live.size(); ++i)
        {
            ref_extent_info_t r;
            ref_extent_info(live[i].ref, &r);
            char what[64];
            std::snprintf(what, sizeof(what), "live #%zu", i);
            if (!sameInfo(r, ourInfo(live[i].our), what, step))
                return false;
            /// The boundary pages map to the extent with its szind and slab.
            unsigned ref_szind = 0;
            bool ref_slab = false;
            void * ref_e = ref_emap_lookup(ref, reinterpret_cast<void *>(r.addr), &ref_szind, &ref_slab);
            FullAllocContext ctx{};
            bool our_missing = our_emap->fullAllocCtxTryLookup(nullptr, live[i].our->e_addr, &ctx);
            if (ref_e != live[i].ref || our_missing || ctx.edata != live[i].our || ctx.szind != ref_szind || ctx.slab != ref_slab)
            {
                std::fprintf(stderr, "step %d: emap mismatch for live #%zu\n", step, i);
                return false;
            }
        }
        return true;
    }

    bool compareAll(int step) { return compareStats(step) && compareExtentSets(step) && compareLive(step); }
};

/// Moves the fake clock forward by about `step_ns`, but never into the window [epoch + interval, epoch + 2 * interval)
/// of a gradual decay, where the (address-seeded, hence different) deadline jitter could decide an epoch advance.
void advanceClock(Harness & h, uint64_t step_ns)
{
    uint64_t t = ref_clock_get() + step_ns;
    bool moved = true;
    while (moved)
    {
        moved = false;
        for (Decay * d : {&h.our->pac.decay_dirty, &h.our->pac.decay_muzzy})
        {
            if (!d->gradually())
                continue;
            uint64_t epoch = d->epoch.ns();
            uint64_t interval = d->interval.ns();
            if (t >= epoch + interval && t < epoch + 2 * interval)
            {
                t = epoch + 2 * interval;
                moved = true;
            }
        }
    }
    ref_clock_set(t);
}

size_t pickLargePages(std::mt19937_64 & rng)
{
    unsigned kind = rng() % 100;
    if (kind < 40)
        return 1 + rng() % 8;
    if (kind < 75)
        return 1 + rng() % 64;
    if (kind < 95)
        return 1 + rng() % 512;
    return 1 + rng() % 2048;
}

struct Workload
{
    unsigned alloc = 35;
    unsigned dalloc = 30;
    unsigned expand = 8;
    unsigned shrink = 7;
    unsigned guarded_permille = 30;
    bool vary_decay_ms = true;
    /// The global `background_thread_enabled` state (disables the oversize purge shortcut of `extent_record`).
    bool background_thread = false;
};

void runSequence(uint64_t seed, int steps, ssize_t dirty_ms, ssize_t muzzy_ms, size_t oversize_threshold, Workload w)
{
    ref_set_background_thread_enabled(w.background_thread);
    background_thread_enabled_state.store(w.background_thread);
    REQUIRE(ref_background_thread_enabled() == backgroundThreadEnabled());

    Harness h(1 + unsigned(seed % 7), dirty_ms, muzzy_ms, oversize_threshold);
    REQUIRE(h.compareAll(-1));

    std::mt19937_64 rng(seed);
    int failures = 0;
    size_t nallocs = 0;
    size_t nexpands = 0;
    size_t npurges = 0;

    for (int step = 0; step < steps && failures < 3; ++step)
    {
        unsigned op = rng() % 100;
        bool ok = true;
        static const bool verbose = std::getenv("ORACLE_VERBOSE") != nullptr;

        if (op < w.alloc || h.live.empty())
        {
            bool slab = rng() % 5 == 0;
            size_t pages = slab ? 1 + rng() % 8 : pickLargePages(rng);
            size_t size = pages * PAGE;
            szind_t szind = slab ? szind_t(rng() % SC_NBINS) : SC_NBINS + szind_t(rng() % 20);
            bool zero = rng() % 5 == 0;
            bool guarded = (rng() % 1000) < w.guarded_permille;
            /// Large allocations are at least `SC_LARGE_MINCLASS` (`pac_dalloc_impl` relies on it for guarded ones).
            if (guarded && !slab && size < SC_LARGE_MINCLASS)
                size = SC_LARGE_MINCLASS + PAGE * (rng() % 4);
            bool ref_deferred = false;
            bool our_deferred = false;
            void * r = nullptr;
            Extent * o = nullptr;
            if (verbose)
                std::fprintf(stderr, "step %d: alloc %zu pages slab %d zero %d guarded %d\n", step, pages, int(slab), int(zero), int(guarded));
            Harness::onSide(REF_SIDE, [&] { r = ref_alloc(h.ref, size, PAGE, slab, szind, zero, guarded, &ref_deferred); });
            Harness::onSide(OUR_SIDE, [&] { o = h.our->alloc(nullptr, size, PAGE, slab, szind, zero, guarded, &our_deferred); });
            if ((r == nullptr) != (o == nullptr) || ref_deferred != our_deferred)
            {
                std::fprintf(stderr, "step %d: alloc result mismatch\n", step);
                ok = false;
            }
            else if (r != nullptr)
            {
                ref_extent_info_t ri;
                ref_extent_info(r, &ri);
                ok = sameInfo(ri, ourInfo(o), "alloc", step);
                if (zero && ok)
                {
                    const unsigned char * p = static_cast<const unsigned char *>(o->e_addr);
                    ok = p[0] == 0 && p[size - 1] == 0;
                }
                h.live.push_back({r, o, size, slab, guarded});
                ++nallocs;
                /// Dirty the memory, so that purging and zeroing matter.
                std::memset(static_cast<char *>(o->e_addr), 0x11, 64);
                std::memset(reinterpret_cast<char *>(ri.addr), 0x11, 64);
            }
        }
        else if (op < w.alloc + w.dalloc)
        {
            size_t i = rng() % h.live.size();
            bool ref_deferred = false;
            bool our_deferred = false;
            if (verbose)
                std::fprintf(stderr, "step %d: dalloc %zu pages\n", step, h.live[i].size / PAGE);
            Harness::onSide(REF_SIDE, [&] { ref_dalloc(h.ref, h.live[i].ref, &ref_deferred); });
            Harness::onSide(OUR_SIDE, [&] { h.our->dalloc(nullptr, h.live[i].our, &our_deferred); });
            ok = ref_deferred == our_deferred;
            h.live.erase(h.live.begin() + long(i));
        }
        else if (op < w.alloc + w.dalloc + w.expand)
        {
            Live & l = h.live[rng() % h.live.size()];
            if (!l.slab)
            {
                size_t new_size = l.size + (1 + rng() % 16) * PAGE;
                szind_t szind = SC_NBINS + szind_t(rng() % 20);
                bool zero = rng() % 3 == 0;
                bool ref_deferred = false;
                bool our_deferred = false;
                bool ref_err = false;
                bool our_err = false;
                if (verbose)
                    std::fprintf(stderr, "step %d: expand %zu -> %zu pages\n", step, l.size / PAGE, new_size / PAGE);
                Harness::onSide(REF_SIDE, [&] { ref_err = ref_expand(h.ref, l.ref, l.size, new_size, szind, zero, &ref_deferred); });
                Harness::onSide(OUR_SIDE, [&] { our_err = h.our->expand(nullptr, l.our, l.size, new_size, szind, zero, &our_deferred); });
                if (ref_err != our_err || ref_deferred != our_deferred)
                {
                    std::fprintf(stderr, "step %d: expand result mismatch (%d vs %d)\n", step, int(ref_err), int(our_err));
                    ok = false;
                }
                else if (!ref_err)
                {
                    l.size = new_size;
                    ++nexpands;
                }
            }
        }
        else if (op < w.alloc + w.dalloc + w.expand + w.shrink)
        {
            Live & l = h.live[rng() % h.live.size()];
            if (!l.slab && l.size > PAGE)
            {
                size_t new_size = l.size - (1 + rng() % (l.size / PAGE - 1)) * PAGE;
                szind_t szind = SC_NBINS + szind_t(rng() % 20);
                bool ref_deferred = false;
                bool our_deferred = false;
                bool ref_err = false;
                bool our_err = false;
                if (verbose)
                    std::fprintf(stderr, "step %d: shrink %zu -> %zu pages\n", step, l.size / PAGE, new_size / PAGE);
                Harness::onSide(REF_SIDE, [&] { ref_err = ref_shrink(h.ref, l.ref, l.size, new_size, szind, &ref_deferred); });
                Harness::onSide(OUR_SIDE, [&] { our_err = h.our->shrink(nullptr, l.our, l.size, new_size, szind, &our_deferred); });
                if (ref_err != our_err || ref_deferred != our_deferred)
                {
                    std::fprintf(stderr, "step %d: shrink result mismatch\n", step);
                    ok = false;
                }
                else if (!ref_err)
                {
                    l.size = new_size;
                }
            }
        }
        else
        {
            unsigned kind = rng() % 100;
            if (kind < 45)
            {
                /// Time passes; maybe purge.
                uint64_t interval = h.our->pac.decay_dirty.interval.ns();
                uint64_t step_ns = rng() % 3 == 0 ? (rng() % 20) * interval : rng() % (interval / 3 + 1);
                advanceClock(h, step_ns);
                int which = rng() % 4 == 0 ? 1 : 0;
                int eagerness = int(rng() % 3);
                bool ref_adv = false;
                bool our_adv = false;
                if (verbose)
                    std::fprintf(stderr, "step %d: maybe_decay_purge %d eagerness %d\n", step, which, eagerness);
                Harness::onSide(REF_SIDE, [&] { ref_adv = ref_maybe_decay_purge(h.ref, which, eagerness); });
                Harness::onSide(
                    OUR_SIDE,
                    [&]
                    {
                        Decay & d = which == 0 ? h.our->pac.decay_dirty : h.our->pac.decay_muzzy;
                        DecayStats & s = which == 0 ? h.our_stats->pac_stats.decay_dirty : h.our_stats->pac_stats.decay_muzzy;
                        ExtentCache & c = which == 0 ? h.our->pac.ecache_dirty : h.our->pac.ecache_muzzy;
                        d.mtx.lock(nullptr);
                        our_adv = h.our->pac.maybeDecayPurge(nullptr, &d, &s, &c, PacPurgeEagerness(eagerness));
                        d.mtx.unlock(nullptr);
                    });
                ok = ref_adv == our_adv;
                ++npurges;
            }
            else if (kind < 65)
            {
                int which = rng() % 3 == 0 ? 1 : 0;
                bool fully = rng() % 2 == 0;
                if (verbose)
                    std::fprintf(stderr, "step %d: decay_all %d fully %d\n", step, which, int(fully));
                Harness::onSide(REF_SIDE, [&] { ref_decay_all(h.ref, which, fully); });
                Harness::onSide(
                    OUR_SIDE,
                    [&]
                    {
                        Decay & d = which == 0 ? h.our->pac.decay_dirty : h.our->pac.decay_muzzy;
                        DecayStats & s = which == 0 ? h.our_stats->pac_stats.decay_dirty : h.our_stats->pac_stats.decay_muzzy;
                        ExtentCache & c = which == 0 ? h.our->pac.ecache_dirty : h.our->pac.ecache_muzzy;
                        d.mtx.lock(nullptr);
                        h.our->pac.decayAll(nullptr, &d, &s, &c, fully);
                        d.mtx.unlock(nullptr);
                    });
                ++npurges;
            }
            else if (kind < 80)
            {
                uint64_t r = 0;
                uint64_t o = 0;
                Harness::onSide(REF_SIDE, [&] { r = ref_time_until_deferred_work(h.ref); });
                Harness::onSide(OUR_SIDE, [&] { o = h.our->timeUntilDeferredWork(nullptr); });
                if (r != o)
                {
                    std::fprintf(stderr, "step %d: time until deferred work %llu vs %llu\n", step, (unsigned long long)r, (unsigned long long)o);
                    ok = false;
                }
            }
            else if (kind < 90 && w.vary_decay_ms)
            {
                static const ssize_t values[] = {-1, 0, 1, 100, 5000, 10000};
                int which = rng() % 3 == 0 ? 1 : 0;
                ssize_t ms = values[rng() % (sizeof(values) / sizeof(values[0]))];
                if (rng() % 10 == 0)
                    ms = -2; /// Invalid.
                int eagerness = int(rng() % 3);
                advanceClock(h, rng() % 1000);
                bool r = false;
                bool o = false;
                if (verbose)
                    std::fprintf(stderr, "step %d: decay_ms_set %d %zd eagerness %d\n", step, which, ms, eagerness);
                Harness::onSide(REF_SIDE, [&] { r = ref_decay_ms_set(h.ref, which, ms, eagerness); });
                Harness::onSide(
                    OUR_SIDE,
                    [&] { o = h.our->decayMsSet(nullptr, which == 0 ? extent_state_dirty : extent_state_muzzy, ms, PacPurgeEagerness(eagerness)); });
                ok = r == o;
            }
            else
            {
                /// The retained grow limit.
                size_t ref_old = 0;
                size_t our_old = 0;
                size_t limit = rng() % 2 == 0 ? (size_t(1) << (20 + rng() % 12)) + rng() % 3 * PAGE : SC_LARGE_MAXCLASS + rng() % 2;
                bool set = rng() % 3 == 0;
                bool r = false;
                bool o = false;
                Harness::onSide(REF_SIDE, [&] { r = ref_retain_grow_limit(h.ref, &ref_old, set ? &limit : nullptr); });
                Harness::onSide(OUR_SIDE, [&] { o = h.our->pac.retainGrowLimitGetSet(nullptr, &our_old, set ? &limit : nullptr); });
                ok = r == o && ref_old == our_old;
            }
        }

        ok = ok && h.compareAll(step);
        if (!ok)
        {
            std::fprintf(stderr, "seed %llu: mismatch at step %d\n", static_cast<unsigned long long>(seed), step);
            ++failures;
            CHECK(ok);
        }
    }

    CHECK_GT(nallocs, size_t(steps / 5));
    CHECK_GT(npurges, size_t(0));
    (void)nexpands;

    /// Teardown as in arena destroy: free everything, purge everything to retained, destroy.
    for (auto & l : h.live)
    {
        bool d1 = false;
        bool d2 = false;
        Harness::onSide(REF_SIDE, [&] { ref_dalloc(h.ref, l.ref, &d1); });
        Harness::onSide(OUR_SIDE, [&] { h.our->dalloc(nullptr, l.our, &d2); });
    }
    h.live.clear();
    for (int which = 0; which < 2; ++which)
    {
        Harness::onSide(REF_SIDE, [&] { ref_decay_all(h.ref, which, true); });
        Harness::onSide(
            OUR_SIDE,
            [&]
            {
                Decay & d = which == 0 ? h.our->pac.decay_dirty : h.our->pac.decay_muzzy;
                DecayStats & s = which == 0 ? h.our_stats->pac_stats.decay_dirty : h.our_stats->pac_stats.decay_muzzy;
                ExtentCache & c = which == 0 ? h.our->pac.ecache_dirty : h.our->pac.ecache_muzzy;
                d.mtx.lock(nullptr);
                h.our->pac.decayAll(nullptr, &d, &s, &c, true);
                d.mtx.unlock(nullptr);
            });
    }
    CHECK(h.compareAll(steps));
    Harness::onSide(REF_SIDE, [&] { ref_shard_destroy(h.ref); });
    Harness::onSide(OUR_SIDE, [&] { h.our->destroy(nullptr); });
    CHECK(h.compareAll(steps + 1));
}

}

TEST(PageAllocatorOracle, Layout)
{
    bootOnce();
    CHECK_EQ(ref_sizeof_pa_shard(), sizeof(PaShard));
    CHECK_EQ(ref_sizeof_pac(), sizeof(PageAllocator));
}

/// ClickHouse's settings: dirty_decay_ms 5000, muzzy_decay_ms 0, no background threads (the oversize shortcut of
/// `extentRecord` is taken for extents of at least the threshold).
TEST(PageAllocatorOracle, ClickHouseSettings)
{
    bootOnce();
    for (uint64_t seed = 0; seed < 6; ++seed)
        runSequence(
            seed,
            2500,
            5000,
            0,
            seed % 2 == 0 ? (size_t(64) << 20) : 32 * PAGE,
            Workload{.vary_decay_ms = false, .background_thread = seed % 3 == 2});
}

/// jemalloc's defaults (muzzy decay enabled), with decay settings changed on the fly.
TEST(PageAllocatorOracle, MuzzyAndSettings)
{
    bootOnce();
    for (uint64_t seed = 10; seed < 16; ++seed)
        runSequence(seed, 2500, 10000, 10000, size_t(8) << 20, Workload{});
}

/// Allocation-heavy (deep extent sets, many coalescing opportunities).
TEST(PageAllocatorOracle, AllocHeavy)
{
    bootOnce();
    for (uint64_t seed = 20; seed < 23; ++seed)
        runSequence(seed, 3000, 5000, 0, size_t(64) << 20, Workload{.alloc = 50, .dalloc = 20, .expand = 10, .shrink = 10, .guarded_permille = 0});
}

/// Without large size classes disabled (the batched retained path is off; size-class based fit).
TEST(PageAllocatorOracle, LargeSizeClassesEnabled)
{
    bootOnce();
    opt.disable_large_size_classes = false;
    ref_set_disable_large_size_classes(false);
    for (uint64_t seed = 30; seed < 33; ++seed)
        runSequence(seed, 2000, 5000, 0, size_t(64) << 20, Workload{});
    opt.disable_large_size_classes = true;
    ref_set_disable_large_size_classes(true);
}
