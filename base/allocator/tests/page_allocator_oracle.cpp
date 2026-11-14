/// Compares `PageAllocatorShard` / `PageAllocator` (and everything below: `ExtentOps`, `ExtentCache`, `ExtentSet`, `Sanitizer`
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

RefExtentInfo ourInfo(const Extent * e)
{
    RefExtentInfo info{};
    info.addr = reinterpret_cast<uintptr_t>(e->address);
    info.size = e->size();
    info.serial_number = e->serialNumber();
    info.state = e->state();
    info.size_class_idx = e->sizeClassIdxMaybeInvalid();
    info.zeroed = e->zeroed();
    info.committed = e->committed();
    info.guarded = e->guarded();
    info.slab = e->slab();
    info.is_head = e->isHead();
    info.arena_idx = unsigned((e->packed_bits & extent_bits::arena.mask()) >> extent_bits::arena.shift);
    return info;
}

bool sameInfo(const RefExtentInfo & r, const RefExtentInfo & o, const char * what, int step)
{
    NormalizedAddr ref_address = normalize(REF_SIDE, r.addr);
    NormalizedAddr our_address = normalize(OUR_SIDE, o.addr);
    bool ok = ref_address.ok && our_address.ok && ref_address == our_address && r.size == o.size && r.serial_number == o.serial_number
        && r.state == o.state && r.size_class_idx == o.size_class_idx && r.zeroed == o.zeroed && r.committed == o.committed
        && r.guarded == o.guarded && r.slab == o.slab && r.is_head == o.is_head && r.arena_idx == o.arena_idx;
    if (!ok)
        std::fprintf(
            stderr,
            "step %d: %s: ref (region %zu + %zu, ok %d, size %zu, sn %llu, state %u, szind %u, z%d c%d g%d s%d h%d a%u) vs "
            "our (region %zu + %zu, ok %d, size %zu, sn %llu, state %u, szind %u, z%d c%d g%d s%d h%d a%u)\n",
            step,
            what,
            ref_address.rank,
            ref_address.offset,
            int(ref_address.ok),
            r.size,
            static_cast<unsigned long long>(r.serial_number),
            r.state,
            r.size_class_idx,
            r.zeroed,
            r.committed,
            r.guarded,
            r.slab,
            r.is_head,
            r.arena_idx,
            our_address.rank,
            our_address.offset,
            int(our_address.ok),
            o.size,
            static_cast<unsigned long long>(o.serial_number),
            o.state,
            o.size_class_idx,
            o.zeroed,
            o.committed,
            o.guarded,
            o.slab,
            o.is_head,
            o.arena_idx);
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
    PageAllocatorShard * our = nullptr;
    PageAllocatorShardStats * our_stats = nullptr;
    ExtentMap * our_extent_map = nullptr;
    Base * our_base = nullptr;
    std::vector<Live> live;

    Harness(unsigned idx, ssize_t dirty_ms, ssize_t muzzy_ms, size_t oversize_threshold)
    {
        ref_trace_set_side(REF_SIDE);
        ref = ref_shard_new(idx, dirty_ms, muzzy_ms, oversize_threshold);
        REQUIRE(ref != nullptr);

        ref_trace_set_side(OUR_SIDE);
        our_base = Base::create(nullptr, idx, &extent_hooks_default_extent_hooks, true);
        REQUIRE(our_base != nullptr);
        void * extent_map_memory = std::aligned_alloc(64, (sizeof(ExtentMap) + 63) / 64 * 64);
        std::memset(extent_map_memory, 0, sizeof(ExtentMap));
        our_extent_map = new (extent_map_memory) ExtentMap();
        REQUIRE(!our_extent_map->init(our_base, /* zeroed */ true));
        our_stats = new PageAllocatorShardStats();
        void * shard_memory = std::aligned_alloc(64, (sizeof(PageAllocatorShard) + 63) / 64 * 64);
        std::memset(shard_memory, 0, sizeof(PageAllocatorShard));
        our = new (shard_memory) PageAllocatorShard();
        Nanoseconds current;
        current.initUpdate();
        REQUIRE(!our->init(nullptr, our_extent_map, our_base, idx, our_stats, nullptr, current, oversize_threshold, dirty_ms, muzzy_ms));
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
        static RefExtentInfo ref_list[20000];
        bool ok = true;
        for (int which = 0; which < 3; ++which)
        {
            ExtentCache * extent_cache = which == 0
                ? &our->page_allocator.extent_cache_dirty
                : (which == 1 ? &our->page_allocator.extent_cache_muzzy : &our->page_allocator.extent_cache_retained);
            for (int guarded = 0; guarded < 2; ++guarded)
            {
                size_t n = ref_extent_cache_list(ref, which, guarded, ref_list, 20000);
                ExtentSet & extent_set = guarded ? extent_cache->guarded_extent_set : extent_cache->extent_set;
                size_t k = 0;
                for (Extent * e = extent_set.lru.first(); e != nullptr; e = extent_set.lru.next(e), ++k)
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
        size_t num_active = 0;
        size_t num_dirty = 0;
        size_t num_muzzy = 0;
        our->basicStatsMerge(&num_active, &num_dirty, &num_muzzy);

        static PageAllocatorShardStats stats;
        static PageAllocatorExtentStats extent_stats[SIZE_CLASS_NUM_PAGE_SIZES];
        stats.~PageAllocatorShardStats();
        new (&stats) PageAllocatorShardStats();
        std::memset(extent_stats, 0, sizeof(extent_stats));
        size_t resident = 0;
        our->statsMerge(nullptr, &stats, extent_stats, &resident);

        size_t i = 0;
        out[i++] = num_active;
        out[i++] = num_dirty;
        out[i++] = num_muzzy;
        out[i++] = resident;
        out[i++] = stats.extent_available;
        out[i++] = stats.page_allocator_stats.retained;
        out[i++] = stats.page_allocator_stats.page_allocator_mapped.load();
        out[i++] = stats.page_allocator_stats.abandoned_vm.load();
        out[i++] = our->page_allocator.mapped();
        out[i++] = stats.page_allocator_stats.decay_dirty.num_purge.read();
        out[i++] = stats.page_allocator_stats.decay_dirty.num_madvises.read();
        out[i++] = stats.page_allocator_stats.decay_dirty.purged.read();
        out[i++] = stats.page_allocator_stats.decay_muzzy.num_purge.read();
        out[i++] = stats.page_allocator_stats.decay_muzzy.num_madvises.read();
        out[i++] = stats.page_allocator_stats.decay_muzzy.purged.read();
        out[i++] = our->page_allocator.extent_serial_number_next.load();
        out[i++] = our->page_allocator.exponential_grow.next;
        out[i++] = our->page_allocator.exponential_grow.limit;
        out[i++] = our->extent_pool.count();
        for (unsigned j = 0; j < SIZE_CLASS_NUM_PAGE_SIZES; ++j)
        {
            out[i++] = extent_stats[j].num_dirty;
            out[i++] = extent_stats[j].dirty_bytes;
            out[i++] = extent_stats[j].num_muzzy;
            out[i++] = extent_stats[j].muzzy_bytes;
            out[i++] = extent_stats[j].num_retained;
            out[i++] = extent_stats[j].retained_bytes;
        }
    }

    bool compareStats(int step)
    {
        static uint64_t ref_out[4096];
        static uint64_t our_out[4096];
        size_t n = ref_num_stats();
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
            size_t num_pages_limit;
            size_t num_unpurged;
            size_t backlog_last;
            ref_decay_state(ref, which, &epoch, &num_pages_limit, &num_unpurged, &backlog_last);
            Decay & d = which == 0 ? our->page_allocator.decay_dirty : our->page_allocator.decay_muzzy;
            if (epoch != d.epoch.ns() || num_pages_limit != d.num_pages_limit || num_unpurged != d.num_unpurged
                || backlog_last != d.backlog[SMOOTHSTEP_NUM_STEPS - 1] || ref_decay_ms_get(ref, which) != d.msRead())
            {
                std::fprintf(stderr, "step %d: decay %d state mismatch\n", step, which);
                ok = false;
            }
        }
        if (ref_num_regions_of(REF_SIDE) != ref_num_regions_of(OUR_SIDE))
        {
            std::fprintf(stderr, "step %d: %zu regions vs %zu\n", step, ref_num_regions_of(REF_SIDE), ref_num_regions_of(OUR_SIDE));
            ok = false;
        }
        return ok;
    }

    /// The live extents, and their emap entries.
    bool compareLive(int step)
    {
        for (size_t i = 0; i < live.size(); ++i)
        {
            RefExtentInfo r;
            ref_extent_info(live[i].ref, &r);
            char what[64];
            std::snprintf(what, sizeof(what), "live #%zu", i);
            if (!sameInfo(r, ourInfo(live[i].our), what, step))
                return false;
            /// The boundary pages map to the extent with its szind and slab.
            unsigned ref_size_class_idx = 0;
            bool ref_slab = false;
            void * ref_e = ref_extent_map_lookup(ref, reinterpret_cast<void *>(r.addr), &ref_size_class_idx, &ref_slab);
            FullAllocContext context{};
            bool our_missing = our_extent_map->fullAllocContextTryLookup(nullptr, live[i].our->address, &context);
            if (ref_e != live[i].ref || our_missing || context.extent != live[i].our || context.size_class_idx != ref_size_class_idx
                || context.slab != ref_slab)
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
        for (Decay * d : {&h.our->page_allocator.decay_dirty, &h.our->page_allocator.decay_muzzy})
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
    unsigned deallocate = 30;
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
    size_t num_allocations = 0;
    size_t num_expands = 0;
    size_t num_purges = 0;

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
            SizeClassIdx size_class_idx = slab ? SizeClassIdx(rng() % SIZE_CLASS_NUM_BINS) : SIZE_CLASS_NUM_BINS + SizeClassIdx(rng() % 20);
            bool zero = rng() % 5 == 0;
            bool guarded = (rng() % 1000) < w.guarded_permille;
            /// Large allocations are at least `SIZE_CLASS_LARGE_MIN_CLASS` (`pac_dalloc_impl` relies on it for guarded ones).
            if (guarded && !slab && size < SIZE_CLASS_LARGE_MIN_CLASS)
                size = SIZE_CLASS_LARGE_MIN_CLASS + PAGE * (rng() % 4);
            bool ref_deferred = false;
            bool our_deferred = false;
            void * r = nullptr;
            Extent * o = nullptr;
            if (verbose)
                std::fprintf(
                    stderr, "step %d: alloc %zu pages slab %d zero %d guarded %d\n", step, pages, int(slab), int(zero), int(guarded));
            Harness::onSide(REF_SIDE, [&] { r = ref_alloc(h.ref, size, PAGE, slab, size_class_idx, zero, guarded, &ref_deferred); });
            Harness::onSide(OUR_SIDE, [&] { o = h.our->alloc(nullptr, size, PAGE, slab, size_class_idx, zero, guarded, &our_deferred); });
            if ((r == nullptr) != (o == nullptr) || ref_deferred != our_deferred)
            {
                std::fprintf(stderr, "step %d: alloc result mismatch\n", step);
                ok = false;
            }
            else if (r != nullptr)
            {
                RefExtentInfo ref_idx;
                ref_extent_info(r, &ref_idx);
                ok = sameInfo(ref_idx, ourInfo(o), "alloc", step);
                if (zero && ok)
                {
                    const unsigned char * p = static_cast<const unsigned char *>(o->address);
                    ok = p[0] == 0 && p[size - 1] == 0;
                }
                h.live.push_back({r, o, size, slab, guarded});
                ++num_allocations;
                /// Dirty the memory, so that purging and zeroing matter.
                std::memset(static_cast<char *>(o->address), 0x11, 64);
                std::memset(reinterpret_cast<char *>(ref_idx.addr), 0x11, 64);
            }
        }
        else if (op < w.alloc + w.deallocate)
        {
            size_t i = rng() % h.live.size();
            bool ref_deferred = false;
            bool our_deferred = false;
            if (verbose)
                std::fprintf(stderr, "step %d: dalloc %zu pages\n", step, h.live[i].size / PAGE);
            Harness::onSide(REF_SIDE, [&] { ref_deallocate(h.ref, h.live[i].ref, &ref_deferred); });
            Harness::onSide(OUR_SIDE, [&] { h.our->deallocate(nullptr, h.live[i].our, &our_deferred); });
            ok = ref_deferred == our_deferred;
            h.live.erase(h.live.begin() + long(i));
        }
        else if (op < w.alloc + w.deallocate + w.expand)
        {
            Live & l = h.live[rng() % h.live.size()];
            if (!l.slab)
            {
                size_t new_size = l.size + (1 + rng() % 16) * PAGE;
                SizeClassIdx size_class_idx = SIZE_CLASS_NUM_BINS + SizeClassIdx(rng() % 20);
                bool zero = rng() % 3 == 0;
                bool ref_deferred = false;
                bool our_deferred = false;
                bool ref_error = false;
                bool our_error = false;
                if (verbose)
                    std::fprintf(stderr, "step %d: expand %zu -> %zu pages\n", step, l.size / PAGE, new_size / PAGE);
                Harness::onSide(
                    REF_SIDE, [&] { ref_error = ref_expand(h.ref, l.ref, l.size, new_size, size_class_idx, zero, &ref_deferred); });
                Harness::onSide(
                    OUR_SIDE, [&] { our_error = h.our->expand(nullptr, l.our, l.size, new_size, size_class_idx, zero, &our_deferred); });
                if (ref_error != our_error || ref_deferred != our_deferred)
                {
                    std::fprintf(stderr, "step %d: expand result mismatch (%d vs %d)\n", step, int(ref_error), int(our_error));
                    ok = false;
                }
                else if (!ref_error)
                {
                    l.size = new_size;
                    ++num_expands;
                }
            }
        }
        else if (op < w.alloc + w.deallocate + w.expand + w.shrink)
        {
            Live & l = h.live[rng() % h.live.size()];
            if (!l.slab && l.size > PAGE)
            {
                size_t new_size = l.size - (1 + rng() % (l.size / PAGE - 1)) * PAGE;
                SizeClassIdx size_class_idx = SIZE_CLASS_NUM_BINS + SizeClassIdx(rng() % 20);
                bool ref_deferred = false;
                bool our_deferred = false;
                bool ref_error = false;
                bool our_error = false;
                if (verbose)
                    std::fprintf(stderr, "step %d: shrink %zu -> %zu pages\n", step, l.size / PAGE, new_size / PAGE);
                Harness::onSide(REF_SIDE, [&] { ref_error = ref_shrink(h.ref, l.ref, l.size, new_size, size_class_idx, &ref_deferred); });
                Harness::onSide(
                    OUR_SIDE, [&] { our_error = h.our->shrink(nullptr, l.our, l.size, new_size, size_class_idx, &our_deferred); });
                if (ref_error != our_error || ref_deferred != our_deferred)
                {
                    std::fprintf(stderr, "step %d: shrink result mismatch\n", step);
                    ok = false;
                }
                else if (!ref_error)
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
                uint64_t interval = h.our->page_allocator.decay_dirty.interval.ns();
                uint64_t step_ns = rng() % 3 == 0 ? (rng() % 20) * interval : rng() % (interval / 3 + 1);
                advanceClock(h, step_ns);
                int which = rng() % 4 == 0 ? 1 : 0;
                int eagerness = int(rng() % 3);
                bool ref_advance = false;
                bool our_advance = false;
                if (verbose)
                    std::fprintf(stderr, "step %d: maybe_decay_purge %d eagerness %d\n", step, which, eagerness);
                Harness::onSide(REF_SIDE, [&] { ref_advance = ref_maybe_decay_purge(h.ref, which, eagerness); });
                Harness::onSide(
                    OUR_SIDE,
                    [&]
                    {
                        Decay & d = which == 0 ? h.our->page_allocator.decay_dirty : h.our->page_allocator.decay_muzzy;
                        DecayStats & s
                            = which == 0 ? h.our_stats->page_allocator_stats.decay_dirty : h.our_stats->page_allocator_stats.decay_muzzy;
                        ExtentCache & c = which == 0 ? h.our->page_allocator.extent_cache_dirty : h.our->page_allocator.extent_cache_muzzy;
                        d.mutex.lock(nullptr);
                        our_advance = h.our->page_allocator.maybeDecayPurge(nullptr, &d, &s, &c, PageAllocatorPurgeEagerness(eagerness));
                        d.mutex.unlock(nullptr);
                    });
                ok = ref_advance == our_advance;
                ++num_purges;
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
                        Decay & d = which == 0 ? h.our->page_allocator.decay_dirty : h.our->page_allocator.decay_muzzy;
                        DecayStats & s
                            = which == 0 ? h.our_stats->page_allocator_stats.decay_dirty : h.our_stats->page_allocator_stats.decay_muzzy;
                        ExtentCache & c = which == 0 ? h.our->page_allocator.extent_cache_dirty : h.our->page_allocator.extent_cache_muzzy;
                        d.mutex.lock(nullptr);
                        h.our->page_allocator.decayAll(nullptr, &d, &s, &c, fully);
                        d.mutex.unlock(nullptr);
                    });
                ++num_purges;
            }
            else if (kind < 80)
            {
                uint64_t r = 0;
                uint64_t o = 0;
                Harness::onSide(REF_SIDE, [&] { r = ref_time_until_deferred_work(h.ref); });
                Harness::onSide(OUR_SIDE, [&] { o = h.our->timeUntilDeferredWork(nullptr); });
                if (r != o)
                {
                    std::fprintf(
                        stderr, "step %d: time until deferred work %llu vs %llu\n", step, (unsigned long long)r, (unsigned long long)o);
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
                    [&]
                    {
                        o = h.our->decayMsSet(
                            nullptr, which == 0 ? extent_state_dirty : extent_state_muzzy, ms, PageAllocatorPurgeEagerness(eagerness));
                    });
                ok = r == o;
            }
            else
            {
                /// The retained grow limit.
                size_t ref_old = 0;
                size_t our_old = 0;
                size_t limit
                    = rng() % 2 == 0 ? (size_t(1) << (20 + rng() % 12)) + rng() % 3 * PAGE : SIZE_CLASS_LARGE_MAX_CLASS + rng() % 2;
                bool set = rng() % 3 == 0;
                bool r = false;
                bool o = false;
                Harness::onSide(REF_SIDE, [&] { r = ref_retain_grow_limit(h.ref, &ref_old, set ? &limit : nullptr); });
                Harness::onSide(
                    OUR_SIDE, [&] { o = h.our->page_allocator.retainGrowLimitGetSet(nullptr, &our_old, set ? &limit : nullptr); });
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

    CHECK_GT(num_allocations, size_t(steps / 5));
    CHECK_GT(num_purges, size_t(0));
    (void)num_expands;

    /// Teardown as in arena destroy: free everything, purge everything to retained, destroy.
    for (auto & l : h.live)
    {
        bool d1 = false;
        bool d2 = false;
        Harness::onSide(REF_SIDE, [&] { ref_deallocate(h.ref, l.ref, &d1); });
        Harness::onSide(OUR_SIDE, [&] { h.our->deallocate(nullptr, l.our, &d2); });
    }
    h.live.clear();
    for (int which = 0; which < 2; ++which)
    {
        Harness::onSide(REF_SIDE, [&] { ref_decay_all(h.ref, which, true); });
        Harness::onSide(
            OUR_SIDE,
            [&]
            {
                Decay & d = which == 0 ? h.our->page_allocator.decay_dirty : h.our->page_allocator.decay_muzzy;
                DecayStats & s = which == 0 ? h.our_stats->page_allocator_stats.decay_dirty : h.our_stats->page_allocator_stats.decay_muzzy;
                ExtentCache & c = which == 0 ? h.our->page_allocator.extent_cache_dirty : h.our->page_allocator.extent_cache_muzzy;
                d.mutex.lock(nullptr);
                h.our->page_allocator.decayAll(nullptr, &d, &s, &c, true);
                d.mutex.unlock(nullptr);
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
    CHECK_EQ(ref_sizeof_page_allocator_shard(), sizeof(PageAllocatorShard));
    CHECK_EQ(ref_sizeof_page_allocator(), sizeof(PageAllocator));
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
        runSequence(
            seed,
            3000,
            5000,
            0,
            size_t(64) << 20,
            Workload{.alloc = 50, .deallocate = 20, .expand = 10, .shrink = 10, .guarded_permille = 0});
}

/// Without large size classes disabled (the batched retained path is off; size-class based fit).
TEST(PageAllocatorOracle, LargeSizeClassesEnabled)
{
    bootOnce();
    options.disable_large_size_classes = false;
    ref_set_disable_large_size_classes(false);
    for (uint64_t seed = 30; seed < 33; ++seed)
        runSequence(seed, 2000, 5000, 0, size_t(64) << 20, Workload{});
    options.disable_large_size_classes = true;
    ref_set_disable_large_size_classes(true);
}
