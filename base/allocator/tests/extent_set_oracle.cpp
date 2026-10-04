/// Compares `ExtentSet` with jemalloc's `eset.c` (linked from the reference `lib_jemalloc.a`): both sets receive
/// identical randomized sequences of insert / remove / fit operations on extents with identical (fake) addresses,
/// sizes and serial numbers, and must return the same extent from every `fit` (various sizes, alignments, `exact_only`
/// and `lg_max_fit` values, with large size classes disabled and enabled). After every operation the stats, the
/// bitmap, the cached heap minimums, the heap roots and the LRU order are compared too.

#include <allocator/ExtentSet.h>
#include <allocator/Options.h>
#include <allocator/SizeClasses.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>

extern "C"
{
void ref_boot(void);
void ref_set_disable_large_size_classes(bool value);
size_t ref_sizeof_eset(void);
void * ref_eset_new(unsigned state);
void ref_eset_delete(void * eset);
void * ref_edata_new(void * addr, size_t size, uint64_t sn, unsigned state);
void ref_edata_delete(void * edata);
void ref_edata_set_state(void * edata, unsigned state);
void ref_eset_insert(void * eset, void * edata);
void ref_eset_remove(void * eset, void * edata);
void * ref_eset_fit(void * eset, size_t esize, size_t alignment, bool exact_only, unsigned lg_max_fit);
size_t ref_eset_npages(void * eset);
size_t ref_eset_nextents(void * eset, unsigned pind);
size_t ref_eset_nbytes(void * eset, unsigned pind);
unsigned ref_eset_npsizes(void);
void ref_eset_heap_min(void * eset, unsigned pind, uint64_t * sn, uintptr_t * addr);
bool ref_eset_bin_empty(void * eset, unsigned pind);
size_t ref_eset_bitmap(void * eset, unsigned long * out, size_t max);
size_t ref_eset_lru(void * eset, void ** out, size_t max);
void * ref_eset_heap_root(void * eset, unsigned pind, size_t * auxcount);

/// `eset.o` pulls in the rest of the reference jemalloc, including the libunwind-based profiler backtrace, which is
/// never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

using namespace jemalloc;

namespace
{

void bootOnce()
{
    static bool booted = false;
    if (!booted)
    {
        ref_boot();
        booted = true;
    }
}

/// One extent known to both sides.
struct Pair
{
    void * ref;
    Extent * our;
    bool present;
};

Extent * newExtent(void * addr, size_t size, uint64_t sn, ExtentState state)
{
    void * p = std::aligned_alloc(EDATA_ALIGNMENT, (sizeof(Extent) + EDATA_ALIGNMENT - 1) / EDATA_ALIGNMENT * EDATA_ALIGNMENT);
    std::memset(p, 0, sizeof(Extent));
    Extent * e = static_cast<Extent *>(p);
    e->init(0, addr, size, false, SC_NSIZES, sn, state, false, true, EXTENT_PAI_PAC, EXTENT_NOT_HEAD);
    return e;
}

struct Harness
{
    void * ref_set = nullptr;
    ExtentSet * our_set = nullptr;
    std::vector<Pair> pairs;

    Harness()
    {
        ref_set = ref_eset_new(extent_state_dirty);
        our_set = static_cast<ExtentSet *>(std::aligned_alloc(64, (sizeof(ExtentSet) + 63) / 64 * 64));
        std::memset(static_cast<void *>(our_set), 0, sizeof(ExtentSet));
        our_set->init(extent_state_dirty);
    }

    ~Harness()
    {
        for (auto & pair : pairs)
        {
            ref_edata_delete(pair.ref);
            std::free(pair.our);
        }
        ref_eset_delete(ref_set);
        std::free(our_set);
    }

    int indexOfRef(void * ref) const
    {
        for (size_t i = 0; i < pairs.size(); ++i)
            if (pairs[i].ref == ref)
                return int(i);
        return -1;
    }

    int indexOfOur(const Extent * our) const
    {
        for (size_t i = 0; i < pairs.size(); ++i)
            if (pairs[i].our == our)
                return int(i);
        return -1;
    }

    void insert(size_t i)
    {
        ref_edata_set_state(pairs[i].ref, extent_state_dirty);
        pairs[i].our->setState(extent_state_dirty);
        ref_eset_insert(ref_set, pairs[i].ref);
        our_set->insert(pairs[i].our);
        pairs[i].present = true;
    }

    /// Removes in the `merging` state sometimes, as `extentCoalesce` / `extentActivateLocked` do.
    void remove(size_t i, bool merging)
    {
        if (merging)
        {
            ref_edata_set_state(pairs[i].ref, extent_state_merging);
            pairs[i].our->setState(extent_state_merging);
        }
        ref_eset_remove(ref_set, pairs[i].ref);
        our_set->remove(pairs[i].our);
        pairs[i].present = false;
    }

    /// Returns false on mismatch.
    bool compareState(int step) const
    {
        bool ok = true;
        if (ref_eset_npages(ref_set) != our_set->npagesGet())
        {
            std::fprintf(stderr, "step %d: npages %zu vs %zu\n", step, ref_eset_npages(ref_set), our_set->npagesGet());
            ok = false;
        }
        for (unsigned pind = 0; pind < ESET_NPSIZES; ++pind)
        {
            if (ref_eset_nextents(ref_set, pind) != our_set->nextentsGet(pind) || ref_eset_nbytes(ref_set, pind) != our_set->nbytesGet(pind))
            {
                std::fprintf(stderr, "step %d: bin %u stats mismatch\n", step, pind);
                ok = false;
            }
            bool ref_empty = ref_eset_bin_empty(ref_set, pind);
            if (ref_empty != our_set->bins[pind].heap.empty())
            {
                std::fprintf(stderr, "step %d: bin %u emptiness mismatch\n", step, pind);
                ok = false;
                continue;
            }
            if (ref_empty)
                continue;
            uint64_t sn;
            uintptr_t addr;
            ref_eset_heap_min(ref_set, pind, &sn, &addr);
            if (sn != our_set->bins[pind].heap_min.sn || addr != our_set->bins[pind].heap_min.addr)
            {
                std::fprintf(stderr, "step %d: bin %u heap_min mismatch\n", step, pind);
                ok = false;
            }
            size_t ref_auxcount;
            void * ref_root = ref_eset_heap_root(ref_set, pind, &ref_auxcount);
            if (indexOfRef(ref_root) != indexOfOur(our_set->bins[pind].heap.rootNode()) || ref_auxcount != our_set->bins[pind].heap.auxCount())
            {
                std::fprintf(stderr, "step %d: bin %u heap root/auxcount mismatch\n", step, pind);
                ok = false;
            }
        }
        unsigned long words[16];
        size_t nwords = ref_eset_bitmap(ref_set, words, 16);
        if (nwords != our_set->bitmap.ngroups)
        {
            std::fprintf(stderr, "bitmap size %zu vs %zu\n", nwords, our_set->bitmap.ngroups);
            ok = false;
        }
        else
        {
            for (size_t i = 0; i < nwords; ++i)
                if (words[i] != our_set->bitmap.groups[i])
                {
                    std::fprintf(stderr, "step %d: bitmap word %zu mismatch\n", step, i);
                    ok = false;
                }
        }
        static void * lru[100000];
        size_t n = ref_eset_lru(ref_set, lru, 100000);
        size_t k = 0;
        for (Extent * e = our_set->lru.first(); e != nullptr; e = our_set->lru.next(e), ++k)
        {
            if (k >= n || indexOfRef(lru[k]) != indexOfOur(e))
            {
                std::fprintf(stderr, "step %d: LRU mismatch at %zu\n", step, k);
                ok = false;
                break;
            }
        }
        if (ok && k != n)
        {
            std::fprintf(stderr, "step %d: LRU length %zu vs %zu\n", step, n, k);
            ok = false;
        }
        return ok;
    }
};

/// A page multiple from a distribution that hits all regions of the page size classes (and many sizes that are not
/// size classes, so that the enumerate search paths are exercised).
size_t pickPages(std::mt19937_64 & rng)
{
    unsigned kind = rng() % 100;
    if (kind < 35)
        return 1 + rng() % 8;
    if (kind < 65)
        return 1 + rng() % 64;
    if (kind < 85)
        return 1 + rng() % 1024;
    if (kind < 97)
        return 1 + rng() % (size_t(1) << 16);
    return (size_t(1) << 16) + rng() % (size_t(1) << 20);
}

size_t pickSize(std::mt19937_64 & rng)
{
    /// Rarely, an extent larger than SC_LARGE_MAXCLASS (the last bin).
    if (rng() % 500 == 0)
        return SC_LARGE_MAXCLASS + PAGE * (1 + rng() % 4);
    return pickPages(rng) * PAGE;
}

size_t pickAlignment(std::mt19937_64 & rng)
{
    unsigned kind = rng() % 100;
    if (kind < 60)
        return PAGE;
    if (kind < 70)
        return 1 + rng() % PAGE; /// Rounded up to PAGE by `fit`.
    return PAGE << (1 + rng() % 12);
}

unsigned pickLgMaxFit(std::mt19937_64 & rng)
{
    unsigned kind = rng() % 10;
    if (kind < 4)
        return 6;
    if (kind < 7)
        return SC_PTR_BITS;
    return unsigned(rng() % (SC_PTR_BITS + 1));
}

void runSequence(uint64_t seed, int steps, bool disable_large_size_classes, unsigned sn_range)
{
    ref_set_disable_large_size_classes(disable_large_size_classes);
    opt.disable_large_size_classes = disable_large_size_classes;

    Harness h;
    std::mt19937_64 rng(seed);
    int failures = 0;
    size_t nfits = 0;
    size_t nfound = 0;

    for (int step = 0; step < steps && failures < 5; ++step)
    {
        unsigned op = rng() % 100;
        bool ok = true;
        size_t npresent = 0;
        for (auto & pair : h.pairs)
            npresent += pair.present;

        if (op < 40 || npresent < 4)
        {
            /// Insert a new extent, or re-insert a removed one.
            size_t index;
            std::vector<size_t> absent;
            for (size_t i = 0; i < h.pairs.size(); ++i)
                if (!h.pairs[i].present)
                    absent.push_back(i);
            if (!absent.empty() && rng() % 3 == 0)
            {
                index = absent[rng() % absent.size()];
            }
            else
            {
                /// Addresses: random page offsets within distinct 2^44-byte slots; some extents are aligned to large
                /// powers of two (fake addresses: never dereferenced).
                uintptr_t slot = (uintptr_t(1) << 46) + (uintptr_t(h.pairs.size() % 1024) << 44);
                uintptr_t offset = (rng() % (uint64_t(1) << 20)) * PAGE;
                if (rng() % 4 == 0)
                    offset &= ~((uintptr_t(PAGE) << (rng() % 14)) - 1);
                void * addr = reinterpret_cast<void *>(slot + offset);
                size_t size = pickSize(rng);
                if (size > SC_LARGE_MAXCLASS)
                    addr = reinterpret_cast<void *>(uintptr_t(PAGE) * (1 + rng() % 1024));
                uint64_t sn = rng() % sn_range;
                h.pairs.push_back({ref_edata_new(addr, size, sn, extent_state_dirty), newExtent(addr, size, sn, extent_state_dirty), false});
                index = h.pairs.size() - 1;
            }
            h.insert(index);
        }
        else if (op < 55)
        {
            /// Remove a random present extent.
            std::vector<size_t> present;
            for (size_t i = 0; i < h.pairs.size(); ++i)
                if (h.pairs[i].present)
                    present.push_back(i);
            h.remove(present[rng() % present.size()], rng() % 4 == 0);
        }
        else
        {
            /// Fit; usually the request is related to an existing extent's size.
            size_t esize;
            if (rng() % 2 == 0 && !h.pairs.empty())
            {
                const Pair & pair = h.pairs[rng() % h.pairs.size()];
                size_t base_size = pair.our->size();
                if (base_size > SC_LARGE_MAXCLASS)
                    base_size = PAGE;
                long delta = long(rng() % 5) - 2;
                esize = base_size + size_t(delta) * PAGE;
                if (esize == 0 || esize > SC_LARGE_MAXCLASS)
                    esize = base_size;
            }
            else
            {
                esize = pickPages(rng) * PAGE;
            }
            size_t alignment = pickAlignment(rng);
            bool exact_only = rng() % 5 == 0;
            unsigned lg_max_fit = pickLgMaxFit(rng);

            void * r = ref_eset_fit(h.ref_set, esize, alignment, exact_only, lg_max_fit);
            Extent * o = h.our_set->fit(esize, alignment, exact_only, lg_max_fit);
            ++nfits;
            int ri = r ? h.indexOfRef(r) : -1;
            int oi = o ? h.indexOfOur(o) : -1;
            if (ri != oi)
            {
                std::fprintf(
                    stderr,
                    "seed %llu step %d: fit(esize=%zu, alignment=%zu, exact_only=%d, lg_max_fit=%u): %d vs %d\n",
                    static_cast<unsigned long long>(seed),
                    step,
                    esize,
                    alignment,
                    int(exact_only),
                    lg_max_fit,
                    ri,
                    oi);
                ok = false;
            }
            else if (ri >= 0)
            {
                ++nfound;
                /// As `extentActivateLocked` does, usually.
                if (rng() % 3 != 0)
                    h.remove(size_t(ri), false);
            }
        }

        ok &= h.compareState(step);
        if (!ok)
        {
            ++failures;
            CHECK(ok);
        }
    }
    /// Sanity check that the workload is meaningful.
    CHECK_GT(nfits, size_t(steps / 5));
    CHECK_GT(nfound, size_t(0));
    CHECK_LT(nfound, nfits);
}

}

TEST(ExtentSetOracle, Layout)
{
    bootOnce();
    CHECK_EQ(ref_sizeof_eset(), sizeof(ExtentSet));
    CHECK_EQ(ref_eset_npsizes(), ESET_NPSIZES);
}

TEST(ExtentSetOracle, RandomizedLargeSizeClassesDisabled)
{
    bootOnce();
    for (uint64_t seed = 0; seed < 12; ++seed)
        runSequence(seed, 4000, /* disable_large_size_classes */ true, seed % 3 == 0 ? 4 : 1000000);
}

TEST(ExtentSetOracle, RandomizedLargeSizeClassesEnabled)
{
    bootOnce();
    for (uint64_t seed = 100; seed < 108; ++seed)
        runSequence(seed, 4000, /* disable_large_size_classes */ false, seed % 2 == 0 ? 3 : 1000000);
    opt.disable_large_size_classes = true;
    ref_set_disable_large_size_classes(true);
}

/// Many extents in the same few bins (deep heaps, so that the 32-node limit of the enumeration matters).
TEST(ExtentSetOracle, DeepHeaps)
{
    bootOnce();
    ref_set_disable_large_size_classes(true);
    opt.disable_large_size_classes = true;

    Harness h;
    std::mt19937_64 rng(42);
    int failures = 0;
    /// Sizes in [17, 20] pages fall into the bin of 16 pages + pad.. a few bins only.
    for (int i = 0; i < 300; ++i)
    {
        size_t size = (16 + rng() % 8) * PAGE;
        void * addr = reinterpret_cast<void *>((uintptr_t(1) << 40) + uintptr_t(i) * (uintptr_t(1) << 30) + (rng() % 1024) * PAGE);
        uint64_t sn = rng() % 50;
        h.pairs.push_back({ref_edata_new(addr, size, sn, extent_state_dirty), newExtent(addr, size, sn, extent_state_dirty), false});
        h.insert(h.pairs.size() - 1);
    }
    REQUIRE(h.compareState(-1));
    for (int step = 0; step < 2000 && failures < 5; ++step)
    {
        size_t esize = (14 + rng() % 12) * PAGE;
        size_t alignment = rng() % 4 == 0 ? (PAGE << (1 + rng() % 4)) : PAGE;
        bool exact_only = rng() % 4 == 0;
        unsigned lg_max_fit = rng() % 2 ? 6 : SC_PTR_BITS;
        void * r = ref_eset_fit(h.ref_set, esize, alignment, exact_only, lg_max_fit);
        Extent * o = h.our_set->fit(esize, alignment, exact_only, lg_max_fit);
        int ri = r ? h.indexOfRef(r) : -1;
        int oi = o ? h.indexOfOur(o) : -1;
        bool ok = ri == oi;
        if (ok && ri >= 0)
        {
            h.remove(size_t(ri), rng() % 2 == 0);
            /// Re-insert another removed extent to keep the heaps deep.
            for (size_t k = 0; k < h.pairs.size(); ++k)
            {
                size_t j = (size_t(ri) + 1 + k) % h.pairs.size();
                if (!h.pairs[j].present)
                {
                    h.insert(j);
                    break;
                }
            }
        }
        ok &= h.compareState(step);
        if (!ok)
        {
            ++failures;
            CHECK(ok);
        }
    }
}
