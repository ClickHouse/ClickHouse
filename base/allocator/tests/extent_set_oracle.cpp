/// Compares `ExtentSet` with jemalloc's `eset.c` (linked from the reference `lib_jemalloc.a`): both sets receive
/// identical randomized sequences of insert / remove / fit operations on extents with identical (fake) addresses,
/// sizes and serial numbers, and must return the same extent from every `fit` (various sizes, alignments, `exact_only`
/// and `log2_max_fit` values, with large size classes disabled and enabled). After every operation the stats, the
/// bitmap, the cached heap minimums, the heap roots and the LRU order are compared too.

#include <allocator/ExtentSet.h>
#include <allocator/Options.h>
#include <allocator/SizeClasses.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>

extern "C" {
void ref_boot(void);
void ref_set_disable_large_size_classes(bool value);
size_t ref_sizeof_extent_set(void);
void * ref_extent_set_new(unsigned state);
void ref_extent_set_delete(void * extent_set);
void * ref_extent_new(void * addr, size_t size, uint64_t serial_number, unsigned state);
void ref_extent_delete(void * extent);
void ref_extent_set_state(void * extent, unsigned state);
void ref_extent_set_insert(void * extent_set, void * extent);
void ref_extent_set_remove(void * extent_set, void * extent);
void * ref_extent_set_fit(void * extent_set, size_t extent_size, size_t alignment, bool exact_only, unsigned log2_max_fit);
size_t ref_extent_set_num_pages(void * extent_set);
size_t ref_extent_set_num_extents(void * extent_set, unsigned page_size_class_idx);
size_t ref_extent_set_num_bytes(void * extent_set, unsigned page_size_class_idx);
unsigned ref_extent_set_num_page_sizes(void);
void ref_extent_set_heap_min(void * extent_set, unsigned page_size_class_idx, uint64_t * serial_number, uintptr_t * addr);
bool ref_extent_set_bin_empty(void * extent_set, unsigned page_size_class_idx);
size_t ref_extent_set_bitmap(void * extent_set, unsigned long * out, size_t max);
size_t ref_extent_set_lru(void * extent_set, void ** out, size_t max);
void * ref_extent_set_heap_root(void * extent_set, unsigned page_size_class_idx, size_t * auxiliary_count);

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

Extent * newExtent(void * addr, size_t size, uint64_t serial_number, ExtentState state)
{
    void * p = std::aligned_alloc(EXTENT_ALIGNMENT, (sizeof(Extent) + EXTENT_ALIGNMENT - 1) / EXTENT_ALIGNMENT * EXTENT_ALIGNMENT);
    std::memset(p, 0, sizeof(Extent));
    Extent * e = static_cast<Extent *>(p);
    e->init(
        0, addr, size, false, SIZE_CLASS_NUM_SIZES, serial_number, state, false, true, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, EXTENT_NOT_HEAD);
    return e;
}

struct Harness
{
    void * ref_set = nullptr;
    ExtentSet * our_set = nullptr;
    std::vector<Pair> pairs;

    Harness()
    {
        ref_set = ref_extent_set_new(extent_state_dirty);
        our_set = static_cast<ExtentSet *>(std::aligned_alloc(64, (sizeof(ExtentSet) + 63) / 64 * 64));
        std::memset(static_cast<void *>(our_set), 0, sizeof(ExtentSet));
        our_set->init(extent_state_dirty);
    }

    ~Harness()
    {
        for (auto & pair : pairs)
        {
            ref_extent_delete(pair.ref);
            std::free(pair.our);
        }
        ref_extent_set_delete(ref_set);
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
        ref_extent_set_state(pairs[i].ref, extent_state_dirty);
        pairs[i].our->setState(extent_state_dirty);
        ref_extent_set_insert(ref_set, pairs[i].ref);
        our_set->insert(pairs[i].our);
        pairs[i].present = true;
    }

    /// Removes in the `merging` state sometimes, as `extentCoalesce` / `extentActivateLocked` do.
    void remove(size_t i, bool merging)
    {
        if (merging)
        {
            ref_extent_set_state(pairs[i].ref, extent_state_merging);
            pairs[i].our->setState(extent_state_merging);
        }
        ref_extent_set_remove(ref_set, pairs[i].ref);
        our_set->remove(pairs[i].our);
        pairs[i].present = false;
    }

    /// Returns false on mismatch.
    bool compareState(int step) const
    {
        bool ok = true;
        if (ref_extent_set_num_pages(ref_set) != our_set->numPagesGet())
        {
            std::fprintf(stderr, "step %d: npages %zu vs %zu\n", step, ref_extent_set_num_pages(ref_set), our_set->numPagesGet());
            ok = false;
        }
        for (unsigned page_size_class_idx = 0; page_size_class_idx < EXTENT_SET_NUM_PAGE_SIZES; ++page_size_class_idx)
        {
            if (ref_extent_set_num_extents(ref_set, page_size_class_idx) != our_set->numExtentsGet(page_size_class_idx)
                || ref_extent_set_num_bytes(ref_set, page_size_class_idx) != our_set->numBytesGet(page_size_class_idx))
            {
                std::fprintf(stderr, "step %d: bin %u stats mismatch\n", step, page_size_class_idx);
                ok = false;
            }
            bool ref_empty = ref_extent_set_bin_empty(ref_set, page_size_class_idx);
            if (ref_empty != our_set->bins[page_size_class_idx].heap.empty())
            {
                std::fprintf(stderr, "step %d: bin %u emptiness mismatch\n", step, page_size_class_idx);
                ok = false;
                continue;
            }
            if (ref_empty)
                continue;
            uint64_t serial_number;
            uintptr_t addr;
            ref_extent_set_heap_min(ref_set, page_size_class_idx, &serial_number, &addr);
            if (serial_number != our_set->bins[page_size_class_idx].heap_min.serial_number
                || addr != our_set->bins[page_size_class_idx].heap_min.addr)
            {
                std::fprintf(stderr, "step %d: bin %u heap_min mismatch\n", step, page_size_class_idx);
                ok = false;
            }
            size_t ref_auxiliary_count;
            void * ref_root = ref_extent_set_heap_root(ref_set, page_size_class_idx, &ref_auxiliary_count);
            if (indexOfRef(ref_root) != indexOfOur(our_set->bins[page_size_class_idx].heap.rootNode())
                || ref_auxiliary_count != our_set->bins[page_size_class_idx].heap.auxiliaryCount())
            {
                std::fprintf(stderr, "step %d: bin %u heap root/auxcount mismatch\n", step, page_size_class_idx);
                ok = false;
            }
        }
        unsigned long words[16];
        size_t num_words = ref_extent_set_bitmap(ref_set, words, 16);
        if (num_words != our_set->bitmap.num_groups)
        {
            std::fprintf(stderr, "bitmap size %zu vs %zu\n", num_words, our_set->bitmap.num_groups);
            ok = false;
        }
        else
        {
            for (size_t i = 0; i < num_words; ++i)
                if (words[i] != our_set->bitmap.groups[i])
                {
                    std::fprintf(stderr, "step %d: bitmap word %zu mismatch\n", step, i);
                    ok = false;
                }
        }
        static void * lru[100000];
        size_t n = ref_extent_set_lru(ref_set, lru, 100000);
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
    /// Rarely, an extent larger than SIZE_CLASS_LARGE_MAX_CLASS (the last bin).
    if (rng() % 500 == 0)
        return SIZE_CLASS_LARGE_MAX_CLASS + PAGE * (1 + rng() % 4);
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

unsigned pickLog2MaxFit(std::mt19937_64 & rng)
{
    unsigned kind = rng() % 10;
    if (kind < 4)
        return 6;
    if (kind < 7)
        return SIZE_CLASS_PTR_BITS;
    return unsigned(rng() % (SIZE_CLASS_PTR_BITS + 1));
}

void runSequence(uint64_t seed, int steps, bool disable_large_size_classes, unsigned serial_number_range)
{
    ref_set_disable_large_size_classes(disable_large_size_classes);
    options.disable_large_size_classes = disable_large_size_classes;

    Harness h;
    std::mt19937_64 rng(seed);
    int failures = 0;
    size_t num_fits = 0;
    size_t num_found = 0;

    for (int step = 0; step < steps && failures < 5; ++step)
    {
        unsigned op = rng() % 100;
        bool ok = true;
        size_t num_present = 0;
        for (auto & pair : h.pairs)
            num_present += pair.present;

        if (op < 40 || num_present < 4)
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
                if (size > SIZE_CLASS_LARGE_MAX_CLASS)
                    addr = reinterpret_cast<void *>(uintptr_t(PAGE) * (1 + rng() % 1024));
                uint64_t serial_number = rng() % serial_number_range;
                h.pairs.push_back(
                    {ref_extent_new(addr, size, serial_number, extent_state_dirty),
                     newExtent(addr, size, serial_number, extent_state_dirty),
                     false});
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
            size_t extent_size;
            if (rng() % 2 == 0 && !h.pairs.empty())
            {
                const Pair & pair = h.pairs[rng() % h.pairs.size()];
                size_t base_size = pair.our->size();
                if (base_size > SIZE_CLASS_LARGE_MAX_CLASS)
                    base_size = PAGE;
                long delta = long(rng() % 5) - 2;
                extent_size = base_size + size_t(delta) * PAGE;
                if (extent_size == 0 || extent_size > SIZE_CLASS_LARGE_MAX_CLASS)
                    extent_size = base_size;
            }
            else
            {
                extent_size = pickPages(rng) * PAGE;
            }
            size_t alignment = pickAlignment(rng);
            bool exact_only = rng() % 5 == 0;
            unsigned log2_max_fit = pickLog2MaxFit(rng);

            void * r = ref_extent_set_fit(h.ref_set, extent_size, alignment, exact_only, log2_max_fit);
            Extent * o = h.our_set->fit(extent_size, alignment, exact_only, log2_max_fit);
            ++num_fits;
            int ref_idx = r ? h.indexOfRef(r) : -1;
            int our_idx = o ? h.indexOfOur(o) : -1;
            if (ref_idx != our_idx)
            {
                std::fprintf(
                    stderr,
                    "seed %llu step %d: fit(esize=%zu, alignment=%zu, exact_only=%d, lg_max_fit=%u): %d vs %d\n",
                    static_cast<unsigned long long>(seed),
                    step,
                    extent_size,
                    alignment,
                    int(exact_only),
                    log2_max_fit,
                    ref_idx,
                    our_idx);
                ok = false;
            }
            else if (ref_idx >= 0)
            {
                ++num_found;
                /// As `extentActivateLocked` does, usually.
                if (rng() % 3 != 0)
                    h.remove(size_t(ref_idx), false);
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
    CHECK_GT(num_fits, size_t(steps / 5));
    CHECK_GT(num_found, size_t(0));
    CHECK_LT(num_found, num_fits);
}

}

TEST(ExtentSetOracle, Layout)
{
    bootOnce();
    CHECK_EQ(ref_sizeof_extent_set(), sizeof(ExtentSet));
    CHECK_EQ(ref_extent_set_num_page_sizes(), EXTENT_SET_NUM_PAGE_SIZES);
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
    options.disable_large_size_classes = true;
    ref_set_disable_large_size_classes(true);
}

/// Many extents in the same few bins (deep heaps, so that the 32-node limit of the enumeration matters).
TEST(ExtentSetOracle, DeepHeaps)
{
    bootOnce();
    ref_set_disable_large_size_classes(true);
    options.disable_large_size_classes = true;

    Harness h;
    std::mt19937_64 rng(42);
    int failures = 0;
    /// Sizes in [17, 20] pages fall into the bin of 16 pages + pad.. a few bins only.
    for (int i = 0; i < 300; ++i)
    {
        size_t size = (16 + rng() % 8) * PAGE;
        void * addr = reinterpret_cast<void *>((uintptr_t(1) << 40) + uintptr_t(i) * (uintptr_t(1) << 30) + (rng() % 1024) * PAGE);
        uint64_t serial_number = rng() % 50;
        h.pairs.push_back(
            {ref_extent_new(addr, size, serial_number, extent_state_dirty),
             newExtent(addr, size, serial_number, extent_state_dirty),
             false});
        h.insert(h.pairs.size() - 1);
    }
    REQUIRE(h.compareState(-1));
    for (int step = 0; step < 2000 && failures < 5; ++step)
    {
        size_t extent_size = (14 + rng() % 12) * PAGE;
        size_t alignment = rng() % 4 == 0 ? (PAGE << (1 + rng() % 4)) : PAGE;
        bool exact_only = rng() % 4 == 0;
        unsigned log2_max_fit = rng() % 2 ? 6 : SIZE_CLASS_PTR_BITS;
        void * r = ref_extent_set_fit(h.ref_set, extent_size, alignment, exact_only, log2_max_fit);
        Extent * o = h.our_set->fit(extent_size, alignment, exact_only, log2_max_fit);
        int ref_idx = r ? h.indexOfRef(r) : -1;
        int our_idx = o ? h.indexOfOur(o) : -1;
        bool ok = ref_idx == our_idx;
        if (ok && ref_idx >= 0)
        {
            h.remove(size_t(ref_idx), rng() % 2 == 0);
            /// Re-insert another removed extent to keep the heaps deep.
            for (size_t k = 0; k < h.pairs.size(); ++k)
            {
                size_t j = (size_t(ref_idx) + 1 + k) % h.pairs.size();
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
