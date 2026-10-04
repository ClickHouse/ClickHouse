/// Compares `CacheBin` with jemalloc's `cache_bin.h` / `cache_bin.c` (`cache_bin_oracle_ref.c`): the same random
/// operation sequences on bins laid out identically in two 64 KiB-aligned stack regions (so the 16-bit low bits are
/// the same); after every operation the results, all fields, the derived counts and the whole stack memory must match.

#include <allocator/CacheBin.h>
#include <allocator/ThreadCacheData.h>
#include <allocator/ThreadEventData.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <vector>

using namespace jemalloc;

/// The C `cache_bin_t` has the same layout as `CacheBin` (checked below), so the reference functions are called on
/// `CacheBin` objects that are only touched by the reference.
using RefBin = CacheBin;

extern "C"
{
size_t ref_cache_bin_size();
size_t ref_cache_bin_ncached_max_limit();
size_t ref_cache_bin_nflush_batch_max();
size_t ref_tcache_sizes(size_t * slow_size);
size_t ref_te_data_size();
size_t ref_cache_bins_init(
    RefBin * bins, const uint16_t * ncached_max, unsigned nbins, void * mem, size_t * computed_size, size_t * computed_alignment);
void ref_cache_bin_init_disabled(RefBin * bin, uint16_t ncached_max);
bool ref_cache_bin_disabled(RefBin * bin);
const void * ref_disabled_bin();
void * ref_cache_bin_alloc_easy(RefBin * bin, bool * success);
void * ref_cache_bin_alloc(RefBin * bin, bool * success);
uint16_t ref_cache_bin_alloc_batch(RefBin * bin, size_t num, void ** out);
bool ref_cache_bin_dalloc_easy(RefBin * bin, void * ptr);
bool ref_cache_bin_stash(RefBin * bin, void * ptr);
bool ref_cache_bin_full(RefBin * bin);
void ref_cache_bin_low_water_set(RefBin * bin);
void ref_cache_bin_low_water_adjust(RefBin * bin);
uint16_t ref_cache_bin_low_water_get(RefBin * bin);
uint16_t ref_cache_bin_ncached_get_local(RefBin * bin);
uint16_t ref_cache_bin_nstashed_get_local(RefBin * bin);
void ref_cache_bin_nitems_get_remote(RefBin * bin, uint16_t * ncached, uint16_t * nstashed);
void ** ref_cache_bin_empty_position_get(RefBin * bin);
void ** ref_cache_bin_low_bound_get(RefBin * bin);
void ** ref_cache_bin_fill_begin(RefBin * bin, uint16_t nfill);
void ref_cache_bin_fill_finish(RefBin * bin, uint16_t nfill, void ** ptr, uint16_t nfilled);
void ** ref_cache_bin_flush_begin(RefBin * bin, uint16_t nflush);
void ref_cache_bin_flush_finish(RefBin * bin, uint16_t nflush, void ** ptr, uint16_t nflushed);
void ** ref_cache_bin_flush_stashed_begin(RefBin * bin, uint16_t nstashed);
void ref_cache_bin_flush_stashed_finish(RefBin * bin);
}

namespace
{

struct Rng
{
    uint64_t x;

    uint64_t next()
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x;
    }

    uint64_t below(uint64_t n) { return n == 0 ? 0 : next() % n; }
};

constexpr size_t REGION_ALIGNMENT = 65536;

/// Two identically laid out sets of bins.
struct Pair
{
    std::vector<uint16_t> ncached_max;
    size_t size = 0;
    std::byte * mem = nullptr;
    std::byte * ref_mem = nullptr;
    std::vector<CacheBin> bins;
    std::vector<RefBin> ref_bins;

    explicit Pair(std::vector<uint16_t> ncached_max_, size_t offset)
        : ncached_max(std::move(ncached_max_)), bins(ncached_max.size()), ref_bins(ncached_max.size())
    {
        std::vector<CacheBinInfo> infos(ncached_max.size());
        for (size_t i = 0; i < infos.size(); ++i)
            infos[i].init(ncached_max[i]);
        size_t alignment;
        cacheBinInfoComputeAlloc(infos.data(), unsigned(infos.size()), size, alignment);
        CHECK_EQ(alignment, PAGE);

        /// `offset` (a multiple of PAGE) moves the stacks relative to the 64 KiB boundaries.
        size_t total = alignmentCeiling(offset + size, REGION_ALIGNMENT);
        mem = static_cast<std::byte *>(std::aligned_alloc(REGION_ALIGNMENT, total)) + offset;
        ref_mem = static_cast<std::byte *>(std::aligned_alloc(REGION_ALIGNMENT, total)) + offset;
        memset(mem, 0xa5, size);
        memset(ref_mem, 0xa5, size);

        size_t cur_offset = 0;
        cacheBinPreincrement(infos.data(), unsigned(infos.size()), mem, cur_offset);
        for (size_t i = 0; i < infos.size(); ++i)
            bins[i].init(infos[i], mem, cur_offset);
        cacheBinPostincrement(mem, cur_offset);
        CHECK_EQ(cur_offset, size);

        size_t ref_size;
        size_t ref_alignment;
        size_t ref_used = ref_cache_bins_init(ref_bins.data(), ncached_max.data(), unsigned(ncached_max.size()), ref_mem, &ref_size, &ref_alignment);
        CHECK_EQ(ref_size, size);
        CHECK_EQ(ref_alignment, alignment);
        CHECK_EQ(ref_used, size);
        compareAll("init");
    }

    void compareBin(size_t i, const char * what)
    {
        CacheBin & a = bins[i];
        RefBin & b = ref_bins[i];
        int failures = allocator_test::failureCount();
        CHECK_EQ(reinterpret_cast<std::byte *>(a.stack_head) - mem, reinterpret_cast<std::byte *>(b.stack_head) - ref_mem);
        CHECK_EQ(a.tstats.nrequests, b.tstats.nrequests);
        CHECK_EQ(a.low_bits_low_water, b.low_bits_low_water);
        CHECK_EQ(a.low_bits_full, b.low_bits_full);
        CHECK_EQ(a.low_bits_empty, b.low_bits_empty);
        CHECK_EQ(a.bin_info.ncached_max, b.bin_info.ncached_max);

        CHECK_EQ(a.full(), ref_cache_bin_full(&b));
        CHECK_EQ(a.ncachedGetLocal(), ref_cache_bin_ncached_get_local(&b));
        CHECK_EQ(a.nstashedGetLocal(), ref_cache_bin_nstashed_get_local(&b));
        CHECK_EQ(a.lowWaterGet(), ref_cache_bin_low_water_get(&b));
        CHECK_EQ(reinterpret_cast<std::byte *>(a.emptyPositionGet()) - mem, reinterpret_cast<std::byte *>(ref_cache_bin_empty_position_get(&b)) - ref_mem);
        CHECK_EQ(reinterpret_cast<std::byte *>(a.lowBoundGet()) - mem, reinterpret_cast<std::byte *>(ref_cache_bin_low_bound_get(&b)) - ref_mem);
        cache_bin_sz_t ncached;
        cache_bin_sz_t nstashed;
        cache_bin_sz_t ref_ncached;
        cache_bin_sz_t ref_nstashed;
        a.nitemsGetRemote(ncached, nstashed);
        ref_cache_bin_nitems_get_remote(&b, &ref_ncached, &ref_nstashed);
        CHECK_EQ(ncached, ref_ncached);
        CHECK_EQ(nstashed, ref_nstashed);
        if (allocator_test::failureCount() != failures)
        {
            std::fprintf(stderr, "  after %s on bin %zu\n", what, i);
            allocator_test::abortTest();
        }
    }

    void compareAll(const char * what)
    {
        for (size_t i = 0; i < bins.size(); ++i)
            compareBin(i, what);
        if (memcmp(mem, ref_mem, size) != 0)
        {
            std::fprintf(stderr, "stack memory differs after %s\n", what);
            allocator_test::abortTest();
        }
    }
};

void runRandom(const std::vector<uint16_t> & ncached_max, size_t offset, uint64_t seed, size_t nops)
{
    Pair pair(ncached_max, offset);
    Rng rng{seed};
    uintptr_t next_ptr = 0x100000;
    auto fakePtr = [&] { return reinterpret_cast<void *>(next_ptr += 16); };

    for (size_t op = 0; op < nops; ++op)
    {
        size_t i = rng.below(pair.bins.size());
        CacheBin & a = pair.bins[i];
        RefBin & b = pair.ref_bins[i];
        uint64_t kind = rng.below(100);
        const char * what = "";
        if (kind < 22)
        {
            what = "allocEasy";
            bool s1 = false;
            bool s2 = false;
            void * p1 = a.allocEasy(s1);
            void * p2 = ref_cache_bin_alloc_easy(&b, &s2);
            CHECK_EQ(s1, s2);
            CHECK_EQ(p1, p2);
            if (s1)
            {
                ++a.tstats.nrequests;
                ++b.tstats.nrequests;
            }
        }
        else if (kind < 40)
        {
            what = "alloc";
            bool s1 = false;
            bool s2 = false;
            void * p1 = a.alloc(s1);
            void * p2 = ref_cache_bin_alloc(&b, &s2);
            CHECK_EQ(s1, s2);
            CHECK_EQ(p1, p2);
            if (s1)
            {
                ++a.tstats.nrequests;
                ++b.tstats.nrequests;
            }
        }
        else if (kind < 70)
        {
            what = "dallocEasy";
            void * p = fakePtr();
            CHECK_EQ(a.dallocEasy(p), ref_cache_bin_dalloc_easy(&b, p));
        }
        else if (kind < 74)
        {
            what = "stash";
            void * p = fakePtr();
            CHECK_EQ(a.stash(p), ref_cache_bin_stash(&b, p));
        }
        else if (kind < 79)
        {
            what = "lowWaterSet";
            a.lowWaterSet();
            ref_cache_bin_low_water_set(&b);
        }
        else if (kind < 82)
        {
            what = "lowWaterAdjust";
            a.lowWaterAdjust();
            ref_cache_bin_low_water_adjust(&b);
        }
        else if (kind < 87)
        {
            what = "fill";
            if (a.ncachedGetLocal() != 0)
            {
                /// Fills are only done on empty bins: flush everything first.
                cache_bin_sz_t n = a.ncachedGetLocal();
                CacheBinPtrArray arr(n);
                a.initPtrArrayForFlush(arr, n);
                a.finishFlush(arr, n);
                void ** ref_ptr = ref_cache_bin_flush_begin(&b, n);
                ref_cache_bin_flush_finish(&b, n, ref_ptr, n);
            }
            cache_bin_sz_t room = cache_bin_sz_t(a.ncachedMaxGet() - a.nstashedGetLocal());
            cache_bin_sz_t nfill = cache_bin_sz_t(rng.below(uint64_t(room) + 1));
            cache_bin_sz_t nfilled = cache_bin_sz_t(rng.below(uint64_t(nfill) + 1));
            CacheBinPtrArray arr(nfill);
            a.initPtrArrayForFill(arr, nfill);
            void ** ref_ptr = ref_cache_bin_fill_begin(&b, nfill);
            CHECK_EQ(reinterpret_cast<std::byte *>(arr.ptr) - pair.mem, reinterpret_cast<std::byte *>(ref_ptr) - pair.ref_mem);
            for (cache_bin_sz_t k = 0; k < nfilled; ++k)
            {
                void * p = fakePtr();
                arr.ptr[k] = p;
                ref_ptr[k] = p;
            }
            a.finishFill(arr, nfilled);
            ref_cache_bin_fill_finish(&b, nfill, ref_ptr, nfilled);
        }
        else if (kind < 93)
        {
            what = "flush";
            cache_bin_sz_t ncached = a.ncachedGetLocal();
            cache_bin_sz_t nflush = cache_bin_sz_t(rng.below(uint64_t(ncached) + 1));
            CacheBinPtrArray arr(nflush);
            a.initPtrArrayForFlush(arr, nflush);
            void ** ref_ptr = ref_cache_bin_flush_begin(&b, nflush);
            CHECK_EQ(reinterpret_cast<std::byte *>(arr.ptr) - pair.mem, reinterpret_cast<std::byte *>(ref_ptr) - pair.ref_mem);
            a.finishFlush(arr, nflush);
            ref_cache_bin_flush_finish(&b, nflush, ref_ptr, nflush);
        }
        else if (kind < 96)
        {
            what = "flushStashed";
            cache_bin_sz_t nstashed = a.nstashedGetLocal();
            if (nstashed > 0)
            {
                CacheBinPtrArray arr(nstashed);
                a.initPtrArrayForStashed(cache_bin_sz_t(i), arr, nstashed);
                void ** ref_ptr = ref_cache_bin_flush_stashed_begin(&b, nstashed);
                CHECK_EQ(reinterpret_cast<std::byte *>(arr.ptr) - pair.mem, reinterpret_cast<std::byte *>(ref_ptr) - pair.ref_mem);
                a.finishFlushStashed();
                ref_cache_bin_flush_stashed_finish(&b);
            }
        }
        else
        {
            what = "allocBatch";
            size_t num = rng.below(300);
            std::vector<void *> out1(num + 1);
            std::vector<void *> out2(num + 1);
            cache_bin_sz_t n1 = a.allocBatch(num, out1.data());
            cache_bin_sz_t n2 = ref_cache_bin_alloc_batch(&b, num, out2.data());
            CHECK_EQ(n1, n2);
            CHECK(memcmp(out1.data(), out2.data(), n1 * sizeof(void *)) == 0);
        }
        pair.compareBin(i, what);
        if (op % 64 == 0 || kind >= 82)
            pair.compareAll(what);
    }
    pair.compareAll("end");
}

}

TEST(CacheBinOracle, Constants)
{
    CHECK_EQ(ref_cache_bin_size(), sizeof(CacheBin));
    CHECK_EQ(ref_cache_bin_ncached_max_limit(), CACHE_BIN_NCACHED_MAX);
    CHECK_EQ(ref_cache_bin_nflush_batch_max(), CACHE_BIN_NFLUSH_BATCH_MAX);
    size_t slow_size;
    CHECK_EQ(ref_tcache_sizes(&slow_size), sizeof(ThreadCache));
    CHECK_EQ(slow_size, sizeof(ThreadCacheSlow));
    CHECK_EQ(ref_te_data_size(), sizeof(ThreadEventData));
}

TEST(CacheBinOracle, Disabled)
{
    CacheBin a;
    RefBin b;
    a.initDisabled(77);
    ref_cache_bin_init_disabled(&b, 77);
    CHECK(a.disabled());
    CHECK(ref_cache_bin_disabled(&b));
    CHECK_EQ(static_cast<const void *>(b.stack_head), ref_disabled_bin());
    auto ref_low_bits = static_cast<cache_bin_sz_t>(reinterpret_cast<uintptr_t>(ref_disabled_bin()));
    auto low_bits = static_cast<cache_bin_sz_t>(reinterpret_cast<uintptr_t>(CacheBin::disabledBinStack()));
    CHECK_EQ(b.low_bits_low_water, ref_low_bits);
    CHECK_EQ(b.low_bits_full, ref_low_bits);
    CHECK_EQ(b.low_bits_empty, ref_low_bits);
    CHECK_EQ(a.low_bits_low_water, low_bits);
    CHECK_EQ(a.low_bits_full, low_bits);
    CHECK_EQ(a.low_bits_empty, low_bits);
    CHECK_EQ(a.bin_info.ncached_max, b.bin_info.ncached_max);
    CHECK_EQ(a.tstats.nrequests, b.tstats.nrequests);
    CHECK_EQ(*static_cast<const uintptr_t *>(ref_disabled_bin()), disabled_bin);
}

TEST(CacheBinOracle, RandomSmall)
{
    for (uint64_t seed = 1; seed <= 20; ++seed)
        runRandom({20, 0, 200, 64, 8, 128}, PAGE * (seed % 3), seed * 0x9e3779b97f4a7c15ULL, 20000);
}

/// The default small bins of 64 KiB pages are 20..200; a bin of the maximum size crosses a 64 KiB boundary of the low
/// bits at every offset.
TEST(CacheBinOracle, RandomLarge)
{
    for (uint64_t seed = 1; seed <= 6; ++seed)
        runRandom({3, 8191, 4000}, PAGE * seed, seed * 0x2545f4914f6cdd1dULL, 60000);
}
