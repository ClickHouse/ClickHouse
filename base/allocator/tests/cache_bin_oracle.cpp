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

extern "C" {
size_t ref_cache_bin_size();
size_t ref_cache_bin_num_cached_max_limit();
size_t ref_cache_bin_num_flush_batch_max();
size_t ref_thread_cache_sizes(size_t * slow_size);
size_t ref_thread_event_data_size();
size_t ref_cache_bins_init(
    RefBin * bins, const uint16_t * num_cached_max, unsigned num_bins, void * memory, size_t * computed_size, size_t * computed_alignment);
void ref_cache_bin_init_disabled(RefBin * bin, uint16_t num_cached_max);
bool ref_cache_bin_disabled(RefBin * bin);
const void * ref_disabled_bin();
void * ref_cache_bin_alloc_easy(RefBin * bin, bool * success);
void * ref_cache_bin_alloc(RefBin * bin, bool * success);
uint16_t ref_cache_bin_alloc_batch(RefBin * bin, size_t num, void ** out);
bool ref_cache_bin_deallocate_easy(RefBin * bin, void * ptr);
bool ref_cache_bin_stash(RefBin * bin, void * ptr);
bool ref_cache_bin_full(RefBin * bin);
void ref_cache_bin_low_water_set(RefBin * bin);
void ref_cache_bin_low_water_adjust(RefBin * bin);
uint16_t ref_cache_bin_low_water_get(RefBin * bin);
uint16_t ref_cache_bin_num_cached_get_local(RefBin * bin);
uint16_t ref_cache_bin_num_stashed_get_local(RefBin * bin);
void ref_cache_bin_num_items_get_remote(RefBin * bin, uint16_t * num_cached, uint16_t * num_stashed);
void ** ref_cache_bin_empty_position_get(RefBin * bin);
void ** ref_cache_bin_low_bound_get(RefBin * bin);
void ** ref_cache_bin_fill_begin(RefBin * bin, uint16_t num_fill);
void ref_cache_bin_fill_finish(RefBin * bin, uint16_t num_fill, void ** ptr, uint16_t num_filled);
void ** ref_cache_bin_flush_begin(RefBin * bin, uint16_t num_flush);
void ref_cache_bin_flush_finish(RefBin * bin, uint16_t num_flush, void ** ptr, uint16_t num_flushed);
void ** ref_cache_bin_flush_stashed_begin(RefBin * bin, uint16_t num_stashed);
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
    std::vector<uint16_t> num_cached_max;
    size_t size = 0;
    std::byte * memory = nullptr;
    std::byte * ref_memory = nullptr;
    std::vector<CacheBin> bins;
    std::vector<RefBin> ref_bins;

    explicit Pair(std::vector<uint16_t> num_cached_max_, size_t offset)
        : num_cached_max(std::move(num_cached_max_))
        , bins(num_cached_max.size())
        , ref_bins(num_cached_max.size())
    {
        std::vector<CacheBinInfo> infos(num_cached_max.size());
        for (size_t i = 0; i < infos.size(); ++i)
            infos[i].init(num_cached_max[i]);
        size_t alignment;
        cacheBinInfoComputeAlloc(infos.data(), unsigned(infos.size()), size, alignment);
        CHECK_EQ(alignment, PAGE);

        /// `offset` (a multiple of PAGE) moves the stacks relative to the 64 KiB boundaries.
        size_t total = alignmentCeiling(offset + size, REGION_ALIGNMENT);
        memory = static_cast<std::byte *>(std::aligned_alloc(REGION_ALIGNMENT, total)) + offset;
        ref_memory = static_cast<std::byte *>(std::aligned_alloc(REGION_ALIGNMENT, total)) + offset;
        memset(memory, 0xa5, size);
        memset(ref_memory, 0xa5, size);

        size_t current_offset = 0;
        cacheBinPreincrement(infos.data(), unsigned(infos.size()), memory, current_offset);
        for (size_t i = 0; i < infos.size(); ++i)
            bins[i].init(infos[i], memory, current_offset);
        cacheBinPostincrement(memory, current_offset);
        CHECK_EQ(current_offset, size);

        size_t ref_size;
        size_t ref_alignment;
        size_t ref_used = ref_cache_bins_init(
            ref_bins.data(), num_cached_max.data(), unsigned(num_cached_max.size()), ref_memory, &ref_size, &ref_alignment);
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
        CHECK_EQ(reinterpret_cast<std::byte *>(a.stack_head) - memory, reinterpret_cast<std::byte *>(b.stack_head) - ref_memory);
        CHECK_EQ(a.thread_cache_stats.num_requests, b.thread_cache_stats.num_requests);
        CHECK_EQ(a.low_bits_low_water, b.low_bits_low_water);
        CHECK_EQ(a.low_bits_full, b.low_bits_full);
        CHECK_EQ(a.low_bits_empty, b.low_bits_empty);
        CHECK_EQ(a.bin_info.num_cached_max, b.bin_info.num_cached_max);

        CHECK_EQ(a.full(), ref_cache_bin_full(&b));
        CHECK_EQ(a.numCachedGetLocal(), ref_cache_bin_num_cached_get_local(&b));
        CHECK_EQ(a.numStashedGetLocal(), ref_cache_bin_num_stashed_get_local(&b));
        CHECK_EQ(a.lowWaterGet(), ref_cache_bin_low_water_get(&b));
        CHECK_EQ(
            reinterpret_cast<std::byte *>(a.emptyPositionGet()) - memory,
            reinterpret_cast<std::byte *>(ref_cache_bin_empty_position_get(&b)) - ref_memory);
        CHECK_EQ(
            reinterpret_cast<std::byte *>(a.lowBoundGet()) - memory,
            reinterpret_cast<std::byte *>(ref_cache_bin_low_bound_get(&b)) - ref_memory);
        CacheBinSize num_cached;
        CacheBinSize num_stashed;
        CacheBinSize ref_num_cached;
        CacheBinSize ref_num_stashed;
        a.numItemsGetRemote(num_cached, num_stashed);
        ref_cache_bin_num_items_get_remote(&b, &ref_num_cached, &ref_num_stashed);
        CHECK_EQ(num_cached, ref_num_cached);
        CHECK_EQ(num_stashed, ref_num_stashed);
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
        if (memcmp(memory, ref_memory, size) != 0)
        {
            std::fprintf(stderr, "stack memory differs after %s\n", what);
            allocator_test::abortTest();
        }
    }
};

void runRandom(const std::vector<uint16_t> & num_cached_max, size_t offset, uint64_t seed, size_t num_ops)
{
    Pair pair(num_cached_max, offset);
    Rng rng{seed};
    uintptr_t next_ptr = 0x100000;
    auto fakePtr = [&] { return reinterpret_cast<void *>(next_ptr += 16); };

    for (size_t op = 0; op < num_ops; ++op)
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
                ++a.thread_cache_stats.num_requests;
                ++b.thread_cache_stats.num_requests;
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
                ++a.thread_cache_stats.num_requests;
                ++b.thread_cache_stats.num_requests;
            }
        }
        else if (kind < 70)
        {
            what = "dallocEasy";
            void * p = fakePtr();
            CHECK_EQ(a.deallocateEasy(p), ref_cache_bin_deallocate_easy(&b, p));
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
            if (a.numCachedGetLocal() != 0)
            {
                /// Fills are only done on empty bins: flush everything first.
                CacheBinSize n = a.numCachedGetLocal();
                CacheBinPtrArray array(n);
                a.initPtrArrayForFlush(array, n);
                a.finishFlush(array, n);
                void ** ref_ptr = ref_cache_bin_flush_begin(&b, n);
                ref_cache_bin_flush_finish(&b, n, ref_ptr, n);
            }
            CacheBinSize room = CacheBinSize(a.numCachedMaxGet() - a.numStashedGetLocal());
            CacheBinSize num_fill = CacheBinSize(rng.below(uint64_t(room) + 1));
            CacheBinSize num_filled = CacheBinSize(rng.below(uint64_t(num_fill) + 1));
            CacheBinPtrArray array(num_fill);
            a.initPtrArrayForFill(array, num_fill);
            void ** ref_ptr = ref_cache_bin_fill_begin(&b, num_fill);
            CHECK_EQ(reinterpret_cast<std::byte *>(array.ptr) - pair.memory, reinterpret_cast<std::byte *>(ref_ptr) - pair.ref_memory);
            for (CacheBinSize k = 0; k < num_filled; ++k)
            {
                void * p = fakePtr();
                array.ptr[k] = p;
                ref_ptr[k] = p;
            }
            a.finishFill(array, num_filled);
            ref_cache_bin_fill_finish(&b, num_fill, ref_ptr, num_filled);
        }
        else if (kind < 93)
        {
            what = "flush";
            CacheBinSize num_cached = a.numCachedGetLocal();
            CacheBinSize num_flush = CacheBinSize(rng.below(uint64_t(num_cached) + 1));
            CacheBinPtrArray array(num_flush);
            a.initPtrArrayForFlush(array, num_flush);
            void ** ref_ptr = ref_cache_bin_flush_begin(&b, num_flush);
            CHECK_EQ(reinterpret_cast<std::byte *>(array.ptr) - pair.memory, reinterpret_cast<std::byte *>(ref_ptr) - pair.ref_memory);
            a.finishFlush(array, num_flush);
            ref_cache_bin_flush_finish(&b, num_flush, ref_ptr, num_flush);
        }
        else if (kind < 96)
        {
            what = "flushStashed";
            CacheBinSize num_stashed = a.numStashedGetLocal();
            if (num_stashed > 0)
            {
                CacheBinPtrArray array(num_stashed);
                a.initPtrArrayForStashed(CacheBinSize(i), array, num_stashed);
                void ** ref_ptr = ref_cache_bin_flush_stashed_begin(&b, num_stashed);
                CHECK_EQ(reinterpret_cast<std::byte *>(array.ptr) - pair.memory, reinterpret_cast<std::byte *>(ref_ptr) - pair.ref_memory);
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
            CacheBinSize n1 = a.allocBatch(num, out1.data());
            CacheBinSize n2 = ref_cache_bin_alloc_batch(&b, num, out2.data());
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
    CHECK_EQ(ref_cache_bin_num_cached_max_limit(), CACHE_BIN_NUM_CACHED_MAX);
    CHECK_EQ(ref_cache_bin_num_flush_batch_max(), CACHE_BIN_NUM_FLUSH_BATCH_MAX);
    size_t slow_size;
    CHECK_EQ(ref_thread_cache_sizes(&slow_size), sizeof(ThreadCache));
    CHECK_EQ(slow_size, sizeof(ThreadCacheSlow));
    CHECK_EQ(ref_thread_event_data_size(), sizeof(ThreadEventData));
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
    auto ref_low_bits = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(ref_disabled_bin()));
    auto low_bits = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(CacheBin::disabledBinStack()));
    CHECK_EQ(b.low_bits_low_water, ref_low_bits);
    CHECK_EQ(b.low_bits_full, ref_low_bits);
    CHECK_EQ(b.low_bits_empty, ref_low_bits);
    CHECK_EQ(a.low_bits_low_water, low_bits);
    CHECK_EQ(a.low_bits_full, low_bits);
    CHECK_EQ(a.low_bits_empty, low_bits);
    CHECK_EQ(a.bin_info.num_cached_max, b.bin_info.num_cached_max);
    CHECK_EQ(a.thread_cache_stats.num_requests, b.thread_cache_stats.num_requests);
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
