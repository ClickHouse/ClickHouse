/// The thread cache: the `num_cached_max` tables per page size, the boot globals, the `thread_cache_num_cached_max` overrides,
/// the fill count adaptation (`cache_bin_fill_ctl_t`), the GC locality heuristic (remote pointer counting and the
/// bin shuffle) on synthetic bins.
/// Ported from jemalloc's `test/unit/ncached_max.c`, `test/unit/thread_cache_max.c` (the parts that do not need the
/// mallctl front-end) and the formulas of `tcache.c`.

#include <allocator/MallocConf.h>
#include <allocator/Options.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <vector>

using namespace jemalloc;
using namespace jemalloc::thread_cache_detail;

namespace
{

/// Restores the tcache options after a test changes them.
struct OptionsGuard
{
    Options saved = options;
    ~OptionsGuard()
    {
        options = saved;
        threadCacheBoot(nullptr, nullptr);
    }
};

/// An independent model of `tcache_ncached_max_compute` (spec 02, section 4.1).
unsigned modelNumCachedMax(SizeClassIdx idx)
{
    if (idx >= SIZE_CLASS_NUM_BINS)
        return options.thread_cache_num_slots_large;
    unsigned low = options.thread_cache_num_slots_small_min;
    unsigned high = options.thread_cache_num_slots_small_max > 8191 ? 8191 : options.thread_cache_num_slots_small_max;
    low += low & 1;
    high -= high & 1;
    low = low < 2 ? 2 : low;
    high = high < 2 ? 2 : high;
    low = low > high ? high : low;
    unsigned num_regions = bin_infos[idx].num_regions;
    unsigned c = options.log2_thread_cache_num_slots_multiplier < 0 ? (num_regions >> -options.log2_thread_cache_num_slots_multiplier)
                                                                    : (num_regions << options.log2_thread_cache_num_slots_multiplier);
    c += c & 1;
    return c <= low ? low : (c <= high ? c : high);
}

/// A cache bin with its own stack (as laid out by `tcache_init` for a single bin).
struct SyntheticBin
{
    CacheBin bin;
    void * memory = nullptr;

    explicit SyntheticBin(CacheBinSize num_cached_max)
    {
        CacheBinInfo info;
        info.init(num_cached_max);
        size_t size;
        size_t alignment;
        cacheBinInfoComputeAlloc(&info, 1, size, alignment);
        memory = std::aligned_alloc(alignment, alignmentCeiling(size, alignment));
        size_t current_offset = 0;
        cacheBinPreincrement(&info, 1, memory, current_offset);
        bin.init(info, memory, current_offset);
        cacheBinPostincrement(memory, current_offset);
        REQUIRE(current_offset == size);
    }

    ~SyntheticBin() { std::free(memory); }

    /// Pushes so that `items[0]` ends up at the head (top) of the stack.
    void fillTopFirst(const std::vector<uintptr_t> & items)
    {
        for (size_t i = items.size(); i > 0; --i)
            REQUIRE(bin.deallocateEasy(reinterpret_cast<void *>(items[i - 1])));
    }

    std::vector<uintptr_t> contents() const
    {
        std::vector<uintptr_t> result;
        for (CacheBinSize i = 0; i < bin.numCachedGetLocal(); ++i)
            result.push_back(reinterpret_cast<uintptr_t>(bin.stack_head[i]));
        return result;
    }
};

}

TEST(ThreadCache, Constants)
{
    /// tcache_types.h; spec 02, section 1.
    if constexpr (LOG2_PAGE == 12)
    {
        CHECK_EQ(THREAD_CACHE_NUM_BINS_MAX, 41u);
        CHECK_EQ(THREAD_CACHE_MAX_CLASS_LIMIT, size_t(32) << 10);
        CHECK_EQ(THREAD_CACHE_GC_SMALL_NUM_BINS_MAX, 4u);
    }
    else if constexpr (LOG2_PAGE == 14)
    {
        CHECK_EQ(THREAD_CACHE_NUM_BINS_MAX, 49u);
        CHECK_EQ(THREAD_CACHE_MAX_CLASS_LIMIT, size_t(128) << 10);
        CHECK_EQ(THREAD_CACHE_GC_SMALL_NUM_BINS_MAX, 5u);
    }
    else if constexpr (LOG2_PAGE == 16)
    {
        CHECK_EQ(THREAD_CACHE_NUM_BINS_MAX, 57u);
        CHECK_EQ(THREAD_CACHE_MAX_CLASS_LIMIT, size_t(512) << 10);
        CHECK_EQ(THREAD_CACHE_GC_SMALL_NUM_BINS_MAX, 6u);
    }
    CHECK_EQ(THREAD_CACHE_GC_LARGE_NUM_BINS_MAX, 1u);
    CHECK_EQ(THREAD_CACHE_GC_NEIGHBOR_LIMIT, uintptr_t(2) << 20);
    CHECK_EQ(THREAD_CACHE_GC_INTERVAL_NS, uint64_t(10000000));
    CHECK_EQ(MALLOCX_THREAD_CACHE_MAX, 4093u);
    CHECK_EQ(sizeof(ThreadCache), 8 + 24 * size_t(THREAD_CACHE_NUM_BINS_MAX));
    CHECK_EQ(reinterpret_cast<uintptr_t>(EXPLICIT_THREAD_CACHES_ELEMENT_NEED_REINIT), uintptr_t(1));
}

TEST(ThreadCache, BootDefaults)
{
    OptionsGuard guard;
    REQUIRE(!threadCacheBoot(nullptr, nullptr));
    /// `arenas.tcache_max` and `arenas.nhbins` are 32 KiB and 41 for every page size.
    CHECK_EQ(global_do_not_change_thread_cache_max_class, size_t(32768));
    CHECK_EQ(global_do_not_change_thread_cache_num_bins, 41u);

    /// The default `num_cached_max` table for 4 KiB pages (spec 02, section 4.1).
    if constexpr (LOG2_PAGE == 12)
    {
        const unsigned expected[41] = {200, 200, 200, 200, 128, 200, 200, 200, 64, 200, 128, 200, 32, 128, 64, 128, 20, 64, 32, 64, 20,
                                       32,  20,  32,  20,  20,  20,  20,  20,  20, 20,  20,  20,  20, 20,  20, 20,  20, 20, 20, 20};
        for (SizeClassIdx i = 0; i < 41; ++i)
            CHECK_EQ(unsigned(threadCacheGetDefaultNumCachedMax()[i].num_cached_max), expected[i]);
    }
    for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
    {
        CHECK_EQ(unsigned(threadCacheGetDefaultNumCachedMax()[i].num_cached_max), modelNumCachedMax(i));
        CHECK(!threadCacheGetDefaultNumCachedMaxSet(i));
    }

    /// The default stack sizes (spec 02, section 2.4).
    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(threadCacheGetDefaultNumCachedMax(), global_do_not_change_thread_cache_num_bins, size, alignment);
    CHECK_EQ(alignment, PAGE);
    if constexpr (LOG2_PAGE == 12)
        CHECK_EQ(size, size_t(24784));
    else if constexpr (LOG2_PAGE == 14)
        CHECK_EQ(size, size_t(36304));
    else if constexpr (LOG2_PAGE == 16)
        CHECK_EQ(size, size_t(47824));
}

TEST(ThreadCache, BootThreadCacheMax)
{
    OptionsGuard guard;
    /// `thread_cache_max:4096` (`test/unit/num_cached_max.c`): the cached size classes end at 4096.
    options.thread_cache_max = 4096;
    REQUIRE(!threadCacheBoot(nullptr, nullptr));
    CHECK_EQ(global_do_not_change_thread_cache_max_class, size_t(4096));
    CHECK_EQ(global_do_not_change_thread_cache_num_bins, size_classes::sizeToIndex(4096) + 1);

    /// A non-class size is rounded up.
    options.thread_cache_max = 5000;
    REQUIRE(!threadCacheBoot(nullptr, nullptr));
    CHECK_EQ(global_do_not_change_thread_cache_max_class, size_t(5120));
    CHECK_EQ(global_do_not_change_thread_cache_num_bins, size_classes::sizeToIndex(5120) + 1);

    options.thread_cache_max = THREAD_CACHE_MAX_CLASS_LIMIT;
    REQUIRE(!threadCacheBoot(nullptr, nullptr));
    CHECK_EQ(global_do_not_change_thread_cache_max_class, THREAD_CACHE_MAX_CLASS_LIMIT);
    CHECK_EQ(global_do_not_change_thread_cache_num_bins, THREAD_CACHE_NUM_BINS_MAX);
}

TEST(ThreadCache, NumCachedMaxOptions)
{
    OptionsGuard guard;
    struct Case
    {
        ssize_t log2_multiplier;
        unsigned small_min;
        unsigned small_max;
        unsigned large;
    };
    const Case cases[] = {
        {1, 20, 200, 20},
        {0, 20, 200, 20},
        {-1, 20, 200, 20},
        {-16, 20, 200, 20},
        {2, 1, 2048, 7},
        {16, 21, 201, 1},
        {1, 300, 100, 20}, /// min > max: min becomes max.
        {1, 1, 1, 20}, /// Both clamped to 2.
        {3, 2048, 2048, 2048},
    };
    for (const auto & c : cases)
    {
        options.log2_thread_cache_num_slots_multiplier = c.log2_multiplier;
        options.thread_cache_num_slots_small_min = c.small_min;
        options.thread_cache_num_slots_small_max = c.small_max;
        options.thread_cache_num_slots_large = c.large;
        CacheBinInfo infos[THREAD_CACHE_NUM_BINS_MAX];
        threadCacheBinInfoCompute(infos);
        for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
        {
            CHECK_EQ(threadCacheNumCachedMaxCompute(i), modelNumCachedMax(i));
            CHECK_EQ(unsigned(infos[i].num_cached_max), modelNumCachedMax(i));
        }
    }

    /// Pinned values: the 8-byte class has the most regions per slab, the large classes use `thread_cache_num_slots_large`.
    options.log2_thread_cache_num_slots_multiplier = 1;
    options.thread_cache_num_slots_small_min = 21;
    options.thread_cache_num_slots_small_max = 201;
    options.thread_cache_num_slots_large = 7;
    CHECK_EQ(threadCacheNumCachedMaxCompute(0), 200u);
    CHECK_EQ(threadCacheNumCachedMaxCompute(SIZE_CLASS_NUM_BINS), 7u);
    /// The largest small class (one or two regions per slab) gets the clamped minimum.
    CHECK_EQ(threadCacheNumCachedMaxCompute(SIZE_CLASS_NUM_BINS - 1), 22u);
}

TEST(ThreadCache, NumCachedMaxConfiguration)
{
    OptionsGuard guard;
    /// The malloc_conf of `test/unit/num_cached_max.c`.
    const char * configuration = "256-1024:1001|2048-2048:0|8192-8192:1";
    REQUIRE(!threadCacheBinInfoDefaultInit(configuration, strlen(configuration)));
    options.thread_cache_max = 4096;
    REQUIRE(!threadCacheBoot(nullptr, nullptr));

    CacheBinInfo infos[THREAD_CACHE_NUM_BINS_MAX];
    threadCacheBinInfoCompute(infos);
    for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
    {
        bool first_range = (i >= size_classes::sizeToIndex(256) && i <= size_classes::sizeToIndex(1024));
        bool second_range = (i == size_classes::sizeToIndex(2048));
        bool third_range = (i == size_classes::sizeToIndex(8192));
        if (first_range || second_range || third_range)
        {
            unsigned target = first_range ? 1001 : (second_range ? 0 : 1);
            CHECK(threadCacheGetDefaultNumCachedMaxSet(i));
            CHECK_EQ(unsigned(infos[i].num_cached_max), target);
            CHECK_EQ(unsigned(threadCacheGetDefaultNumCachedMax()[i].num_cached_max), target);
        }
        else
        {
            CHECK(!threadCacheGetDefaultNumCachedMaxSet(i));
            CHECK_EQ(unsigned(infos[i].num_cached_max), modelNumCachedMax(i));
        }
    }

    /// Clipping: `n` above `CACHE_BIN_NUM_CACHED_MAX`, a range end beyond the limit, an empty range.
    const char * configuration2 = "8-8:100000|16384-100000000:3|64-32:5";
    REQUIRE(!threadCacheBinInfoDefaultInit(configuration2, strlen(configuration2)));
    threadCacheBinInfoCompute(infos);
    CHECK_EQ(unsigned(infos[0].num_cached_max), 8191u);
    for (SizeClassIdx i = size_classes::sizeToIndex(16384); i < THREAD_CACHE_NUM_BINS_MAX; ++i)
        CHECK_EQ(unsigned(infos[i].num_cached_max), 3u);
    CHECK_EQ(unsigned(infos[size_classes::sizeToIndex(64)].num_cached_max), modelNumCachedMax(size_classes::sizeToIndex(64)));

    /// Malformed settings are rejected.
    const char * bad = "8-16";
    CHECK(threadCacheBinInfoDefaultInit(bad, strlen(bad)));
}

TEST(ThreadCache, FillCountAdaptation)
{
    OptionsGuard guard;
    ThreadCacheSlow slow;
    const SizeClassIdx idx = 0;
    threadCacheBinFillControlInit(&slow, idx);
    CacheBinFillControl * mallctl = threadCacheBinFillControlGet(&slow, idx);
    CHECK_EQ(mallctl->base, 1);
    CHECK_EQ(mallctl->offset, 0);
    CHECK_EQ(threadCacheNumFillSmallLog2DivisionGet(&slow, idx), 1);

    /// No burst room at base 1.
    threadCacheNumFillSmallBurstPrepare(&slow, idx);
    CHECK_EQ(mallctl->offset, 0);

    /// GC periods with unused items halve the fill count while `num_cached_max >> base > 1`: for 200 the base stops at 7
    /// (200 >> 7 == 1), i.e. fills of 100, 50, 25, 12, 6, 3, 1.
    const unsigned expected_num_fill[] = {100, 50, 25, 12, 6, 3, 1, 1, 1};
    for (unsigned step = 0; step < 9; ++step)
    {
        unsigned num_fill = 200u >> threadCacheNumFillSmallLog2DivisionGet(&slow, idx);
        CHECK_EQ(num_fill == 0 ? 1u : num_fill, expected_num_fill[step]);
        threadCacheNumFillSmallGCUpdate(&slow, idx, 200);
    }
    CHECK_EQ(mallctl->base, 7);

    /// Bursts within a GC period: the offset grows up to base - 1.
    for (unsigned i = 1; i <= 10; ++i)
    {
        threadCacheNumFillSmallBurstPrepare(&slow, idx);
        CHECK_EQ(unsigned(mallctl->offset), i < 6 ? i : 6u);
        CHECK_EQ(unsigned(threadCacheNumFillSmallLog2DivisionGet(&slow, idx)), 7u - (i < 6 ? i : 6u));
    }
    /// A flush resets the offset.
    threadCacheNumFillSmallBurstReset(&slow, idx);
    CHECK_EQ(mallctl->offset, 0);

    /// Without the experimental GC the offset is ignored.
    threadCacheNumFillSmallBurstPrepare(&slow, idx);
    threadCacheNumFillSmallBurstPrepare(&slow, idx);
    options.experimental_thread_cache_gc = false;
    CHECK_EQ(threadCacheNumFillSmallLog2DivisionGet(&slow, idx), 7);
    options.experimental_thread_cache_gc = true;
    CHECK_EQ(threadCacheNumFillSmallLog2DivisionGet(&slow, idx), 5);

    /// A GC update resets the offset; periods with refills and no unused items double the fill count down to base 1.
    for (unsigned expected_base = 6; expected_base >= 1; --expected_base)
    {
        threadCacheNumFillSmallGCUpdate(&slow, idx, 0);
        CHECK_EQ(unsigned(mallctl->base), expected_base);
        CHECK_EQ(mallctl->offset, 0);
    }
    threadCacheNumFillSmallGCUpdate(&slow, idx, 0);
    CHECK_EQ(mallctl->base, 1);

    /// Small `num_cached_max`: 2 >> 1 == 1, so the base never grows.
    threadCacheNumFillSmallGCUpdate(&slow, idx, 2);
    CHECK_EQ(mallctl->base, 1);
    threadCacheNumFillSmallGCUpdate(&slow, idx, 4);
    CHECK_EQ(mallctl->base, 2);
    threadCacheNumFillSmallGCUpdate(&slow, idx, 4);
    CHECK_EQ(mallctl->base, 2);
}

TEST(ThreadCache, GCItemDelay)
{
    OptionsGuard guard;
    options.thread_cache_gc_delay_bytes = 0;
    CHECK_EQ(threadCacheGCItemDelayCompute(0), 0);
    options.thread_cache_gc_delay_bytes = 1024;
    CHECK_EQ(threadCacheGCItemDelayCompute(0), 128); /// 8-byte class
    CHECK_EQ(threadCacheGCItemDelayCompute(1), 64);
    CHECK_EQ(threadCacheGCItemDelayCompute(size_classes::sizeToIndex(1024)), 1);
    CHECK_EQ(threadCacheGCItemDelayCompute(size_classes::sizeToIndex(2048)), 0);
    options.thread_cache_gc_delay_bytes = 1 << 20;
    CHECK_EQ(threadCacheGCItemDelayCompute(0), 255);
    CHECK_EQ(threadCacheGCItemDelayCompute(size_classes::sizeToIndex(4096)), 255);
    CHECK_EQ(threadCacheGCItemDelayCompute(size_classes::sizeToIndex(8192)), 128);
}

TEST(ThreadCache, GCShuffle)
{
    /// The hand-computed example: (head -> bottom) [R1, L1, R2, L2, L3, R3] becomes [L1, L2, L3, R1, R2, R3].
    const uintptr_t low = 0x100000;
    const uintptr_t high = 0x200000;
    const uintptr_t L1 = low + 0x10;
    const uintptr_t L2 = low + 0x20;
    const uintptr_t L3 = low + 0x30;
    const uintptr_t R1 = high + 0x10;
    const uintptr_t R2 = 0x10;
    const uintptr_t R3 = high + 0x30;
    {
        SyntheticBin synthetic_bin(20);
        synthetic_bin.fillTopFirst({R1, L1, R2, L2, L3, R3});
        threadCacheGCSmallBinShuffle(&synthetic_bin.bin, 3, low, high);
        CHECK(synthetic_bin.contents() == (std::vector<uintptr_t>{L1, L2, L3, R1, R2, R3}));
    }
    {
        /// Remote items only in the top part: the bottom (remote-free) part is swapped up.
        SyntheticBin synthetic_bin(20);
        synthetic_bin.fillTopFirst({R1, R2, L1, L2, L3});
        threadCacheGCSmallBinShuffle(&synthetic_bin.bin, 2, low, high);
        CHECK(synthetic_bin.contents() == (std::vector<uintptr_t>{L1, L2, L3, R2, R1}));
    }
    {
        /// Exhaustive: every remote/local pattern of 8 items keeps the local items in order on top.
        for (unsigned mask = 1; mask < 255; ++mask)
        {
            std::vector<uintptr_t> items;
            std::vector<uintptr_t> locals;
            unsigned num_remote = 0;
            for (unsigned i = 0; i < 8; ++i)
            {
                if (mask & (1u << i))
                {
                    items.push_back(high + 0x1000 * (i + 1));
                    ++num_remote;
                }
                else
                {
                    items.push_back(low + 0x100 * (i + 1));
                    locals.push_back(items.back());
                }
            }
            SyntheticBin synthetic_bin(20);
            synthetic_bin.fillTopFirst(items);
            threadCacheGCSmallBinShuffle(&synthetic_bin.bin, static_cast<CacheBinSize>(num_remote), low, high);
            auto result = synthetic_bin.contents();
            CHECK(std::vector<uintptr_t>(result.begin(), result.begin() + long(locals.size())) == locals);
            for (size_t i = locals.size(); i < result.size(); ++i)
                CHECK(result[i] >= high);
        }
    }
}

TEST(ThreadCache, GCNumRemote)
{
    const SizeClassIdx size_class_idx = 0;
    const size_t slab_size = bin_infos[size_class_idx].slab_size;
    const uintptr_t addr = uintptr_t(1) << 32;
    const uintptr_t two_mib = uintptr_t(2) << 20;

    SyntheticBin synthetic_bin(200);
    /// 3 in the slab, 2 in the neighborhood (outside the slab), 4 far away.
    std::vector<uintptr_t> items
        = {addr,
           addr + slab_size - 8,
           addr + 16,
           addr + slab_size,
           addr - 8,
           addr + two_mib,
           addr - two_mib - 8,
           0x1000,
           addr + (uintptr_t(1) << 30)};
    synthetic_bin.fillTopFirst(items);

    uintptr_t min;
    uintptr_t max;
    /// nflush <= number of far pointers: keep the neighborhood.
    CHECK_EQ(unsigned(threadCacheGCSmallNumRemoteGet(&synthetic_bin.bin, reinterpret_cast<void *>(addr), min, max, size_class_idx, 4)), 4u);
    CHECK_EQ(min, addr - two_mib);
    CHECK_EQ(max, addr + two_mib);
    CHECK_EQ(unsigned(threadCacheGCSmallNumRemoteGet(&synthetic_bin.bin, reinterpret_cast<void *>(addr), min, max, size_class_idx, 0)), 4u);
    /// More to flush than far pointers: keep only the slab.
    CHECK_EQ(unsigned(threadCacheGCSmallNumRemoteGet(&synthetic_bin.bin, reinterpret_cast<void *>(addr), min, max, size_class_idx, 5)), 6u);
    CHECK_EQ(min, addr);
    CHECK_EQ(max, addr + slab_size);

    /// Near the bottom of the address space the neighborhood starts at 0.
    CHECK_EQ(
        unsigned(
            threadCacheGCSmallNumRemoteGet(&synthetic_bin.bin, reinterpret_cast<void *>(uintptr_t(0x100000)), min, max, size_class_idx, 0)),
        8u);
    CHECK_EQ(min, uintptr_t(0));
    CHECK_EQ(max, uintptr_t(0x100000) + two_mib);

    /// Remote checks are half-open intervals.
    CHECK(!threadCacheGCIsAddrRemote(reinterpret_cast<void *>(uintptr_t(10)), 10, 20));
    CHECK(threadCacheGCIsAddrRemote(reinterpret_cast<void *>(uintptr_t(20)), 10, 20));
    CHECK(threadCacheGCIsAddrRemote(reinterpret_cast<void *>(uintptr_t(9)), 10, 20));
}
