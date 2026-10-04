/// Port of jemalloc's `test/unit/cache_bin.c`, plus checks of the disabled bin, the stack layout and the racy counts.

#include <allocator/CacheBin.h>

#include "Test.h"

#include <cstdlib>
#include <vector>

using namespace jemalloc;

namespace
{

void doFillTest(CacheBin & bin, void ** ptrs, cache_bin_sz_t nfill_attempt, cache_bin_sz_t nfill_succeed)
{
    bool success;
    void * ptr;
    REQUIRE(bin.ncachedGetLocal() == 0);
    CacheBinPtrArray arr(nfill_attempt);
    bin.initPtrArrayForFill(arr, nfill_attempt);
    for (cache_bin_sz_t i = 0; i < nfill_succeed; ++i)
        arr.ptr[i] = &ptrs[i];
    bin.finishFill(arr, nfill_succeed);
    CHECK_EQ(bin.ncachedGetLocal(), nfill_succeed);
    bin.lowWaterSet();

    for (cache_bin_sz_t i = 0; i < nfill_succeed; ++i)
    {
        ptr = bin.alloc(success);
        CHECK(success);
        CHECK_EQ(ptr, static_cast<void *>(&ptrs[i])); /// Should pop in order filled.
        CHECK_EQ(bin.lowWaterGet(), nfill_succeed - i - 1);
    }
    CHECK_EQ(bin.ncachedGetLocal(), 0);
    CHECK_EQ(bin.lowWaterGet(), 0);
}

void doFlushTest(CacheBin & bin, void ** ptrs, cache_bin_sz_t nfill, cache_bin_sz_t nflush)
{
    bool success;
    REQUIRE(bin.ncachedGetLocal() == 0);

    for (cache_bin_sz_t i = 0; i < nfill; ++i)
    {
        success = bin.dallocEasy(&ptrs[i]);
        CHECK(success);
    }

    CacheBinPtrArray arr(nflush);
    bin.initPtrArrayForFlush(arr, nflush);
    for (cache_bin_sz_t i = 0; i < nflush; ++i)
        CHECK_EQ(arr.ptr[i], static_cast<void *>(&ptrs[nflush - i - 1]));
    bin.finishFlush(arr, nflush);

    CHECK_EQ(bin.ncachedGetLocal(), nfill - nflush);
    while (bin.ncachedGetLocal() > 0)
        bin.alloc(success);
}

void doBatchAllocTest(CacheBin & bin, void ** ptrs, cache_bin_sz_t nfill, size_t batch)
{
    REQUIRE(bin.ncachedGetLocal() == 0);
    CacheBinPtrArray arr(nfill);
    bin.initPtrArrayForFill(arr, nfill);
    for (cache_bin_sz_t i = 0; i < nfill; ++i)
        arr.ptr[i] = &ptrs[i];
    bin.finishFill(arr, nfill);
    REQUIRE(bin.ncachedGetLocal() == nfill);
    bin.lowWaterSet();

    std::vector<void *> out(batch + 1);
    size_t n = bin.allocBatch(batch, out.data());
    CHECK_EQ(n, size_t(nfill) < batch ? size_t(nfill) : batch);
    for (cache_bin_sz_t i = 0; i < cache_bin_sz_t(n); ++i)
        CHECK_EQ(out[i], static_cast<void *>(&ptrs[i]));
    CHECK_EQ(bin.lowWaterGet(), nfill - cache_bin_sz_t(n));
    while (bin.ncachedGetLocal() > 0)
    {
        bool success;
        bin.alloc(success);
    }
}

/// Allocates a stack for one bin (leaked: the tests are short).
void testBinInit(CacheBin & bin, const CacheBinInfo & info)
{
    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(&info, 1, size, alignment);
    CHECK_EQ(alignment, PAGE);
    CHECK_EQ(size, 16 + 8 * size_t(info.ncached_max));
    void * mem = std::aligned_alloc(alignment, alignmentCeiling(size, alignment));
    REQUIRE(mem != nullptr);

    size_t cur_offset = 0;
    cacheBinPreincrement(&info, 1, mem, cur_offset);
    bin.init(info, mem, cur_offset);
    cacheBinPostincrement(mem, cur_offset);
    CHECK_EQ(cur_offset, size); /// Should use all requested memory.
    CHECK_EQ(static_cast<uintptr_t *>(mem)[0], cache_bin_preceding_junk);
    CHECK_EQ(static_cast<uintptr_t *>(mem)[size / 8 - 1], cache_bin_trailing_junk);
}

void doFlushStashedTest(CacheBin & bin, void ** ptrs, cache_bin_sz_t nfill, cache_bin_sz_t nstash)
{
    CHECK_EQ(bin.ncachedGetLocal(), 0); /// Bin not empty.
    CHECK_EQ(bin.nstashedGetLocal(), 0);
    CHECK(nfill + nstash <= bin.bin_info.ncached_max);

    bool ret;
    /// Fill.
    for (cache_bin_sz_t i = 0; i < nfill; ++i)
    {
        ret = bin.dallocEasy(&ptrs[i]);
        CHECK(ret);
    }
    CHECK_EQ(bin.ncachedGetLocal(), nfill);

    /// Stash.
    for (cache_bin_sz_t i = 0; i < nstash; ++i)
    {
        ret = bin.stash(&ptrs[i + nfill]);
        CHECK(ret);
    }
    CHECK_EQ(bin.nstashedGetLocal(), nstash);

    if (nfill + nstash == bin.bin_info.ncached_max)
    {
        ret = bin.dallocEasy(&ptrs[0]);
        CHECK(!ret); /// Should not dalloc into a full bin.
        ret = bin.stash(&ptrs[0]);
        CHECK(!ret); /// Should not stash into a full bin.
    }

    /// Alloc filled ones.
    for (cache_bin_sz_t i = 0; i < nfill; ++i)
    {
        void * ptr = bin.alloc(ret);
        CHECK(ret);
        /// Verify it's not from the stashed range.
        CHECK(reinterpret_cast<uintptr_t>(ptr) < reinterpret_cast<uintptr_t>(&ptrs[nfill]));
    }
    CHECK_EQ(bin.ncachedGetLocal(), 0);
    CHECK_EQ(bin.nstashedGetLocal(), nstash);

    bin.alloc(ret);
    CHECK(!ret); /// Should not alloc stashed.

    /// Clear stashed ones.
    bin.finishFlushStashed();
    CHECK_EQ(bin.ncachedGetLocal(), 0);
    CHECK_EQ(bin.nstashedGetLocal(), 0);

    bin.alloc(ret);
    CHECK(!ret); /// Should not alloc from empty bin.
}

}

TEST(CacheBin, Basic)
{
    const int ncached_max = 100;
    bool success;
    void * ptr;

    CacheBinInfo info;
    info.init(ncached_max);
    CacheBin bin;
    CHECK(bin.stillZeroInitialized());
    testBinInit(bin, info);
    CHECK(!bin.stillZeroInitialized());
    CHECK(!bin.disabled());

    /// Initialize to empty; should then have 0 elements.
    CHECK_EQ(int(bin.ncachedMaxGet()), ncached_max);
    CHECK_EQ(bin.ncachedGetLocal(), 0);
    CHECK_EQ(bin.lowWaterGet(), 0);

    ptr = bin.allocEasy(success);
    CHECK(!success); /// Shouldn't successfully allocate when empty.
    CHECK(ptr == nullptr);

    ptr = bin.alloc(success);
    CHECK(!success);
    CHECK(ptr == nullptr);

    /// We allocate one more item than ncached_max, so we can test cache bin exhaustion.
    std::vector<void *> ptrs_storage(ncached_max + 1);
    void ** ptrs = ptrs_storage.data();
    for (cache_bin_sz_t i = 0; i < ncached_max; ++i)
    {
        CHECK_EQ(bin.ncachedGetLocal(), i);
        success = bin.dallocEasy(&ptrs[i]);
        CHECK(success); /// Should be able to dalloc into a non-full cache bin.
        CHECK_EQ(bin.lowWaterGet(), 0); /// Pushes and pops shouldn't change low water of zero.
    }
    CHECK_EQ(int(bin.ncachedGetLocal()), ncached_max);
    success = bin.dallocEasy(&ptrs[ncached_max]);
    CHECK(!success); /// Shouldn't be able to dalloc into a full bin.

    bin.lowWaterSet();

    for (cache_bin_sz_t i = 0; i < ncached_max; ++i)
    {
        CHECK_EQ(bin.lowWaterGet(), ncached_max - i);
        CHECK_EQ(bin.ncachedGetLocal(), ncached_max - i);
        /// This should fail -- the easy variant can't change the low water mark.
        ptr = bin.allocEasy(success);
        CHECK(ptr == nullptr);
        CHECK(!success);
        CHECK_EQ(bin.lowWaterGet(), ncached_max - i);
        CHECK_EQ(bin.ncachedGetLocal(), ncached_max - i);

        /// This should succeed, though.
        ptr = bin.alloc(success);
        CHECK(success);
        CHECK_EQ(ptr, static_cast<void *>(&ptrs[ncached_max - i - 1])); /// Alloc should pop in stack order.
        CHECK_EQ(bin.lowWaterGet(), ncached_max - i - 1);
        CHECK_EQ(bin.ncachedGetLocal(), ncached_max - i - 1);
    }
    /// Now we're empty -- all alloc attempts should fail.
    CHECK_EQ(bin.ncachedGetLocal(), 0);
    ptr = bin.allocEasy(success);
    CHECK(ptr == nullptr);
    CHECK(!success);
    ptr = bin.alloc(success);
    CHECK(ptr == nullptr);
    CHECK(!success);

    for (cache_bin_sz_t i = 0; i < ncached_max / 2; ++i)
        bin.dallocEasy(&ptrs[i]);
    bin.lowWaterSet();

    for (cache_bin_sz_t i = ncached_max / 2; i < ncached_max; ++i)
        bin.dallocEasy(&ptrs[i]);
    CHECK_EQ(int(bin.ncachedGetLocal()), ncached_max);
    for (cache_bin_sz_t i = ncached_max - 1; i >= ncached_max / 2; --i)
    {
        /// Size is bigger than low water -- the reduced version should succeed.
        ptr = bin.allocEasy(success);
        CHECK(success);
        CHECK_EQ(ptr, static_cast<void *>(&ptrs[i]));
    }
    /// But now, we've hit low-water.
    ptr = bin.allocEasy(success);
    CHECK(!success);
    CHECK(ptr == nullptr);

    /// We're going to test filling -- we must be empty to start.
    while (bin.ncachedGetLocal())
    {
        bin.alloc(success);
        CHECK(success);
    }

    /// Test fill.
    /// Try to fill all, succeed fully.
    doFillTest(bin, ptrs, ncached_max, ncached_max);
    /// Try to fill all, succeed partially.
    doFillTest(bin, ptrs, ncached_max, ncached_max / 2);
    /// Try to fill all, fail completely.
    doFillTest(bin, ptrs, ncached_max, 0);

    /// Try to fill some, succeed fully.
    doFillTest(bin, ptrs, ncached_max / 2, ncached_max / 2);
    /// Try to fill some, succeed partially.
    doFillTest(bin, ptrs, ncached_max / 2, ncached_max / 4);
    /// Try to fill some, fail completely.
    doFillTest(bin, ptrs, ncached_max / 2, 0);

    doFlushTest(bin, ptrs, ncached_max, ncached_max);
    doFlushTest(bin, ptrs, ncached_max, ncached_max / 2);
    doFlushTest(bin, ptrs, ncached_max, 0);
    doFlushTest(bin, ptrs, ncached_max / 2, ncached_max / 2);
    doFlushTest(bin, ptrs, ncached_max / 2, ncached_max / 4);
    doFlushTest(bin, ptrs, ncached_max / 2, 0);

    doBatchAllocTest(bin, ptrs, ncached_max, ncached_max);
    doBatchAllocTest(bin, ptrs, ncached_max, ncached_max * 2);
    doBatchAllocTest(bin, ptrs, ncached_max, ncached_max / 2);
    doBatchAllocTest(bin, ptrs, ncached_max, 2);
    doBatchAllocTest(bin, ptrs, ncached_max, 1);
    doBatchAllocTest(bin, ptrs, ncached_max, 0);
    doBatchAllocTest(bin, ptrs, ncached_max / 2, ncached_max / 2);
    doBatchAllocTest(bin, ptrs, ncached_max / 2, ncached_max);
    doBatchAllocTest(bin, ptrs, ncached_max / 2, ncached_max / 4);
    doBatchAllocTest(bin, ptrs, ncached_max / 2, 2);
    doBatchAllocTest(bin, ptrs, ncached_max / 2, 1);
    doBatchAllocTest(bin, ptrs, ncached_max / 2, 0);
    doBatchAllocTest(bin, ptrs, 2, ncached_max);
    doBatchAllocTest(bin, ptrs, 2, 2);
    doBatchAllocTest(bin, ptrs, 2, 1);
    doBatchAllocTest(bin, ptrs, 2, 0);
    doBatchAllocTest(bin, ptrs, 1, 2);
    doBatchAllocTest(bin, ptrs, 1, 1);
    doBatchAllocTest(bin, ptrs, 1, 0);
    doBatchAllocTest(bin, ptrs, 0, 2);
    doBatchAllocTest(bin, ptrs, 0, 1);
    doBatchAllocTest(bin, ptrs, 0, 0);
}

TEST(CacheBin, Stash)
{
    const int ncached_max = 100;

    CacheBin bin;
    CacheBinInfo info;
    info.init(ncached_max);
    testBinInit(bin, info);

    /// The content of this array is not accessed; instead the interior addresses are used to insert / stash into the
    /// bins as test pointers.
    std::vector<void *> ptrs_storage(ncached_max + 1);
    void ** ptrs = ptrs_storage.data();
    bool ret;
    for (cache_bin_sz_t i = 0; i < ncached_max; ++i)
    {
        CHECK_EQ(bin.ncachedGetLocal(), i / 2 + i % 2);
        CHECK_EQ(bin.nstashedGetLocal(), i / 2);
        cache_bin_sz_t ncached_remote;
        cache_bin_sz_t nstashed_remote;
        bin.nitemsGetRemote(ncached_remote, nstashed_remote);
        CHECK_EQ(ncached_remote, i / 2 + i % 2);
        CHECK_EQ(nstashed_remote, i / 2);
        if (i % 2 == 0)
        {
            bin.dallocEasy(&ptrs[i]);
        }
        else
        {
            ret = bin.stash(&ptrs[i]);
            CHECK(ret); /// Should be able to stash into a non-full cache bin.
        }
    }
    ret = bin.dallocEasy(&ptrs[0]);
    CHECK(!ret); /// Should not dalloc into a full cache bin.
    ret = bin.stash(&ptrs[0]);
    CHECK(!ret); /// Should not stash into a full cache bin.
    for (cache_bin_sz_t i = 0; i < ncached_max; ++i)
    {
        void * ptr = bin.alloc(ret);
        if (i < ncached_max / 2)
        {
            CHECK(ret); /// Should be able to alloc.
            uintptr_t d = (reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(&ptrs[0])) / sizeof(void *);
            CHECK(d % 2 == 0);
        }
        else
        {
            CHECK(!ret); /// Should not alloc stashed.
            CHECK_EQ(int(bin.nstashedGetLocal()), ncached_max / 2);
        }
    }

    /// The stashed pointers are flushed from the low bound up.
    CacheBinPtrArray arr(bin.nstashedGetLocal());
    bin.initPtrArrayForStashed(0, arr, bin.nstashedGetLocal());
    for (cache_bin_sz_t i = 0; i < ncached_max / 2; ++i)
        CHECK_EQ(arr.ptr[i], static_cast<void *>(&ptrs[2 * i + 1]));

    testBinInit(bin, info);
    doFlushStashedTest(bin, ptrs, ncached_max, 0);
    doFlushStashedTest(bin, ptrs, 0, ncached_max);
    doFlushStashedTest(bin, ptrs, ncached_max / 2, ncached_max / 2);
    doFlushStashedTest(bin, ptrs, ncached_max / 4, ncached_max / 2);
    doFlushStashedTest(bin, ptrs, ncached_max / 2, ncached_max / 4);
    doFlushStashedTest(bin, ptrs, ncached_max / 4, ncached_max / 4);
}

TEST(CacheBin, Disabled)
{
    CacheBin bin;
    bin.initDisabled(42);
    CHECK(bin.disabled());
    CHECK(!bin.stillZeroInitialized());
    CHECK_EQ(static_cast<const void *>(bin.stack_head), CacheBin::disabledBinStack());
    CHECK_EQ(bin.ncachedMaxGetUnsafe(), 42);
    auto low_bits = static_cast<cache_bin_sz_t>(reinterpret_cast<uintptr_t>(&disabled_bin));
    CHECK_EQ(bin.low_bits_low_water, low_bits);
    CHECK_EQ(bin.low_bits_full, low_bits);
    CHECK_EQ(bin.low_bits_empty, low_bits);
    CHECK_EQ(disabled_bin, JUNK_ADDR);

    /// Allocation and deallocation always fail, without touching the bin.
    bool success = true;
    CHECK(bin.allocEasy(success) == nullptr);
    CHECK(!success);
    success = true;
    CHECK(bin.alloc(success) == nullptr);
    CHECK(!success);
    int x;
    CHECK(!bin.dallocEasy(&x));
    CHECK(bin.full());
    CHECK_EQ(static_cast<const void *>(bin.stack_head), CacheBin::disabledBinStack());
}

/// Several bins sharing one stack allocation, as the tcache lays them out (including a zero-sized bin).
TEST(CacheBin, Layout)
{
    CacheBinInfo infos[4];
    infos[0].init(20);
    infos[1].init(0);
    infos[2].init(200);
    infos[3].init(CACHE_BIN_NCACHED_MAX);
    CHECK_EQ(CACHE_BIN_NCACHED_MAX, 8191u);
    CHECK_EQ(CACHE_BIN_NFLUSH_BATCH_MAX, 255u);

    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(infos, 4, size, alignment);
    CHECK_EQ(size, 16 + 8 * (20 + 0 + 200 + CACHE_BIN_NCACHED_MAX));
    CHECK_EQ(alignment, PAGE);
    void * mem = std::aligned_alloc(alignment, alignmentCeiling(size, alignment));
    REQUIRE(mem != nullptr);

    CacheBin bins[4];
    size_t cur_offset = 0;
    cacheBinPreincrement(infos, 4, mem, cur_offset);
    for (int i = 0; i < 4; ++i)
        bins[i].init(infos[i], mem, cur_offset);
    cacheBinPostincrement(mem, cur_offset);
    CHECK_EQ(cur_offset, size);

    auto * base = static_cast<std::byte *>(mem);
    CHECK_EQ(reinterpret_cast<std::byte *>(bins[0].stack_head), base + 8 + 20 * 8);
    CHECK_EQ(bins[1].stack_head, bins[0].stack_head);
    CHECK(bins[1].full());
    CHECK_EQ(reinterpret_cast<std::byte *>(bins[2].stack_head), base + 8 + 220 * 8);
    CHECK_EQ(reinterpret_cast<std::byte *>(bins[3].stack_head), base + size - 8);
    CHECK_EQ(*bins[3].stack_head, reinterpret_cast<void *>(cache_bin_trailing_junk));

    /// The biggest bin crosses a 64 KiB boundary of the low bits; fill it completely.
    std::vector<void *> ptrs(CACHE_BIN_NCACHED_MAX);
    for (size_t i = 0; i < CACHE_BIN_NCACHED_MAX; ++i)
    {
        CHECK(bins[3].dallocEasy(&ptrs[i]));
        CHECK_EQ(size_t(bins[3].ncachedGetLocal()), i + 1);
    }
    CHECK(bins[3].full());
    CHECK(!bins[3].dallocEasy(&ptrs[0]));
    bins[3].lowWaterSet();
    CHECK_EQ(size_t(bins[3].lowWaterGet()), CACHE_BIN_NCACHED_MAX);
    CacheBinPtrArray arr(100);
    bins[3].initPtrArrayForFlush(arr, 100);
    for (size_t i = 0; i < 100; ++i)
        CHECK_EQ(arr.ptr[i], static_cast<void *>(&ptrs[99 - i]));
    bins[3].finishFlush(arr, 100);
    CHECK_EQ(size_t(bins[3].ncachedGetLocal()), CACHE_BIN_NCACHED_MAX - 100);
    CHECK_EQ(size_t(bins[3].lowWaterGet()), CACHE_BIN_NCACHED_MAX - 100);
    bool success;
    CHECK_EQ(bins[3].alloc(success), static_cast<void *>(&ptrs[CACHE_BIN_NCACHED_MAX - 1]));
    CHECK(success);
    std::free(mem);
}
