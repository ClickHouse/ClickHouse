/// Port of jemalloc's `test/unit/cache_bin.c`, plus checks of the disabled bin, the stack layout and the racy counts.

#include <allocator/CacheBin.h>

#include "Test.h"

#include <cstdlib>
#include <vector>

using namespace jemalloc;

namespace
{

void doFillTest(CacheBin & bin, void ** ptrs, CacheBinSize num_fill_attempt, CacheBinSize num_fill_succeed)
{
    bool success;
    void * ptr;
    REQUIRE(bin.numCachedGetLocal() == 0);
    CacheBinPtrArray array(num_fill_attempt);
    bin.initPtrArrayForFill(array, num_fill_attempt);
    for (CacheBinSize i = 0; i < num_fill_succeed; ++i)
        array.ptr[i] = &ptrs[i];
    bin.finishFill(array, num_fill_succeed);
    CHECK_EQ(bin.numCachedGetLocal(), num_fill_succeed);
    bin.lowWaterSet();

    for (CacheBinSize i = 0; i < num_fill_succeed; ++i)
    {
        ptr = bin.alloc(success);
        CHECK(success);
        CHECK_EQ(ptr, static_cast<void *>(&ptrs[i])); /// Should pop in order filled.
        CHECK_EQ(bin.lowWaterGet(), num_fill_succeed - i - 1);
    }
    CHECK_EQ(bin.numCachedGetLocal(), 0);
    CHECK_EQ(bin.lowWaterGet(), 0);
}

void doFlushTest(CacheBin & bin, void ** ptrs, CacheBinSize num_fill, CacheBinSize num_flush)
{
    bool success;
    REQUIRE(bin.numCachedGetLocal() == 0);

    for (CacheBinSize i = 0; i < num_fill; ++i)
    {
        success = bin.deallocateEasy(&ptrs[i]);
        CHECK(success);
    }

    CacheBinPtrArray array(num_flush);
    bin.initPtrArrayForFlush(array, num_flush);
    for (CacheBinSize i = 0; i < num_flush; ++i)
        CHECK_EQ(array.ptr[i], static_cast<void *>(&ptrs[num_flush - i - 1]));
    bin.finishFlush(array, num_flush);

    CHECK_EQ(bin.numCachedGetLocal(), num_fill - num_flush);
    while (bin.numCachedGetLocal() > 0)
        bin.alloc(success);
}

void doBatchAllocTest(CacheBin & bin, void ** ptrs, CacheBinSize num_fill, size_t batch)
{
    REQUIRE(bin.numCachedGetLocal() == 0);
    CacheBinPtrArray array(num_fill);
    bin.initPtrArrayForFill(array, num_fill);
    for (CacheBinSize i = 0; i < num_fill; ++i)
        array.ptr[i] = &ptrs[i];
    bin.finishFill(array, num_fill);
    REQUIRE(bin.numCachedGetLocal() == num_fill);
    bin.lowWaterSet();

    std::vector<void *> out(batch + 1);
    size_t n = bin.allocBatch(batch, out.data());
    CHECK_EQ(n, size_t(num_fill) < batch ? size_t(num_fill) : batch);
    for (CacheBinSize i = 0; i < CacheBinSize(n); ++i)
        CHECK_EQ(out[i], static_cast<void *>(&ptrs[i]));
    CHECK_EQ(bin.lowWaterGet(), num_fill - CacheBinSize(n));
    while (bin.numCachedGetLocal() > 0)
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
    CHECK_EQ(size, 16 + 8 * size_t(info.num_cached_max));
    void * memory = std::aligned_alloc(alignment, alignmentCeiling(size, alignment));
    REQUIRE(memory != nullptr);

    size_t current_offset = 0;
    cacheBinPreincrement(&info, 1, memory, current_offset);
    bin.init(info, memory, current_offset);
    cacheBinPostincrement(memory, current_offset);
    CHECK_EQ(current_offset, size); /// Should use all requested memory.
    CHECK_EQ(static_cast<uintptr_t *>(memory)[0], cache_bin_preceding_junk);
    CHECK_EQ(static_cast<uintptr_t *>(memory)[size / 8 - 1], cache_bin_trailing_junk);
}

void doFlushStashedTest(CacheBin & bin, void ** ptrs, CacheBinSize num_fill, CacheBinSize num_stash)
{
    CHECK_EQ(bin.numCachedGetLocal(), 0); /// Bin not empty.
    CHECK_EQ(bin.numStashedGetLocal(), 0);
    CHECK(num_fill + num_stash <= bin.bin_info.num_cached_max);

    bool result;
    /// Fill.
    for (CacheBinSize i = 0; i < num_fill; ++i)
    {
        result = bin.deallocateEasy(&ptrs[i]);
        CHECK(result);
    }
    CHECK_EQ(bin.numCachedGetLocal(), num_fill);

    /// Stash.
    for (CacheBinSize i = 0; i < num_stash; ++i)
    {
        result = bin.stash(&ptrs[i + num_fill]);
        CHECK(result);
    }
    CHECK_EQ(bin.numStashedGetLocal(), num_stash);

    if (num_fill + num_stash == bin.bin_info.num_cached_max)
    {
        result = bin.deallocateEasy(&ptrs[0]);
        CHECK(!result); /// Should not dalloc into a full bin.
        result = bin.stash(&ptrs[0]);
        CHECK(!result); /// Should not stash into a full bin.
    }

    /// Alloc filled ones.
    for (CacheBinSize i = 0; i < num_fill; ++i)
    {
        void * ptr = bin.alloc(result);
        CHECK(result);
        /// Verify it's not from the stashed range.
        CHECK(reinterpret_cast<uintptr_t>(ptr) < reinterpret_cast<uintptr_t>(&ptrs[num_fill]));
    }
    CHECK_EQ(bin.numCachedGetLocal(), 0);
    CHECK_EQ(bin.numStashedGetLocal(), num_stash);

    bin.alloc(result);
    CHECK(!result); /// Should not alloc stashed.

    /// Clear stashed ones.
    bin.finishFlushStashed();
    CHECK_EQ(bin.numCachedGetLocal(), 0);
    CHECK_EQ(bin.numStashedGetLocal(), 0);

    bin.alloc(result);
    CHECK(!result); /// Should not alloc from empty bin.
}

}

TEST(CacheBin, Basic)
{
    const int num_cached_max = 100;
    bool success;
    void * ptr;

    CacheBinInfo info;
    info.init(num_cached_max);
    CacheBin bin;
    CHECK(bin.stillZeroInitialized());
    testBinInit(bin, info);
    CHECK(!bin.stillZeroInitialized());
    CHECK(!bin.disabled());

    /// Initialize to empty; should then have 0 elements.
    CHECK_EQ(int(bin.numCachedMaxGet()), num_cached_max);
    CHECK_EQ(bin.numCachedGetLocal(), 0);
    CHECK_EQ(bin.lowWaterGet(), 0);

    ptr = bin.allocEasy(success);
    CHECK(!success); /// Shouldn't successfully allocate when empty.
    CHECK(ptr == nullptr);

    ptr = bin.alloc(success);
    CHECK(!success);
    CHECK(ptr == nullptr);

    /// We allocate one more item than ncached_max, so we can test cache bin exhaustion.
    std::vector<void *> ptrs_storage(num_cached_max + 1);
    void ** ptrs = ptrs_storage.data();
    for (CacheBinSize i = 0; i < num_cached_max; ++i)
    {
        CHECK_EQ(bin.numCachedGetLocal(), i);
        success = bin.deallocateEasy(&ptrs[i]);
        CHECK(success); /// Should be able to dalloc into a non-full cache bin.
        CHECK_EQ(bin.lowWaterGet(), 0); /// Pushes and pops shouldn't change low water of zero.
    }
    CHECK_EQ(int(bin.numCachedGetLocal()), num_cached_max);
    success = bin.deallocateEasy(&ptrs[num_cached_max]);
    CHECK(!success); /// Shouldn't be able to dalloc into a full bin.

    bin.lowWaterSet();

    for (CacheBinSize i = 0; i < num_cached_max; ++i)
    {
        CHECK_EQ(bin.lowWaterGet(), num_cached_max - i);
        CHECK_EQ(bin.numCachedGetLocal(), num_cached_max - i);
        /// This should fail -- the easy variant can't change the low water mark.
        ptr = bin.allocEasy(success);
        CHECK(ptr == nullptr);
        CHECK(!success);
        CHECK_EQ(bin.lowWaterGet(), num_cached_max - i);
        CHECK_EQ(bin.numCachedGetLocal(), num_cached_max - i);

        /// This should succeed, though.
        ptr = bin.alloc(success);
        CHECK(success);
        CHECK_EQ(ptr, static_cast<void *>(&ptrs[num_cached_max - i - 1])); /// Alloc should pop in stack order.
        CHECK_EQ(bin.lowWaterGet(), num_cached_max - i - 1);
        CHECK_EQ(bin.numCachedGetLocal(), num_cached_max - i - 1);
    }
    /// Now we're empty -- all alloc attempts should fail.
    CHECK_EQ(bin.numCachedGetLocal(), 0);
    ptr = bin.allocEasy(success);
    CHECK(ptr == nullptr);
    CHECK(!success);
    ptr = bin.alloc(success);
    CHECK(ptr == nullptr);
    CHECK(!success);

    for (CacheBinSize i = 0; i < num_cached_max / 2; ++i)
        bin.deallocateEasy(&ptrs[i]);
    bin.lowWaterSet();

    for (CacheBinSize i = num_cached_max / 2; i < num_cached_max; ++i)
        bin.deallocateEasy(&ptrs[i]);
    CHECK_EQ(int(bin.numCachedGetLocal()), num_cached_max);
    for (CacheBinSize i = num_cached_max - 1; i >= num_cached_max / 2; --i)
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
    while (bin.numCachedGetLocal())
    {
        bin.alloc(success);
        CHECK(success);
    }

    /// Test fill.
    /// Try to fill all, succeed fully.
    doFillTest(bin, ptrs, num_cached_max, num_cached_max);
    /// Try to fill all, succeed partially.
    doFillTest(bin, ptrs, num_cached_max, num_cached_max / 2);
    /// Try to fill all, fail completely.
    doFillTest(bin, ptrs, num_cached_max, 0);

    /// Try to fill some, succeed fully.
    doFillTest(bin, ptrs, num_cached_max / 2, num_cached_max / 2);
    /// Try to fill some, succeed partially.
    doFillTest(bin, ptrs, num_cached_max / 2, num_cached_max / 4);
    /// Try to fill some, fail completely.
    doFillTest(bin, ptrs, num_cached_max / 2, 0);

    doFlushTest(bin, ptrs, num_cached_max, num_cached_max);
    doFlushTest(bin, ptrs, num_cached_max, num_cached_max / 2);
    doFlushTest(bin, ptrs, num_cached_max, 0);
    doFlushTest(bin, ptrs, num_cached_max / 2, num_cached_max / 2);
    doFlushTest(bin, ptrs, num_cached_max / 2, num_cached_max / 4);
    doFlushTest(bin, ptrs, num_cached_max / 2, 0);

    doBatchAllocTest(bin, ptrs, num_cached_max, num_cached_max);
    doBatchAllocTest(bin, ptrs, num_cached_max, num_cached_max * 2);
    doBatchAllocTest(bin, ptrs, num_cached_max, num_cached_max / 2);
    doBatchAllocTest(bin, ptrs, num_cached_max, 2);
    doBatchAllocTest(bin, ptrs, num_cached_max, 1);
    doBatchAllocTest(bin, ptrs, num_cached_max, 0);
    doBatchAllocTest(bin, ptrs, num_cached_max / 2, num_cached_max / 2);
    doBatchAllocTest(bin, ptrs, num_cached_max / 2, num_cached_max);
    doBatchAllocTest(bin, ptrs, num_cached_max / 2, num_cached_max / 4);
    doBatchAllocTest(bin, ptrs, num_cached_max / 2, 2);
    doBatchAllocTest(bin, ptrs, num_cached_max / 2, 1);
    doBatchAllocTest(bin, ptrs, num_cached_max / 2, 0);
    doBatchAllocTest(bin, ptrs, 2, num_cached_max);
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
    const int num_cached_max = 100;

    CacheBin bin;
    CacheBinInfo info;
    info.init(num_cached_max);
    testBinInit(bin, info);

    /// The content of this array is not accessed; instead the interior addresses are used to insert / stash into the
    /// bins as test pointers.
    std::vector<void *> ptrs_storage(num_cached_max + 1);
    void ** ptrs = ptrs_storage.data();
    bool result;
    for (CacheBinSize i = 0; i < num_cached_max; ++i)
    {
        CHECK_EQ(bin.numCachedGetLocal(), i / 2 + i % 2);
        CHECK_EQ(bin.numStashedGetLocal(), i / 2);
        CacheBinSize num_cached_remote;
        CacheBinSize num_stashed_remote;
        bin.numItemsGetRemote(num_cached_remote, num_stashed_remote);
        CHECK_EQ(num_cached_remote, i / 2 + i % 2);
        CHECK_EQ(num_stashed_remote, i / 2);
        if (i % 2 == 0)
        {
            bin.deallocateEasy(&ptrs[i]);
        }
        else
        {
            result = bin.stash(&ptrs[i]);
            CHECK(result); /// Should be able to stash into a non-full cache bin.
        }
    }
    result = bin.deallocateEasy(&ptrs[0]);
    CHECK(!result); /// Should not dalloc into a full cache bin.
    result = bin.stash(&ptrs[0]);
    CHECK(!result); /// Should not stash into a full cache bin.
    for (CacheBinSize i = 0; i < num_cached_max; ++i)
    {
        void * ptr = bin.alloc(result);
        if (i < num_cached_max / 2)
        {
            CHECK(result); /// Should be able to alloc.
            uintptr_t d = (reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(&ptrs[0])) / sizeof(void *);
            CHECK(d % 2 == 0);
        }
        else
        {
            CHECK(!result); /// Should not alloc stashed.
            CHECK_EQ(int(bin.numStashedGetLocal()), num_cached_max / 2);
        }
    }

    /// The stashed pointers are flushed from the low bound up.
    CacheBinPtrArray array(bin.numStashedGetLocal());
    bin.initPtrArrayForStashed(0, array, bin.numStashedGetLocal());
    for (CacheBinSize i = 0; i < num_cached_max / 2; ++i)
        CHECK_EQ(array.ptr[i], static_cast<void *>(&ptrs[2 * i + 1]));

    testBinInit(bin, info);
    doFlushStashedTest(bin, ptrs, num_cached_max, 0);
    doFlushStashedTest(bin, ptrs, 0, num_cached_max);
    doFlushStashedTest(bin, ptrs, num_cached_max / 2, num_cached_max / 2);
    doFlushStashedTest(bin, ptrs, num_cached_max / 4, num_cached_max / 2);
    doFlushStashedTest(bin, ptrs, num_cached_max / 2, num_cached_max / 4);
    doFlushStashedTest(bin, ptrs, num_cached_max / 4, num_cached_max / 4);
}

TEST(CacheBin, Disabled)
{
    CacheBin bin;
    bin.initDisabled(42);
    CHECK(bin.disabled());
    CHECK(!bin.stillZeroInitialized());
    CHECK_EQ(static_cast<const void *>(bin.stack_head), CacheBin::disabledBinStack());
    CHECK_EQ(bin.numCachedMaxGetUnsafe(), 42);
    auto low_bits = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(&disabled_bin));
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
    CHECK(!bin.deallocateEasy(&x));
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
    infos[3].init(CACHE_BIN_NUM_CACHED_MAX);
    CHECK_EQ(CACHE_BIN_NUM_CACHED_MAX, 8191u);
    CHECK_EQ(CACHE_BIN_NUM_FLUSH_BATCH_MAX, 255u);

    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(infos, 4, size, alignment);
    CHECK_EQ(size, 16 + 8 * (20 + 0 + 200 + CACHE_BIN_NUM_CACHED_MAX));
    CHECK_EQ(alignment, PAGE);
    void * memory = std::aligned_alloc(alignment, alignmentCeiling(size, alignment));
    REQUIRE(memory != nullptr);

    CacheBin bins[4];
    size_t current_offset = 0;
    cacheBinPreincrement(infos, 4, memory, current_offset);
    for (int i = 0; i < 4; ++i)
        bins[i].init(infos[i], memory, current_offset);
    cacheBinPostincrement(memory, current_offset);
    CHECK_EQ(current_offset, size);

    auto * base = static_cast<std::byte *>(memory);
    CHECK_EQ(reinterpret_cast<std::byte *>(bins[0].stack_head), base + 8 + 20 * 8);
    CHECK_EQ(bins[1].stack_head, bins[0].stack_head);
    CHECK(bins[1].full());
    CHECK_EQ(reinterpret_cast<std::byte *>(bins[2].stack_head), base + 8 + 220 * 8);
    CHECK_EQ(reinterpret_cast<std::byte *>(bins[3].stack_head), base + size - 8);
    CHECK_EQ(*bins[3].stack_head, reinterpret_cast<void *>(cache_bin_trailing_junk));

    /// The biggest bin crosses a 64 KiB boundary of the low bits; fill it completely.
    std::vector<void *> ptrs(CACHE_BIN_NUM_CACHED_MAX);
    for (size_t i = 0; i < CACHE_BIN_NUM_CACHED_MAX; ++i)
    {
        CHECK(bins[3].deallocateEasy(&ptrs[i]));
        CHECK_EQ(size_t(bins[3].numCachedGetLocal()), i + 1);
    }
    CHECK(bins[3].full());
    CHECK(!bins[3].deallocateEasy(&ptrs[0]));
    bins[3].lowWaterSet();
    CHECK_EQ(size_t(bins[3].lowWaterGet()), CACHE_BIN_NUM_CACHED_MAX);
    CacheBinPtrArray array(100);
    bins[3].initPtrArrayForFlush(array, 100);
    for (size_t i = 0; i < 100; ++i)
        CHECK_EQ(array.ptr[i], static_cast<void *>(&ptrs[99 - i]));
    bins[3].finishFlush(array, 100);
    CHECK_EQ(size_t(bins[3].numCachedGetLocal()), CACHE_BIN_NUM_CACHED_MAX - 100);
    CHECK_EQ(size_t(bins[3].lowWaterGet()), CACHE_BIN_NUM_CACHED_MAX - 100);
    bool success;
    CHECK_EQ(bins[3].alloc(success), static_cast<void *>(&ptrs[CACHE_BIN_NUM_CACHED_MAX - 1]));
    CHECK(success);
    std::free(memory);
}
