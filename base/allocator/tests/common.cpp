#include <allocator/Common.h>

#include "Test.h"

using namespace jemalloc;

TEST(Common, BitUtils)
{
    CHECK_EQ(log2Floor(1), 0u);
    CHECK_EQ(log2Floor(4097), 12u);
    CHECK_EQ(log2Ceil(4097), 13u);
    CHECK_EQ(log2Ceil(4096), 12u);
    CHECK_EQ(pow2Ceil(0), 0u);
    CHECK_EQ(pow2Ceil(1), 1u);
    CHECK_EQ(pow2Ceil(3), 4u);
    CHECK_EQ(pow2Ceil(4096), 4096u);
    CHECK_EQ(findFirstSet(uint64_t(8)), 3u);
    uint64_t x = 0b1010;
    CHECK_EQ(clearFirstSet(x), 1u);
    CHECK_EQ(x, 0b1000u);
    static_assert(log2CeilConst(232) == 8);
}

TEST(Common, Flags)
{
    CHECK_EQ(alignmentFromFlags(4), 16u);
    CHECK_EQ(alignmentFromFlags(0), 0u);
    CHECK_EQ(arenaFromFlags((5 + 1) << 20), 5u);
    CHECK_EQ(threadCacheFromFlags(MALLOCX_THREAD_CACHE_NONE_FLAG), unsigned(-1));
}
