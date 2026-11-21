/// Compares `ExponentialGrow` with jemalloc's `exp_grow_t`.

#include <allocator/ExponentialGrow.h>

#include "Test.h"

#include <random>

extern "C" {
void ref_exponential_grow_boot();
void ref_exponential_grow_init(unsigned * next, unsigned * limit);
bool ref_exponential_grow_size_prepare(
    unsigned next, unsigned limit, size_t alloc_size_min, size_t * result_alloc_size, unsigned * result_skip);
unsigned ref_exponential_grow_size_commit(unsigned next, unsigned limit, unsigned skip);
}

using namespace jemalloc;

TEST(ExponentialGrowOracle, Init)
{
    ref_exponential_grow_boot();
    unsigned next;
    unsigned limit;
    ref_exponential_grow_init(&next, &limit);
    ExponentialGrow exponential_grow;
    exponential_grow.init();
    CHECK_EQ(exponential_grow.next, next);
    CHECK_EQ(exponential_grow.limit, limit);
    CHECK_EQ(exponential_grow.limit, SIZE_CLASS_NUM_PAGE_SIZES - 1);
    if constexpr (LOG2_PAGE == 16)
        CHECK_EQ(exponential_grow.next, 15u);
}

TEST(ExponentialGrowOracle, PrepareCommit)
{
    std::mt19937_64 rng(1);
    for (PageSizeClassIdx next = 0; next < SIZE_CLASS_NUM_PAGE_SIZES; ++next)
    {
        for (PageSizeClassIdx limit :
             {PageSizeClassIdx(0),
              next,
              PageSizeClassIdx(next + 1),
              PageSizeClassIdx(SIZE_CLASS_NUM_PAGE_SIZES / 2),
              PageSizeClassIdx(SIZE_CLASS_NUM_PAGE_SIZES - 1)})
        {
            ExponentialGrow exponential_grow{next, limit};
            for (int i = 0; i < 64; ++i)
            {
                size_t alloc_size_min;
                switch (i % 4)
                {
                    case 0: alloc_size_min = size_t(1) + rng() % (size_t(1) << (rng() % 48)); break;
                    case 1:
                        alloc_size_min = size_classes::pageSizeClassIdxToSize(PageSizeClassIdx(rng() % SIZE_CLASS_NUM_PAGE_SIZES));
                        break;
                    case 2:
                        alloc_size_min = size_classes::pageSizeClassIdxToSize(PageSizeClassIdx(rng() % SIZE_CLASS_NUM_PAGE_SIZES)) + 1;
                        break;
                    default: alloc_size_min = SIZE_CLASS_LARGE_MAX_CLASS - rng() % 3; break;
                }
                size_t expected_size = 0;
                unsigned expected_skip = 0;
                bool expected_error = ref_exponential_grow_size_prepare(next, limit, alloc_size_min, &expected_size, &expected_skip);
                size_t actual_size = 0;
                PageSizeClassIdx actual_skip = 0;
                bool actual_error = exponential_grow.sizePrepare(alloc_size_min, &actual_size, &actual_skip);
                CHECK_EQ(expected_error, actual_error);
                CHECK_EQ(expected_size, actual_size);
                CHECK_EQ(expected_skip, actual_skip);
                if (!expected_error)
                {
                    ExponentialGrow committed = exponential_grow;
                    committed.sizeCommit(actual_skip);
                    CHECK_EQ(ref_exponential_grow_size_commit(next, limit, expected_skip), committed.next);
                    CHECK_EQ(committed.limit, limit);
                }
            }
        }
    }
}
