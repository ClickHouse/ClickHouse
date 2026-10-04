/// Compares `ExpGrow` with jemalloc's `exp_grow_t`.

#include <allocator/ExpGrow.h>

#include "Test.h"

#include <random>

extern "C"
{
void ref_exp_grow_boot();
void ref_exp_grow_init(unsigned * next, unsigned * limit);
bool ref_exp_grow_size_prepare(unsigned next, unsigned limit, size_t alloc_size_min, size_t * r_alloc_size, unsigned * r_skip);
unsigned ref_exp_grow_size_commit(unsigned next, unsigned limit, unsigned skip);
}

using namespace jemalloc;

TEST(ExpGrowOracle, Init)
{
    ref_exp_grow_boot();
    unsigned next;
    unsigned limit;
    ref_exp_grow_init(&next, &limit);
    ExpGrow eg;
    eg.init();
    CHECK_EQ(eg.next, next);
    CHECK_EQ(eg.limit, limit);
    CHECK_EQ(eg.limit, SC_NPSIZES - 1);
    if constexpr (LG_PAGE == 16)
        CHECK_EQ(eg.next, 15u);
}

TEST(ExpGrowOracle, PrepareCommit)
{
    std::mt19937_64 rng(1);
    for (pszind_t next = 0; next < SC_NPSIZES; ++next)
    {
        for (pszind_t limit : {pszind_t(0), next, pszind_t(next + 1), pszind_t(SC_NPSIZES / 2), pszind_t(SC_NPSIZES - 1)})
        {
            ExpGrow eg{next, limit};
            for (int i = 0; i < 64; ++i)
            {
                size_t alloc_size_min;
                switch (i % 4)
                {
                    case 0: alloc_size_min = size_t(1) + rng() % (size_t(1) << (rng() % 48)); break;
                    case 1: alloc_size_min = sz::pind2sz(pszind_t(rng() % SC_NPSIZES)); break;
                    case 2: alloc_size_min = sz::pind2sz(pszind_t(rng() % SC_NPSIZES)) + 1; break;
                    default: alloc_size_min = SC_LARGE_MAXCLASS - rng() % 3; break;
                }
                size_t expected_size = 0;
                unsigned expected_skip = 0;
                bool expected_err = ref_exp_grow_size_prepare(next, limit, alloc_size_min, &expected_size, &expected_skip);
                size_t actual_size = 0;
                pszind_t actual_skip = 0;
                bool actual_err = eg.sizePrepare(alloc_size_min, &actual_size, &actual_skip);
                CHECK_EQ(expected_err, actual_err);
                CHECK_EQ(expected_size, actual_size);
                CHECK_EQ(expected_skip, actual_skip);
                if (!expected_err)
                {
                    ExpGrow committed = eg;
                    committed.sizeCommit(actual_skip);
                    CHECK_EQ(ref_exp_grow_size_commit(next, limit, expected_skip), committed.next);
                    CHECK_EQ(committed.limit, limit);
                }
            }
        }
    }
}
