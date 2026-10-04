/// Pinned behavior of `ExpGrow` (values from jemalloc's `exp_grow_init` / `exp_grow_size_*`).

#include <allocator/ExpGrow.h>

#include "Test.h"

using namespace jemalloc;

TEST(ExpGrow, Init)
{
    ExpGrow eg;
    eg.init();
    /// The index of 2 MiB: 31 (4 KiB pages), 23 (16 KiB), 15 (64 KiB).
    CHECK_EQ(eg.next, 3u + 4u * (21u - (LG_PAGE + 2)));
    CHECK_EQ(sz::pind2sz(eg.next), size_t(2) << 20);
    CHECK_EQ(eg.limit, SC_NPSIZES - 1);
}

TEST(ExpGrow, Series)
{
    ExpGrow eg;
    eg.init();

    /// A small request takes the next size of the series, and the series advances by one class.
    size_t alloc_size;
    pszind_t skip;
    CHECK(!eg.sizePrepare(PAGE, &alloc_size, &skip));
    CHECK_EQ(alloc_size, size_t(2) << 20);
    CHECK_EQ(skip, 0u);
    eg.sizeCommit(skip);
    CHECK_EQ(sz::pind2sz(eg.next), (size_t(5) << 20) / 2);

    /// A larger request skips classes.
    CHECK(!eg.sizePrepare(size_t(8) << 20, &alloc_size, &skip));
    CHECK_EQ(alloc_size, size_t(8) << 20);
    CHECK_EQ(skip, 7u);
    pszind_t next = eg.next;
    eg.sizeCommit(skip);
    CHECK_EQ(eg.next, next + 8);

    /// Beyond the largest class: error.
    CHECK(eg.sizePrepare(SC_LARGE_MAXCLASS + 1, &alloc_size, &skip));

    /// The limit caps the series.
    eg.limit = eg.next + 1;
    eg.sizeCommit(5);
    CHECK_EQ(eg.next, eg.limit);
}
