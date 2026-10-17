/// Pinned behavior of `ExponentialGrow` (values from jemalloc's `exp_grow_init` / `exp_grow_size_*`).

#include <allocator/ExponentialGrow.h>

#include "Test.h"

using namespace jemalloc;

TEST(ExponentialGrow, Init)
{
    ExponentialGrow exponential_grow;
    exponential_grow.init();
    /// The index of 2 MiB: 31 (4 KiB pages), 23 (16 KiB), 15 (64 KiB).
    CHECK_EQ(exponential_grow.next, 3u + 4u * (21u - (LOG2_PAGE + 2)));
    CHECK_EQ(size_classes::pageSizeClassIdxToSize(exponential_grow.next), size_t(2) << 20);
    CHECK_EQ(exponential_grow.limit, SIZE_CLASS_NUM_PAGE_SIZES - 1);
}

TEST(ExponentialGrow, Series)
{
    ExponentialGrow exponential_grow;
    exponential_grow.init();

    /// A small request takes the next size of the series, and the series advances by one class.
    size_t alloc_size;
    PageSizeClassIdx skip;
    CHECK(!exponential_grow.sizePrepare(PAGE, &alloc_size, &skip));
    CHECK_EQ(alloc_size, size_t(2) << 20);
    CHECK_EQ(skip, 0u);
    exponential_grow.sizeCommit(skip);
    CHECK_EQ(size_classes::pageSizeClassIdxToSize(exponential_grow.next), (size_t(5) << 20) / 2);

    /// A larger request skips classes.
    CHECK(!exponential_grow.sizePrepare(size_t(8) << 20, &alloc_size, &skip));
    CHECK_EQ(alloc_size, size_t(8) << 20);
    CHECK_EQ(skip, 7u);
    PageSizeClassIdx next = exponential_grow.next;
    exponential_grow.sizeCommit(skip);
    CHECK_EQ(exponential_grow.next, next + 8);

    /// Beyond the largest class: error.
    CHECK(exponential_grow.sizePrepare(SIZE_CLASS_LARGE_MAX_CLASS + 1, &alloc_size, &skip));

    /// The limit caps the series.
    exponential_grow.limit = exponential_grow.next + 1;
    exponential_grow.sizeCommit(5);
    CHECK_EQ(exponential_grow.next, exponential_grow.limit);
}
