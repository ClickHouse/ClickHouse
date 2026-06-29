#include <Interpreters/FileCache/FileCacheUtils.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>

#include <gtest/gtest.h>

TEST(FileCacheUtils, RoundDownToMultiple)
{
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(0, 0), 0u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(123, 0), 123u);

    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(0, 8), 0u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(7, 8), 0u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(8, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(9, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(15, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(16, 8), 16u);
}

TEST(FileCacheUtils, RoundUpToMultipleBasic)
{
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(0, 0), 0u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(123, 0), 123u);

    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(0, 8), 0u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(1, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(7, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(8, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(9, 8), 16u);
}

TEST(FileCacheUtils, RoundUpToMultipleBoundary)
{
    /// The result is representable at the extreme top of size_t — the old `num + multiple - 1`
    /// trick would have falsely overflowed here, which is the regression this PR addresses.
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(2, SIZE_MAX), SIZE_MAX);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(SIZE_MAX, SIZE_MAX), SIZE_MAX);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(SIZE_MAX - 1, SIZE_MAX), SIZE_MAX);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(SIZE_MAX, 1), SIZE_MAX);
}

TEST(FileCacheUtils, RoundUpToMultipleOverflow)
{
    /// `num = SIZE_MAX`, `multiple = SIZE_MAX - 1`: the next multiple above `num` is
    /// `2 * (SIZE_MAX - 1)` which does not fit in size_t — must throw rather than wrap.
    EXPECT_THROW(FileCacheUtils::roundUpToMultiple(SIZE_MAX, SIZE_MAX - 1), std::overflow_error);

    /// `num = SIZE_MAX - 1`, `multiple = 4`: the next multiple of 4 above `num` is past SIZE_MAX.
    EXPECT_THROW(FileCacheUtils::roundUpToMultiple(SIZE_MAX - 1, 4), std::overflow_error);
}
