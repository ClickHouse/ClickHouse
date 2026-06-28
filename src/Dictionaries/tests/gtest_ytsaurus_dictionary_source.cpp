#include <gtest/gtest.h>

#include "config.h"

#if USE_YTSAURUS

#include <Dictionaries/YTsaurusDictionarySource.h>

#include <base/types.h>

using namespace DB;

namespace
{
using Vec = VectorWithMemoryTracking<UInt64>;
}

/// Regression test for the empty-input no-op invariant of `divideVectorByChunkSize`.
/// An empty selective load must produce zero chunks (and therefore zero lookup blocks), so that
/// `YTsaurusSourceFactory::createPipe` issues no `lookup_rows` request and consumes no throttler token.
/// This must also hold for the unlimited case (`chunk_size == 0`), which previously returned one empty
/// chunk for an empty input and thus bypassed the empty-batch guard.
TEST(YTsaurusDictionarySource, DivideEmptyInputProducesNoChunks)
{
    EXPECT_TRUE(divideVectorByChunkSize(Vec{}, 0).empty());
    EXPECT_TRUE(divideVectorByChunkSize(Vec{}, 1).empty());
    EXPECT_TRUE(divideVectorByChunkSize(Vec{}, 100).empty());
}

TEST(YTsaurusDictionarySource, DivideUnlimitedChunkSizeReturnsSingleChunk)
{
    const Vec input{1, 2, 3, 4, 5};
    const auto chunks = divideVectorByChunkSize(input, 0);
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0], input);
}

TEST(YTsaurusDictionarySource, DivideSplitsIntoChunks)
{
    const Vec input{1, 2, 3, 4, 5};

    const auto chunks = divideVectorByChunkSize(input, 2);
    ASSERT_EQ(chunks.size(), 3u);
    EXPECT_EQ(chunks[0], (Vec{1, 2}));
    EXPECT_EQ(chunks[1], (Vec{3, 4}));
    EXPECT_EQ(chunks[2], (Vec{5}));

    const auto exact = divideVectorByChunkSize(input, 5);
    ASSERT_EQ(exact.size(), 1u);
    EXPECT_EQ(exact[0], input);

    const auto larger = divideVectorByChunkSize(input, 10);
    ASSERT_EQ(larger.size(), 1u);
    EXPECT_EQ(larger[0], input);
}

#endif
