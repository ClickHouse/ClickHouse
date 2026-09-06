#include <gtest/gtest.h>

#include "config.h"

#if USE_YTSAURUS

#include <Processors/Sources/YTsaurusSource.h>

#include <base/types.h>

using namespace DB;

/// Regression test for the empty-input no-op invariant of a YTsaurus selective load.
/// An empty key set must produce zero `lookup_rows` requests, so that no throttler token is consumed and no empty
/// request is sent to YTsaurus. This must also hold for the unlimited case (`chunk_size == 0`).
TEST(YTsaurusDictionarySource, LookupChunkRowsEmptyInputProducesNoRequest)
{
    EXPECT_EQ(lookupChunkRows(0, 0, 0), 0u);
    EXPECT_EQ(lookupChunkRows(0, 0, 1), 0u);
    EXPECT_EQ(lookupChunkRows(0, 0, 100), 0u);
}

TEST(YTsaurusDictionarySource, LookupChunkRowsUnlimitedChunkSizeIsASingleRequest)
{
    EXPECT_EQ(lookupChunkRows(5, 0, 0), 5u);
    EXPECT_EQ(lookupChunkRows(5, 5, 0), 0u);
}

TEST(YTsaurusDictionarySource, LookupChunkRowsSplitsRowsIntoRequests)
{
    /// 5 rows with at most 2 rows per request: 2, 2, 1 and then nothing left.
    EXPECT_EQ(lookupChunkRows(5, 0, 2), 2u);
    EXPECT_EQ(lookupChunkRows(5, 2, 2), 2u);
    EXPECT_EQ(lookupChunkRows(5, 4, 2), 1u);
    EXPECT_EQ(lookupChunkRows(5, 5, 2), 0u);

    /// A chunk size equal to or larger than the input is a single request.
    EXPECT_EQ(lookupChunkRows(5, 0, 5), 5u);
    EXPECT_EQ(lookupChunkRows(5, 0, 10), 5u);

    /// The offset never exceeds the number of rows, but an out of range offset must stay a no-op anyway.
    EXPECT_EQ(lookupChunkRows(5, 100, 2), 0u);
}

#endif
