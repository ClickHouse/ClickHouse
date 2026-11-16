#include <gtest/gtest.h>
#include "config.h"

#if USE_AWS_S3

#include <IO/S3/S3Capabilities.h>

using namespace DB;

TEST(S3Capabilities, SetBatchDeleteUnsupportedUnlessSupported)
{
    S3Capabilities undecided;
    EXPECT_TRUE(undecided.setBatchDeleteUnsupportedUnlessSupported());
    EXPECT_EQ(undecided.isBatchDeleteSupported(), false);

    /// A second caller that raced with the first must still be allowed to fall back.
    EXPECT_TRUE(undecided.setBatchDeleteUnsupportedUnlessSupported());
    EXPECT_EQ(undecided.isBatchDeleteSupported(), false);

    S3Capabilities supported(true);
    EXPECT_FALSE(supported.setBatchDeleteUnsupportedUnlessSupported());
    EXPECT_EQ(supported.isBatchDeleteSupported(), true);
}

#endif
