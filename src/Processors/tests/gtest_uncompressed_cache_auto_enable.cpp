#include <Processors/QueryPlan/UncompressedCacheUtils.h>

#include <gtest/gtest.h>


namespace DB
{

TEST(UncompressedCacheAutoEnable, RequiresAnAvailableCache)
{
    EXPECT_FALSE(canAutoEnableUncompressedCacheForMergeTreeRead(
        /*any_parts_on_remote_disk=*/false,
        /*has_uncompressed_cache=*/false));
}

TEST(UncompressedCacheAutoEnable, RequiresAllPartsToBeLocal)
{
    EXPECT_FALSE(canAutoEnableUncompressedCacheForMergeTreeRead(
        /*any_parts_on_remote_disk=*/true,
        /*has_uncompressed_cache=*/true));
}

TEST(UncompressedCacheAutoEnable, AutoEnablesForEligibleReads)
{
    EXPECT_TRUE(shouldUseUncompressedCacheForMergeTreeRead(
        /*setting_changed=*/false,
        /*setting_value=*/false,
        /*query_fits_cache_thresholds=*/true,
        /*auto_enable_supported=*/true));
}

TEST(UncompressedCacheAutoEnable, KeepsRemoteReadsOptInByDefault)
{
    EXPECT_FALSE(shouldUseUncompressedCacheForMergeTreeRead(
        /*setting_changed=*/false,
        /*setting_value=*/false,
        /*query_fits_cache_thresholds=*/true,
        /*auto_enable_supported=*/canAutoEnableUncompressedCacheForMergeTreeRead(
            /*any_parts_on_remote_disk=*/true,
            /*has_uncompressed_cache=*/true)));
}

TEST(UncompressedCacheAutoEnable, KeepsReadsOptInByDefaultWhenCacheIsDisabled)
{
    EXPECT_FALSE(shouldUseUncompressedCacheForMergeTreeRead(
        /*setting_changed=*/false,
        /*setting_value=*/false,
        /*query_fits_cache_thresholds=*/true,
        /*auto_enable_supported=*/canAutoEnableUncompressedCacheForMergeTreeRead(
            /*any_parts_on_remote_disk=*/false,
            /*has_uncompressed_cache=*/false)));
}

TEST(UncompressedCacheAutoEnable, RespectsExplicitDisable)
{
    EXPECT_FALSE(shouldUseUncompressedCacheForMergeTreeRead(
        /*setting_changed=*/true,
        /*setting_value=*/false,
        /*query_fits_cache_thresholds=*/true,
        /*auto_enable_supported=*/true));
}

TEST(UncompressedCacheAutoEnable, RespectsExplicitEnable)
{
    EXPECT_TRUE(shouldUseUncompressedCacheForMergeTreeRead(
        /*setting_changed=*/true,
        /*setting_value=*/true,
        /*query_fits_cache_thresholds=*/true,
        /*auto_enable_supported=*/false));
}

TEST(UncompressedCacheAutoEnable, DisablesLargeReadsEvenWhenEnabled)
{
    EXPECT_FALSE(shouldUseUncompressedCacheForMergeTreeRead(
        /*setting_changed=*/true,
        /*setting_value=*/true,
        /*query_fits_cache_thresholds=*/false,
        /*auto_enable_supported=*/true));
}

}
