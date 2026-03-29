#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/Web/OriginComparisonUtils.h>

#include <Poco/URI.h>


TEST(WebIndexOriginComparison, TreatsDefaultHttpPortAsEquivalent)
{
    Poco::URI source("http://example.com/data/**/part*.tsv");
    Poco::URI candidate("http://example.com:80/data/2025/part1.tsv");

    ASSERT_TRUE(DB::WebIndexPage::isSameOrigin(source, candidate));
}


TEST(WebIndexOriginComparison, TreatsDefaultHttpsPortAsEquivalent)
{
    Poco::URI source("https://example.com/data/**/part*.tsv");
    Poco::URI candidate("https://example.com:443/data/2025/part1.tsv");

    ASSERT_TRUE(DB::WebIndexPage::isSameOrigin(source, candidate));
}


TEST(WebIndexOriginComparison, RejectsDifferentEffectivePorts)
{
    Poco::URI source("http://example.com/data/**/part*.tsv");
    Poco::URI candidate("http://example.com:8080/data/2025/part1.tsv");

    ASSERT_FALSE(DB::WebIndexPage::isSameOrigin(source, candidate));
}


TEST(WebIndexOriginComparison, TreatsDefaultPortVariantAsSameListingPrefix)
{
    Poco::URI listing("http://example.com/data/2025/");
    Poco::URI candidate("http://example.com:80/data/2025/part1.tsv");

    ASSERT_TRUE(DB::WebIndexPage::isSameOrigin(listing, candidate));
    ASSERT_TRUE(DB::WebIndexPage::hasPathPrefix(candidate, listing));
}


TEST(WebIndexOriginComparison, BuildsRelativePathAcrossDefaultPortVariants)
{
    Poco::URI base("http://example.com/");
    Poco::URI candidate("http://example.com:80/data/2025/part1.tsv?download=1#frag");

    ASSERT_EQ(
        DB::WebIndexPage::getRelativePathWithQueryAndFragment(candidate, base),
        "data/2025/part1.tsv?download=1#frag");
}
