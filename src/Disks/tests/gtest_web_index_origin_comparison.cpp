#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromIndexPages.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Web/OriginComparisonUtils.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/URI.h>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FILE_SIZE;
}

namespace
{
class WebObjectStorageWithUnknownSizeMetadata final : public DB::WebObjectStorage
{
public:
    WebObjectStorageWithUnknownSizeMetadata()
        : DB::WebObjectStorage("http://example.com/", "", ::getContext().context)
    {
    }

    DB::ObjectMetadata getObjectMetadata(const std::string &, bool) const override
    {
        return makeMetadata();
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string &, bool) const override
    {
        return makeMetadata();
    }

private:
    static DB::ObjectMetadata makeMetadata()
    {
        DB::ObjectMetadata metadata;
        metadata.is_size_known = false;
        metadata.size_bytes = 0;
        return metadata;
    }
};
}


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


TEST(WebIndexOriginComparison, BuildsRelativePathPrefixFromNonRootBase)
{
    Poco::URI base("http://example.com/prefix/");
    Poco::URI listing_prefix("http://example.com/prefix/data/2025/");

    ASSERT_EQ(
        DB::WebIndexPage::getPathPrefixRelativeToBase(listing_prefix, base),
        "data/2025/");
}


TEST(WebIndexOriginComparison, RejectsPercentEncodedParentDirectoryInListingPrefix)
{
    Poco::URI listing("http://example.com/data/2025/", false);
    Poco::URI candidate("http://example.com/data/2025/%2e%2e/secret.tsv", false);

    ASSERT_FALSE(DB::WebIndexPage::hasPathPrefix(candidate, listing));
}


TEST(WebIndexOriginComparison, RejectsPercentEncodedSlashParentDirectoryInListingPrefix)
{
    Poco::URI listing("http://example.com/data/2025/", false);
    Poco::URI candidate("http://example.com/data/2025/..%2fsecret.tsv", false);

    ASSERT_FALSE(DB::WebIndexPage::hasPathPrefix(candidate, listing));
}


TEST(WebIndexOriginComparison, AcceptsPercentEncodedOrdinaryPathSegmentInListingPrefix)
{
    Poco::URI listing("http://example.com/data/2025/", false);
    Poco::URI candidate("http://example.com/data/2025/partition%3D1/data.tsv", false);

    ASSERT_TRUE(DB::WebIndexPage::hasPathPrefix(candidate, listing));
}

TEST(WebIndexOriginComparison, InheritsRawQueryWithoutDoubleEncoding)
{
    ASSERT_EQ(
        DB::WebIndexPage::getEffectiveRelativePathForDeduplication(
            "data/2025/part1.tsv",
            "http://example.com/data/**/part*.tsv?token=a%2Fb"),
        "data/2025/part1.tsv?token=a%2Fb");
}

TEST(WebObjectStorage, BuildsURLWithRawQueryWithoutDoubleEncoding)
{
    DB::WebObjectStorage object_storage("http://example.com/data/", "?token=a%2Fb", ::getContext().context);

    ASSERT_EQ(
        object_storage.buildURLs("part1.tsv"),
        std::vector<std::string>{"http://example.com/data/part1.tsv?token=a%2Fb"});
}

TEST(WebIndexMetadataStorage, DoesNotReportUnknownObjectSizeAsZero)
{
    WebObjectStorageWithUnknownSizeMetadata object_storage;
    DB::MetadataStorageFromIndexPages metadata_storage(object_storage);

    EXPECT_EQ(metadata_storage.getFileSizeIfExists("data/part.tsv"), std::nullopt);

    try
    {
        (void)metadata_storage.getFileSize("data/part.tsv");
        FAIL() << "Expected UNKNOWN_FILE_SIZE";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FILE_SIZE);
    }
}
