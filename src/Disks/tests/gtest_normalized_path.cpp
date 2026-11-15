#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(NormalizedPath, Simple)
{
    EXPECT_EQ(normalizePath("a/b/c/d").string(), "a/b/c/d");
    EXPECT_EQ(normalizePath("a//b//c////d").string(), "a/b/c/d");
    EXPECT_EQ(normalizePath("a/b/c/d/").string(), "a/b/c/d");
    EXPECT_EQ(normalizePath("/a/b/c/d").string(), "a/b/c/d");
    EXPECT_EQ(normalizePath("/a/b/c/d/").string(), "a/b/c/d");
    EXPECT_EQ(normalizePath("a/b/c/d///////").string(), "a/b/c/d");
    EXPECT_EQ(normalizePath("//////a/b/c/d").string(), "a/b/c/d");
    EXPECT_EQ(normalizePath("//////a/b/c/d/////").string(), "a/b/c/d");
}

TEST(NormalizedPath, Root)
{
    EXPECT_EQ(normalizePath("").string(), "");
    EXPECT_EQ(normalizePath("/").string(), "");
    EXPECT_EQ(normalizePath("////").string(), "");
}

TEST(NormalizedPath, Relative)
{
    EXPECT_EQ(normalizePath("./").string(), "");
    EXPECT_EQ(normalizePath("./a/b/c").string(), "a/b/c");
    EXPECT_EQ(normalizePath("./////a/////b////c").string(), "a/b/c");
    EXPECT_EQ(normalizePath("././///.////././././a/./././///././b/./././///.//.///c/././././//.//.").string(), "a/b/c");
}
