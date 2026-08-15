#include <gtest/gtest.h>

#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>

#include <sstream>

using namespace DB;

TEST(KeeperPathNormalize, NormalizeKeeperPath)
{
    EXPECT_EQ(normalizeKeeperPath("/"), "/");
    EXPECT_EQ(normalizeKeeperPath("/foo/../bar"), "/bar");
    EXPECT_EQ(normalizeKeeperPath("/foo/./bar"), "/foo/bar");
    EXPECT_EQ(normalizeKeeperPath("//foo///bar"), "/foo/bar");
    EXPECT_EQ(normalizeKeeperPath("/.."), "/");
    EXPECT_EQ(normalizeKeeperPath("/../foo"), "/foo");
    EXPECT_EQ(normalizeKeeperPath("/proc/self/cwd/foo"), "/proc/self/cwd/foo");
}

TEST(KeeperPathNormalize, GetAbsolutePath)
{
    std::ostringstream cout;
    std::ostringstream cerr;
    KeeperClientBase client(cout, cerr);

    client.cwd = "/a/b";
    EXPECT_EQ(client.getAbsolutePath("foo").string(), "/a/b/foo");
    EXPECT_EQ(client.getAbsolutePath("../c").string(), "/a/c");
    EXPECT_EQ(client.getAbsolutePath("/x").string(), "/x");

    client.cwd = "/";
    EXPECT_EQ(client.getAbsolutePath("foo").string(), "/foo");

    client.cwd = "/proc/self/cwd";
    EXPECT_EQ(client.getAbsolutePath("foo").string(), "/proc/self/cwd/foo");
}
