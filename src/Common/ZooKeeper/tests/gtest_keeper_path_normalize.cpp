#include <gtest/gtest.h>

#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>

#include <sstream>

using namespace DB;

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
    EXPECT_EQ(client.getAbsolutePath("/foo/../bar").string(), "/bar");
    EXPECT_EQ(client.getAbsolutePath("/foo/./bar").string(), "/foo/bar");
    EXPECT_EQ(client.getAbsolutePath("//foo///bar").string(), "/foo/bar");
    EXPECT_EQ(client.getAbsolutePath("/..").string(), "/");
    EXPECT_EQ(client.getAbsolutePath("/../foo").string(), "/foo");

    /// Keeper znode paths must not be resolved through the filesystem. A path segment
    /// like /proc/self/cwd is a literal znode name, not a symlink to be canonicalized.
    client.cwd = "/proc/self/cwd";
    EXPECT_EQ(client.getAbsolutePath("foo").string(), "/proc/self/cwd/foo");
    EXPECT_EQ(client.getAbsolutePath("/proc/self/cwd/foo").string(), "/proc/self/cwd/foo");
}
