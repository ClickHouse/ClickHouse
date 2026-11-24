#include <gtest/gtest.h>

#include <Server/UsersWithoutPasswordWarning.h>

using DB::UsersWithoutPasswordWarning;

TEST(UsersWithoutPasswordWarning, IsLoopbackHost)
{
    EXPECT_TRUE(UsersWithoutPasswordWarning::isLoopbackHost("127.0.0.1"));
    EXPECT_TRUE(UsersWithoutPasswordWarning::isLoopbackHost("127.1.2.3"));
    EXPECT_TRUE(UsersWithoutPasswordWarning::isLoopbackHost("::1"));
    EXPECT_TRUE(UsersWithoutPasswordWarning::isLoopbackHost("::ffff:127.0.0.1"));
    EXPECT_TRUE(UsersWithoutPasswordWarning::isLoopbackHost("localhost"));
    EXPECT_TRUE(UsersWithoutPasswordWarning::isLoopbackHost("LocalHost"));

    EXPECT_FALSE(UsersWithoutPasswordWarning::isLoopbackHost("::"));
    EXPECT_FALSE(UsersWithoutPasswordWarning::isLoopbackHost("0.0.0.0"));
    EXPECT_FALSE(UsersWithoutPasswordWarning::isLoopbackHost("192.0.2.1"));
    EXPECT_FALSE(UsersWithoutPasswordWarning::isLoopbackHost("::ffff:192.0.2.1"));
    EXPECT_FALSE(UsersWithoutPasswordWarning::isLoopbackHost("2001:db8::1"));
    /// Host names other than `localhost` are not resolved.
    EXPECT_FALSE(UsersWithoutPasswordWarning::isLoopbackHost("example.com"));
    EXPECT_FALSE(UsersWithoutPasswordWarning::isLoopbackHost(""));
}
