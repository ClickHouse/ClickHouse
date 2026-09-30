#include <gtest/gtest.h>

#include <Common/ProxyConfiguration.h>

namespace DB
{

TEST(ProxyCredentials, ParseUserInfo)
{
    /// Empty userinfo means no credentials at all.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("");
        ASSERT_EQ(username, "");
        ASSERT_EQ(password, "");
    }

    /// Username and password.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user:password");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "password");
    }

    /// Username only, no separator.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "");
    }

    /// Trailing separator, empty password.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user:");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "");
    }

    /// A password may contain colons. Only the first one separates.
    {
        const auto [username, password] = ProxyConfiguration::parseUserInfo("user:pass:word");
        ASSERT_EQ(username, "user");
        ASSERT_EQ(password, "pass:word");
    }
}

}
