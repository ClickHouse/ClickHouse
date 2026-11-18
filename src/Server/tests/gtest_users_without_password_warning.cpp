#include <gtest/gtest.h>

#include <Access/AuthenticationData.h>
#include <Access/User.h>
#include <Server/StartupWarnings.h>

#include <ctime>
#include <initializer_list>

using namespace DB;

namespace
{

constexpr time_t now = 1'800'000'000;
constexpr time_t past = now - 1;
constexpr time_t future = now + 1;

AuthenticationData makeMethod(AuthenticationType type, const String & password, time_t valid_until = 0)
{
    AuthenticationData method(type);
    if (type != AuthenticationType::NO_PASSWORD)
        method.setPassword(password, /* second_factor = */ std::nullopt, /* validate = */ false);
    method.setValidUntil(valid_until);
    return method;
}

User makeUser(std::initializer_list<AuthenticationData> methods)
{
    User user;
    user.authentication_methods.assign(methods);
    return user;
}

bool accepts(const User & user, bool allow_no_password = true, bool allow_plaintext_password = true)
{
    return userAcceptsEmptyPassword(user, allow_no_password, allow_plaintext_password, now);
}

}

TEST(UsersWithoutPasswordWarning, EmptyPasswordMethods)
{
    EXPECT_TRUE(accepts(makeUser({makeMethod(AuthenticationType::NO_PASSWORD, "")})));
    EXPECT_TRUE(accepts(makeUser({makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "")})));
    EXPECT_TRUE(accepts(makeUser({makeMethod(AuthenticationType::SHA256_PASSWORD, "")})));
    EXPECT_TRUE(accepts(makeUser({makeMethod(AuthenticationType::DOUBLE_SHA1_PASSWORD, "")})));

    EXPECT_FALSE(accepts(makeUser({makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "secret")})));
    EXPECT_FALSE(accepts(makeUser({makeMethod(AuthenticationType::SHA256_PASSWORD, "secret")})));
    EXPECT_FALSE(accepts(makeUser({})));
}

TEST(UsersWithoutPasswordWarning, DisabledMethods)
{
    const auto no_password = makeUser({makeMethod(AuthenticationType::NO_PASSWORD, "")});
    EXPECT_FALSE(accepts(no_password, /* allow_no_password = */ false));
    EXPECT_TRUE(accepts(no_password, /* allow_no_password = */ true, /* allow_plaintext_password = */ false));

    const auto plaintext = makeUser({makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "")});
    EXPECT_FALSE(accepts(plaintext, /* allow_no_password = */ true, /* allow_plaintext_password = */ false));
    EXPECT_TRUE(accepts(plaintext, /* allow_no_password = */ false, /* allow_plaintext_password = */ true));

    /// The settings only disable `no_password` and `plaintext_password`.
    EXPECT_TRUE(accepts(makeUser({makeMethod(AuthenticationType::SHA256_PASSWORD, "")}), false, false));

    /// A disabled method is skipped, but another method can still accept the empty password.
    const auto plaintext_and_sha256
        = makeUser({makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, ""), makeMethod(AuthenticationType::SHA256_PASSWORD, "")});
    EXPECT_TRUE(accepts(plaintext_and_sha256, /* allow_no_password = */ true, /* allow_plaintext_password = */ false));
}

TEST(UsersWithoutPasswordWarning, ExpiredMethods)
{
    EXPECT_FALSE(accepts(makeUser({makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "", past)})));
    EXPECT_FALSE(accepts(makeUser({makeMethod(AuthenticationType::NO_PASSWORD, "", past)})));
    EXPECT_TRUE(accepts(makeUser({makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "", future)})));
    EXPECT_TRUE(accepts(makeUser({makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "", now)})));

    /// An expired method that accepts the empty password rejects the login even if another one accepts it too:
    /// the session expires at the earliest `VALID UNTIL` of all matching methods.
    EXPECT_FALSE(accepts(
        makeUser({makeMethod(AuthenticationType::SHA256_PASSWORD, ""), makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "", past)})));

    /// An expired method with a different password does not match the empty one.
    EXPECT_TRUE(accepts(makeUser(
        {makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, ""), makeMethod(AuthenticationType::SHA256_PASSWORD, "secret", past)})));

    /// A disabled expired method is ignored entirely.
    EXPECT_TRUE(accepts(
        makeUser({makeMethod(AuthenticationType::SHA256_PASSWORD, ""), makeMethod(AuthenticationType::PLAINTEXT_PASSWORD, "", past)}),
        /* allow_no_password = */ true,
        /* allow_plaintext_password = */ false));
}
