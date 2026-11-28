#include <gtest/gtest.h>

#include <Access/AccessControl.h>
#include <Access/Authentication.h>
#include <Access/AuthenticationData.h>
#include <Access/Common/OneTimePassword.h>
#include <Access/Credentials.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/User.h>
#include <Interpreters/ClientInfo.h>
#include <Common/Exception.h>

#include <Poco/Net/IPAddress.h>

#include "config.h"

#include <chrono>
#include <ctime>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int WRONG_PASSWORD;
extern const int REQUIRED_SECOND_FACTOR;
}

namespace
{

using Util = AuthenticationData::Util;

const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
/// Far enough from `now` that the real clock used by `authenticate` cannot cross them during the test.
const time_t past = now - 86400;
const time_t future = now + 86400;

OneTimePasswordSecret otpSecret() { return OneTimePasswordSecret{"JBSWY3DPEHPK3PXP"}; }

AuthenticationData withPassword(AuthenticationType type, const String & password, std::optional<OneTimePasswordSecret> otp = {})
{
    AuthenticationData method(type);
    method.setPassword(password, std::move(otp), /* validate = */ false);
    return method;
}

AuthenticationData withSalt(AuthenticationType type, const String & password, const String & salt)
{
    AuthenticationData method(type);
    method.setSalt(salt);
    if (type == AuthenticationType::SHA256_PASSWORD)
        method.setPasswordHashBinary(Util::encodeSHA256(password + salt), std::nullopt, /* validate = */ false);
    else
        method.setPasswordHashBinary(Util::encodeScramSHA256(password, salt), std::nullopt, /* validate = */ false);
    return method;
}

AuthenticationData noPassword(std::optional<OneTimePasswordSecret> otp = {})
{
    AuthenticationData method(AuthenticationType::NO_PASSWORD);
    if (otp)
        method.setPassword("", std::move(otp), /* validate = */ false);
    return method;
}

#if USE_BCRYPT
AuthenticationData bcrypt(const String & password)
{
    AuthenticationData method(AuthenticationType::BCRYPT_PASSWORD);
    method.setPasswordBcrypt(password, /* workfactor = */ 4, std::nullopt, /* validate = */ false);
    return method;
}
#endif

AuthenticationData withValidUntil(AuthenticationData method, time_t valid_until)
{
    method.setValidUntil(valid_until);
    return method;
}

/// One method of each kind that matters for the empty password, without `VALID UNTIL`.
std::vector<AuthenticationData> baseMethods()
{
    std::vector<AuthenticationData> methods{
        noPassword(),
        noPassword(otpSecret()),
        withPassword(AuthenticationType::PLAINTEXT_PASSWORD, ""),
        withPassword(AuthenticationType::PLAINTEXT_PASSWORD, "secret"),
        withPassword(AuthenticationType::PLAINTEXT_PASSWORD, "", otpSecret()),
        withPassword(AuthenticationType::DOUBLE_SHA1_PASSWORD, ""),
        withPassword(AuthenticationType::DOUBLE_SHA1_PASSWORD, "secret"),
        AuthenticationData(AuthenticationType::KERBEROS),
        AuthenticationData(AuthenticationType::SSL_CERTIFICATE),
    };
#if USE_SSL
    methods.push_back(withPassword(AuthenticationType::SHA256_PASSWORD, ""));
    methods.push_back(withSalt(AuthenticationType::SHA256_PASSWORD, "", "salt"));
    methods.push_back(withSalt(AuthenticationType::SHA256_PASSWORD, "secret", "salt"));
    methods.push_back(withSalt(AuthenticationType::SCRAM_SHA256_PASSWORD, "", "salt"));
    methods.push_back(withSalt(AuthenticationType::SCRAM_SHA256_PASSWORD, "secret", "salt"));
#endif
#if USE_BCRYPT
    methods.push_back(bcrypt(""));
    methods.push_back(bcrypt("secret"));
#endif
    return methods;
}

std::vector<AuthenticationData> withAllValidities(const std::vector<AuthenticationData> & methods)
{
    std::vector<AuthenticationData> res;
    for (const auto & method : methods)
        for (time_t valid_until : {time_t{0}, past, future})
            res.push_back(withValidUntil(method, valid_until));
    return res;
}

/// Whether the real authentication lets the user log in with an empty password.
bool loginWithEmptyPassword(AccessControl & access_control, const User & user, bool allow_no_password, bool allow_plaintext_password)
{
    MemoryAccessStorage storage("memory", access_control.getChangesNotifier(), /* allow_backup = */ false);
    storage.insert(std::make_shared<User>(user));

    try
    {
        storage.authenticate(
            BasicCredentials(user.getName(), ""),
            Poco::Net::IPAddress("192.0.2.1"),
            access_control.getExternalAuthenticators(),
            ClientInfo{},
            allow_no_password,
            allow_plaintext_password);
        return true;
    }
    catch (const Exception & e)
    {
        EXPECT_TRUE(e.code() == ErrorCodes::WRONG_PASSWORD || e.code() == ErrorCodes::REQUIRED_SECOND_FACTOR) << e.message();
        return false;
    }
}

User makeUser(std::vector<AuthenticationData> methods)
{
    User user;
    user.setName("u");
    user.authentication_methods = std::move(methods);
    return user;
}

String describe(const User & user, bool allow_no_password, bool allow_plaintext_password)
{
    String res = fmt::format("allow_no_password={}, allow_plaintext_password={}, methods:", allow_no_password, allow_plaintext_password);
    for (const auto & method : user.authentication_methods)
        res += fmt::format(
            " [{}, valid_until={}, otp={}, salt='{}']",
            toString(method.getType()),
            method.getValidUntil() == 0 ? "0" : (method.getValidUntil() < now ? "past" : "future"),
            method.getOneTimePassword().has_value(),
            method.getSalt());
    return res;
}

void expectAgreesWithLogin(AccessControl & access_control, const User & user)
{
    for (bool allow_no_password : {false, true})
    {
        for (bool allow_plaintext_password : {false, true})
        {
            time_t valid_until = -1;
            bool accepts = IAccessStorage::acceptsEmptyPassword(user, allow_no_password, allow_plaintext_password, now, &valid_until);
            EXPECT_EQ(accepts, loginWithEmptyPassword(access_control, user, allow_no_password, allow_plaintext_password))
                << describe(user, allow_no_password, allow_plaintext_password);

            if (accepts)
            {
                /// The login expires at the earliest `VALID UNTIL` of the allowed methods accepting the empty password.
                time_t expected_valid_until = 0;
                for (const auto & method : user.authentication_methods)
                {
                    const bool allowed = (method.getType() != AuthenticationType::NO_PASSWORD || allow_no_password)
                        && (method.getType() != AuthenticationType::PLAINTEXT_PASSWORD || allow_plaintext_password);
                    if (allowed && Authentication::acceptsEmptyPassword(method) && method.getValidUntil()
                        && (!expected_valid_until || method.getValidUntil() < expected_valid_until))
                        expected_valid_until = method.getValidUntil();
                }
                EXPECT_EQ(valid_until, expected_valid_until) << describe(user, allow_no_password, allow_plaintext_password);
            }
        }
    }
}

}

TEST(AcceptsEmptyPassword, Methods)
{
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(noPassword()));
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(withPassword(AuthenticationType::PLAINTEXT_PASSWORD, "")));
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(withPassword(AuthenticationType::DOUBLE_SHA1_PASSWORD, "")));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(withPassword(AuthenticationType::PLAINTEXT_PASSWORD, "secret")));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(withPassword(AuthenticationType::DOUBLE_SHA1_PASSWORD, "secret")));

    /// A required one-time password is a secret on its own.
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(noPassword(otpSecret())));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(withPassword(AuthenticationType::PLAINTEXT_PASSWORD, "", otpSecret())));

    /// Not verified locally, or not by a password.
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(AuthenticationData(AuthenticationType::LDAP)));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(AuthenticationData(AuthenticationType::HTTP)));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(AuthenticationData(AuthenticationType::KERBEROS)));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(AuthenticationData(AuthenticationType::SSL_CERTIFICATE)));

    /// `VALID UNTIL` is not taken into account.
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(withValidUntil(noPassword(), past)));

#if USE_SSL
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(withPassword(AuthenticationType::SHA256_PASSWORD, "")));
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(withSalt(AuthenticationType::SHA256_PASSWORD, "", "salt")));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(withSalt(AuthenticationType::SHA256_PASSWORD, "secret", "salt")));
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(withSalt(AuthenticationType::SCRAM_SHA256_PASSWORD, "", "salt")));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(withSalt(AuthenticationType::SCRAM_SHA256_PASSWORD, "secret", "salt")));
#endif
#if USE_BCRYPT
    EXPECT_TRUE(Authentication::acceptsEmptyPassword(bcrypt("")));
    EXPECT_FALSE(Authentication::acceptsEmptyPassword(bcrypt("secret")));
#endif
}

TEST(AcceptsEmptyPassword, Users)
{
    const auto user_with = [](std::vector<AuthenticationData> methods) { return makeUser(std::move(methods)); };
    const auto accepts = [](const User & user, bool allow_no_password = true, bool allow_plaintext_password = true)
    { return IAccessStorage::acceptsEmptyPassword(user, allow_no_password, allow_plaintext_password, now); };

    const auto plaintext = withPassword(AuthenticationType::PLAINTEXT_PASSWORD, "");
    const auto double_sha1 = withPassword(AuthenticationType::DOUBLE_SHA1_PASSWORD, "");

    EXPECT_TRUE(accepts(user_with({plaintext})));
    EXPECT_FALSE(accepts(user_with({})));

    /// A disabled method is skipped, but another method can still accept the empty password.
    EXPECT_FALSE(accepts(user_with({noPassword()}), /* allow_no_password = */ false));
    EXPECT_FALSE(accepts(user_with({plaintext}), true, /* allow_plaintext_password = */ false));
    EXPECT_TRUE(accepts(user_with({plaintext, double_sha1}), true, /* allow_plaintext_password = */ false));

    /// An expired method accepting the empty password rejects the login even if another one accepts it too,
    /// unless the expired method is disabled.
    EXPECT_FALSE(accepts(user_with({withValidUntil(plaintext, past)})));
    EXPECT_FALSE(accepts(user_with({double_sha1, withValidUntil(plaintext, past)})));
    EXPECT_TRUE(accepts(user_with({double_sha1, withValidUntil(plaintext, past)}), true, /* allow_plaintext_password = */ false));
    EXPECT_TRUE(accepts(user_with({plaintext, withValidUntil(withPassword(AuthenticationType::DOUBLE_SHA1_PASSWORD, "secret"), past)})));

    /// The login expires at the earliest `VALID UNTIL` of the methods accepting the empty password.
    time_t valid_until = -1;
    EXPECT_TRUE(
        IAccessStorage::acceptsEmptyPassword(
            user_with({withValidUntil(plaintext, future + 10), withValidUntil(double_sha1, future)}), true, true, now, &valid_until));
    EXPECT_EQ(valid_until, future);
    EXPECT_TRUE(IAccessStorage::acceptsEmptyPassword(user_with({plaintext}), true, true, now, &valid_until));
    EXPECT_EQ(valid_until, 0);
    EXPECT_TRUE(IAccessStorage::acceptsEmptyPassword(user_with({withValidUntil(plaintext, now)}), true, true, now));
}

/// `IAccessStorage::acceptsEmptyPassword` must agree with the real authentication, which it re-implements without the
/// side effects and external probes. A user with one authentication method of each kind.
TEST(AcceptsEmptyPassword, AgreesWithLoginOneMethod)
{
    AccessControl access_control;
    for (const auto & method : withAllValidities(baseMethods()))
        expectAgreesWithLogin(access_control, makeUser({method}));
}

/// Users with two authentication methods: the disabled ones, the expiry of the combination and the methods requiring
/// a second factor interact here.
TEST(AcceptsEmptyPassword, AgreesWithLoginTwoMethods)
{
    std::vector<AuthenticationData> base{
        noPassword(),
        noPassword(otpSecret()),
        withPassword(AuthenticationType::PLAINTEXT_PASSWORD, ""),
        withPassword(AuthenticationType::PLAINTEXT_PASSWORD, "secret"),
        withPassword(AuthenticationType::DOUBLE_SHA1_PASSWORD, ""),
    };
#if USE_SSL
    base.push_back(withSalt(AuthenticationType::SHA256_PASSWORD, "", "salt"));
#endif
    AccessControl access_control;
    const auto methods = withAllValidities(base);
    for (const auto & first : methods)
        for (const auto & second : methods)
            expectAgreesWithLogin(access_control, makeUser({first, second}));
}
