#include "config.h"

#if USE_JWT_CPP && USE_SSL

#include <gtest/gtest.h>

#include <Client/ConnectionParameters.h>
#include <Client/JWTProvider.h>
#include <Client/NonInteractiveAuthentication.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

#include <atomic>
#include <sstream>
#include <thread>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int AUTHENTICATION_FAILED;
extern const int BAD_ARGUMENTS;
}

namespace
{

std::ostringstream & messages()
{
    static std::ostringstream stream;
    return stream;
}

/// Returns a new token on every call and records how many threads are inside `fetchJWT` at the same time.
class FakeJWTProvider : public JWTProvider
{
public:
    FakeJWTProvider() : JWTProvider(JWTProviderOptions{}, messages(), messages()) {}

    std::atomic<int> active = 0;
    std::atomic<int> max_active = 0;
    std::atomic<int> calls = 0;
    std::string fixed_token;

protected:
    std::string fetchJWT() override
    {
        const int now_active = ++active;
        int seen = max_active.load();
        while (seen < now_active && !max_active.compare_exchange_weak(seen, now_active))
        {
        }

        /// Give the other threads a chance to get in if they are not excluded.
        for (int i = 0; i < 1000; ++i)
            std::this_thread::yield();

        const int call = ++calls;
        --active;
        return fixed_token.empty() ? "token-" + std::to_string(call) : fixed_token;
    }
};


/// Has the structure of the real provider when there is no refresh token: a valid token is returned as it is, and
/// otherwise the only way forward is a login, which only a thread that may ask the user can do.
class ExpiringTokenProvider : public JWTProvider
{
public:
    ExpiringTokenProvider() : JWTProvider(JWTProviderOptions{}, messages(), messages()) {}

    int logins = 0;
    bool token_valid = false;

protected:
    std::string fetchJWT() override
    {
        if (token_valid)
            return "token";

        if (NonInteractiveAuthenticationScope::isActive())
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "cannot log in on a worker");

        ++logins;
        token_valid = true;
        return "token";
    }
};

}

/// An ordinary foreground query is run by a worker (so that Ctrl+B can detach it). After the access token expired
/// without a refresh token, the login has to happen on the main thread before the query is handed over; the worker
/// then finds a valid token and does not need to log in.
TEST(JWTProvider, MainThreadRenewsTokenForWorker)
{
    ExpiringTokenProvider provider;

    /// Without the renewal on the main thread, the worker fails and nothing was done by it.
    {
        NonInteractiveAuthenticationScope scope;
        EXPECT_THROW(provider.getJWT(), Exception);
    }
    EXPECT_EQ(provider.logins, 0);

    /// The main thread logs in, and the worker gets the token.
    EXPECT_EQ(provider.getJWT(), "token");
    EXPECT_EQ(provider.logins, 1);
    {
        NonInteractiveAuthenticationScope scope;
        EXPECT_EQ(provider.getJWT(), "token");
    }
    EXPECT_EQ(provider.logins, 1);

    /// The token expires again: the worker still cannot log in, the main thread can.
    provider.token_valid = false;
    {
        NonInteractiveAuthenticationScope scope;
        EXPECT_THROW(provider.getJWT(), Exception);
    }
    EXPECT_EQ(provider.getJWT(), "token");
    EXPECT_EQ(provider.logins, 2);
}

/// The foreground and the background jobs share one provider: its token state and a rotating refresh token
/// must not be used by two callers at once.
TEST(JWTProvider, GetJWTIsSerialized)
{
    auto provider = std::make_shared<FakeJWTProvider>();

    constexpr size_t threads_count = 8;
    constexpr size_t calls_per_thread = 50;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < threads_count; ++i)
        threads.emplace_back([&]
        {
            for (size_t j = 0; j < calls_per_thread; ++j)
                provider->getJWT();
        });
    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(provider->calls.load(), static_cast<int>(threads_count * calls_per_thread));
    EXPECT_EQ(provider->max_active.load(), 1);
}

/// Parameters copied for background jobs keep pointing to the same provider, so refreshing them concurrently
/// is serialized too.
TEST(JWTProvider, ConnectionParametersCopiesRefreshSerialized)
{
    auto provider = std::make_shared<FakeJWTProvider>();
    ConnectionParameters foreground;
    foreground.jwt = "login-token";
    foreground.jwt_provider = provider;

    std::vector<ConnectionParameters> copies(8, foreground);
    std::vector<std::thread> threads;
    for (auto & copy : copies)
        threads.emplace_back([&copy]
        {
            for (size_t j = 0; j < 50; ++j)
                copy.refreshJWT();
        });
    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(provider->calls.load(), 8 * 50);
    EXPECT_EQ(provider->max_active.load(), 1);
}

/// A background connection must not authenticate with the token of the login if it has expired since.
TEST(JWTProvider, RefreshReplacesLoginToken)
{
    auto provider = std::make_shared<FakeJWTProvider>();
    provider->fixed_token = "replacement-token";

    ConnectionParameters parameters;
    parameters.jwt = "expired-login-token";
    parameters.jwt_provider = provider;
    parameters.refreshJWT();

    EXPECT_EQ(parameters.jwt, "replacement-token");
    EXPECT_EQ(provider->calls.load(), 1);
}

/// Connections that do not authenticate with a token never start asking for one.
TEST(JWTProvider, RefreshLeavesOtherAuthenticationAlone)
{
    auto provider = std::make_shared<FakeJWTProvider>();

    ConnectionParameters without_login_token;
    without_login_token.jwt_provider = provider;
    without_login_token.refreshJWT();
    EXPECT_TRUE(without_login_token.jwt.empty());

    ConnectionParameters without_provider;
    without_provider.jwt = "static-token";
    without_provider.refreshJWT();
    EXPECT_EQ(without_provider.jwt, "static-token");

    EXPECT_EQ(provider->calls.load(), 0);
}

/// A background job must not start the device code login: it writes to the terminal of the prompt and waits for
/// the user without any way to cancel it. Without a token and without a refresh token that is the only way forward,
/// so the job fails with an explicit error. The main thread is not affected.
TEST(JWTProvider, BackgroundThreadDoesNotStartDeviceLogin)
{
    JWTProvider provider(JWTProviderOptions{}, messages(), messages());
    messages().str("");

    {
        NonInteractiveAuthenticationScope scope;
        EXPECT_TRUE(NonInteractiveAuthenticationScope::isActive());
        try
        {
            provider.getJWT();
            FAIL() << "the login must not start";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::AUTHENTICATION_FAILED);
        }

        /// Not even a line was written to the streams of the main client.
        EXPECT_TRUE(messages().str().empty());
    }

    /// The scope ends with its object: the same call now reaches the login (and stops at the missing client id,
    /// before any network access).
    EXPECT_FALSE(NonInteractiveAuthenticationScope::isActive());
    try
    {
        provider.getJWT();
        FAIL() << "the login must start";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

/// The restriction is per thread: the main thread keeps asking while a background thread does not.
TEST(JWTProvider, NonInteractiveScopeIsPerThread)
{
    NonInteractiveAuthenticationScope scope;

    bool other_thread_is_restricted = true;
    std::thread other([&] { other_thread_is_restricted = NonInteractiveAuthenticationScope::isActive(); });
    other.join();
    EXPECT_FALSE(other_thread_is_restricted);

    {
        NonInteractiveAuthenticationScope nested;
        EXPECT_TRUE(NonInteractiveAuthenticationScope::isActive());
    }
    EXPECT_TRUE(NonInteractiveAuthenticationScope::isActive());
}

#endif
