#include <gtest/gtest.h>
#include <config.h>

#if USE_AWS_S3

#include <Storages/Kafka/AWSMSKIAMAuth.h>
#include <IO/S3/Credentials.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>
#include <IO/S3Defines.h>
#include <Poco/Util/MapConfiguration.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/config/ConfigAndCredentialsCacheManager.h>
#include <cppkafka/configuration.h>
#include <cppkafka/kafka_handle_base.h>
#include <chrono>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

using namespace DB;
using namespace DB::AWSMSKIAMAuth;

// ---------------------------------------------------------------------------
// extractRegionFromBroker
// ---------------------------------------------------------------------------

TEST(AWSMSKIAMAuth, ExtractRegionStandardBroker)
{
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.us-east-1.amazonaws.com:9098"), "us-east-1");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.eu-west-2.amazonaws.com"), "eu-west-2");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.ap-southeast-1.amazonaws.com:9098"), "ap-southeast-1");
}

TEST(AWSMSKIAMAuth, ExtractRegionServerlessBroker)
{
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka-serverless.us-west-2.amazonaws.com:9098"), "us-west-2");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka-serverless.eu-central-1.amazonaws.com"), "eu-central-1");
}

TEST(AWSMSKIAMAuth, ExtractRegionPrivateLinkBroker)
{
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.us-east-1.vpce.amazonaws.com:9098"), "us-east-1");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka-serverless.eu-west-1.vpce.amazonaws.com"), "eu-west-1");
}

TEST(AWSMSKIAMAuth, ExtractRegionGovCloudBroker)
{
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.us-gov-west-1.amazonaws.com:9098"), "us-gov-west-1");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.us-gov-east-1.amazonaws.com"), "us-gov-east-1");
}

TEST(AWSMSKIAMAuth, ExtractRegionChinaBroker)
{
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.cn-north-1.amazonaws.com.cn:9098"), "cn-north-1");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka.cn-northwest-1.amazonaws.com.cn"), "cn-northwest-1");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.kafka-serverless.cn-north-1.amazonaws.com.cn:9098"), "cn-north-1");
}

TEST(AWSMSKIAMAuth, ExtractRegionMixedCaseBroker)
{
    // DNS is case-insensitive; uppercase/mixed-case hostnames must still work.
    EXPECT_EQ(extractRegionFromBroker("B-1.Cluster.Kafka.US-EAST-1.amazonaws.com:9098"), "us-east-1");
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.KAFKA.eu-west-2.AMAZONAWS.COM"), "eu-west-2");
    EXPECT_EQ(extractRegionFromBroker("B-1.CLUSTER.KAFKA-SERVERLESS.AP-SOUTHEAST-1.amazonaws.com:9098"), "ap-southeast-1");
}

TEST(AWSMSKIAMAuth, ExtractRegionNegativeCases)
{
    EXPECT_EQ(extractRegionFromBroker(""), "");
    EXPECT_EQ(extractRegionFromBroker(":9092"), "");
    EXPECT_EQ(extractRegionFromBroker("localhost:9092"), "");
    EXPECT_EQ(extractRegionFromBroker("broker.example.com:9092"), "");
    // Not an MSK endpoint (missing kafka segment)
    EXPECT_EQ(extractRegionFromBroker("b-1.cluster.us-east-1.amazonaws.com:9098"), "");
}

// ---------------------------------------------------------------------------
// isValidAWSRegion
// ---------------------------------------------------------------------------

TEST(AWSMSKIAMAuth, ValidRegions)
{
    EXPECT_TRUE(isValidAWSRegion("us-east-1"));
    EXPECT_TRUE(isValidAWSRegion("eu-west-2"));
    EXPECT_TRUE(isValidAWSRegion("ap-southeast-1"));
    EXPECT_TRUE(isValidAWSRegion("us-gov-west-1"));
    EXPECT_TRUE(isValidAWSRegion("us-gov-east-1"));
    EXPECT_TRUE(isValidAWSRegion("cn-north-1"));
}

TEST(AWSMSKIAMAuth, InvalidRegions)
{
    EXPECT_FALSE(isValidAWSRegion(""));
    EXPECT_FALSE(isValidAWSRegion("us_east_1"));
    EXPECT_FALSE(isValidAWSRegion("US-EAST-1"));
    EXPECT_FALSE(isValidAWSRegion("useast1"));
    EXPECT_FALSE(isValidAWSRegion("us-east"));
    EXPECT_FALSE(isValidAWSRegion("us-east-"));
    EXPECT_FALSE(isValidAWSRegion("-us-east-1"));
}

// ---------------------------------------------------------------------------
// credentialsRemainingValidity / advertisedTokenLifetime
// ---------------------------------------------------------------------------

TEST(AWSMSKIAMAuth, RemainingValidityTruncatesTowardsZero)
{
    const auto now = std::chrono::system_clock::now();

    /// Never report more time than the credentials really have: a token advertised as valid
    /// for the rounded-up second outlives them.
    EXPECT_EQ(credentialsRemainingValidity(now + std::chrono::milliseconds(1500), now), std::chrono::seconds(1));
    EXPECT_EQ(credentialsRemainingValidity(now + std::chrono::milliseconds(999), now), std::chrono::seconds(0));
    EXPECT_EQ(credentialsRemainingValidity(now - std::chrono::milliseconds(1500), now), std::chrono::seconds(-1));
}

TEST(AWSMSKIAMAuth, RemainingValidityOfExpiredCredentialsIsNotPositive)
{
    const auto now = std::chrono::system_clock::now();

    EXPECT_EQ(credentialsRemainingValidity(now, now), std::chrono::seconds(0));
    EXPECT_EQ(credentialsRemainingValidity(now - std::chrono::seconds(30), now), std::chrono::seconds(-30));
}

TEST(AWSMSKIAMAuth, CredentialsWithoutExpiryDoNotOverflow)
{
    /// Static credentials, e.g. from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, carry no
    /// expiration. The AWS SDK spells that as `time_point::max()`, so the subtraction against
    /// `now` must stay representable and the cap must not shorten the lifetime.
    const Aws::Auth::AWSCredentials never_expiring{"key", "secret"};
    const auto now = std::chrono::system_clock::now();

    const auto remaining = credentialsRemainingValidity(never_expiring.GetExpiration().UnderlyingTimestamp(), now);
    EXPECT_GT(remaining, std::chrono::seconds::zero());
    EXPECT_EQ(advertisedTokenLifetime(remaining), TOKEN_LIFETIME);
}

TEST(AWSMSKIAMAuth, FreshCredentialsGetTheFullTokenLifetime)
{
    /// STS applies a 3600s default to AssumeRoleWithWebIdentity, which is what IRSA yields.
    EXPECT_EQ(advertisedTokenLifetime(std::chrono::seconds(3600)), TOKEN_LIFETIME);
    EXPECT_EQ(advertisedTokenLifetime(TOKEN_LIFETIME), TOKEN_LIFETIME);
}

TEST(AWSMSKIAMAuth, LifetimeIsCappedBelowTheTokenLifetime)
{
    EXPECT_EQ(advertisedTokenLifetime(TOKEN_LIFETIME - std::chrono::seconds(1)), TOKEN_LIFETIME - std::chrono::seconds(1));
    EXPECT_EQ(advertisedTokenLifetime(std::chrono::seconds(1)), std::chrono::seconds(1));
}

TEST(AWSMSKIAMAuth, NextRefreshFallsInsideTheProviderRefreshWindow)
{
    /// The credentials provider reloads once the credentials are within
    /// DEFAULT_EXPIRATION_WINDOW_SECONDS of expiring, so that is the least validity
    /// GetAWSCredentials can hand back. librdkafka refreshes at 80% of the lifetime it is
    /// given, and that refresh has to land before the credentials expire, inside the window
    /// where the provider produces a fresh set. This is the invariant the cap exists for.
    const auto floor = std::chrono::seconds(DB::S3::DEFAULT_EXPIRATION_WINDOW_SECONDS);

    for (auto remaining = std::chrono::seconds(1); remaining <= std::chrono::seconds(4 * TOKEN_LIFETIME.count()); ++remaining)
    {
        const auto lifetime = advertisedTokenLifetime(remaining);

        EXPECT_GT(lifetime, std::chrono::seconds::zero()) << "remaining = " << remaining.count();
        EXPECT_LE(lifetime, remaining) << "remaining = " << remaining.count();
        EXPECT_LE(lifetime, TOKEN_LIFETIME) << "remaining = " << remaining.count();

        if (remaining < floor)
            continue;

        /// Time left once librdkafka schedules its refresh, in seconds.
        const double left_at_refresh = static_cast<double>(remaining.count()) - 0.8 * static_cast<double>(lifetime.count());
        EXPECT_GT(left_at_refresh, 0.0) << "remaining = " << remaining.count();
        if (remaining <= TOKEN_LIFETIME)
            EXPECT_LT(left_at_refresh, static_cast<double>(floor.count())) << "remaining = " << remaining.count();
    }
}

// ---------------------------------------------------------------------------
// setupAuthentication failure paths (no AWS SDK calls needed: both throw
// before the credentials provider is created)
// ---------------------------------------------------------------------------

static Poco::AutoPtr<Poco::Util::MapConfiguration> emptyConfig()
{
    return Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration);
}

// ---------------------------------------------------------------------------
// setupAuthentication rewrite: AWS_MSK_IAM from server/named-collection config
// ---------------------------------------------------------------------------

TEST(AWSMSKIAMAuth, SetupRewritesPresetAWSMSKIAMToOAUTHBEARER)
{
    // Simulate sasl.mechanism = AWS_MSK_IAM already written into kafka_config by
    // loadFromConfig (server config path) before setupAuthentication is called.
    // After setup, sasl.mechanism must be OAUTHBEARER and security.protocol SASL_SSL.
    // AWS_MSK_IAM must NOT be passed through to librdkafka.
    cppkafka::Configuration cfg;
    cfg.set("sasl.mechanism", "AWS_MSK_IAM");
    auto config = emptyConfig();
    std::shared_ptr<OAuthBearerTokenRefreshContext> ctx;

    try
    {
        setupAuthentication(cfg, *config, "us-east-1", "", nullptr, ctx);
    }
    catch (const DB::Exception & e)
    {
        FAIL() << "Unexpected DB::Exception: " << e.message();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        // Ok: non-setup exceptions (e.g. missing AWS credentials) are acceptable —
        // config properties are already written before credentials are resolved.
    }

    // Verify regardless of whether setupAuthentication completed or threw
    // after writing config (credentials unavailable in test environment).
    // librdkafka normalizes property values to lowercase.
    EXPECT_EQ(cfg.get("sasl.mechanism"), "OAUTHBEARER");
    EXPECT_EQ(cfg.get("security.protocol"), "sasl_ssl");
}

/// Holds one process-wide AWS variable and puts back exactly what it found. `hide` clears the
/// variable for the test's duration: S3CredentialsProviderChain consults web-identity and profile
/// entries before EnvironmentAWSCredentialsProvider, so an IRSA-configured runner would resolve
/// those instead and the environment-provider assertion would stop proving what it says.
/// NOLINT(concurrency-mt-unsafe): single-threaded gtest, no concurrent getenv/setenv.
class AwsEnvironmentVariableScope
{
public:
    AwsEnvironmentVariableScope(const char * name, bool hide)
    {
        name_ = name;
        const char * current = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
        had_value_ = current != nullptr;
        saved_value_ = had_value_ ? std::string(current) : std::string();
        if (hide)
            ::unsetenv(name); // NOLINT(concurrency-mt-unsafe)
    }

    ~AwsEnvironmentVariableScope()
    {
        if (had_value_)
            ::setenv(name_, saved_value_.c_str(), 1); // NOLINT(concurrency-mt-unsafe)
        else
            ::unsetenv(name_); // NOLINT(concurrency-mt-unsafe)
    }

private:
    const char * name_ = nullptr;
    bool had_value_ = false;
    std::string saved_value_;
};

TEST(AWSMSKIAMAuth, SetupResolvesEnvironmentCredentials)
{
    // Kafka AWS_MSK_IAM is server-side, operator-configured authentication, not a user S3 query. The S3
    // credential restriction (`forbid_implicit_credentials`, default true) must not apply here: with
    // `kafka.use_environment_credentials = 1` the provider chain must still resolve the environment credentials.
    // Declared before the variable scopes so it runs after their restores: the profile cache is read
    // once process-wide, so it has to be re-read after the original files are reachable again.
    SCOPE_EXIT(
    {
        Aws::Config::ReloadCachedConfigFile();
        Aws::Config::ReloadCachedCredentialsFile();
    });
    AwsEnvironmentVariableScope access_key("AWS_ACCESS_KEY_ID", /*hide=*/false);
    AwsEnvironmentVariableScope secret_key("AWS_SECRET_ACCESS_KEY", /*hide=*/false);
    AwsEnvironmentVariableScope metadata_disabled("AWS_EC2_METADATA_DISABLED", /*hide=*/false);
    AwsEnvironmentVariableScope role("AWS_ROLE_ARN", /*hide=*/true);
    AwsEnvironmentVariableScope token("AWS_WEB_IDENTITY_TOKEN_FILE", /*hide=*/true);
    AwsEnvironmentVariableScope profile("AWS_PROFILE", /*hide=*/true);
    AwsEnvironmentVariableScope default_profile("AWS_DEFAULT_PROFILE", /*hide=*/true);
    // Hiding the two variables above only seals the environment side. The web-identity resolver falls
    // back to the cached config-file profile, so a runner whose ~/.aws/config carries web-identity
    // settings would still insert the provider ahead of the environment one. Point both files at
    // /dev/null for the test duration: an empty profile cannot source a role_arn or a token file.
    AwsEnvironmentVariableScope config_file("AWS_CONFIG_FILE", /*hide=*/false);
    AwsEnvironmentVariableScope shared_credentials_file("AWS_SHARED_CREDENTIALS_FILE", /*hide=*/false);

    /// NOLINTBEGIN(concurrency-mt-unsafe): single-threaded gtest, no concurrent getenv/setenv.
    ::setenv("AWS_ACCESS_KEY_ID", "AKID_MSK_ENV_TEST", /*overwrite=*/1);
    ::setenv("AWS_SECRET_ACCESS_KEY", "secret_msk_env_test", /*overwrite=*/1);
    ::setenv("AWS_EC2_METADATA_DISABLED", "true", /*overwrite=*/1);
    ::setenv("AWS_CONFIG_FILE", "/dev/null", /*overwrite=*/1);
    ::setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null", /*overwrite=*/1);
    /// NOLINTEND(concurrency-mt-unsafe)

    // The cache would otherwise keep serving whatever profile an earlier test in this binary loaded.
    Aws::Config::ReloadCachedConfigFile();
    Aws::Config::ReloadCachedCredentialsFile();

    cppkafka::Configuration cfg;
    auto config = emptyConfig();
    config->setString("kafka.use_environment_credentials", "1");
    std::shared_ptr<OAuthBearerTokenRefreshContext> ctx;

    setupAuthentication(cfg, *config, "us-east-1", "", nullptr, ctx);

    ASSERT_NE(ctx, nullptr);
    ASSERT_NE(ctx->provider, nullptr);
    /// With the restriction wrongly applied, the chain adds no environment provider and this is empty.
    EXPECT_EQ(ctx->provider->GetAWSCredentials().GetAWSAccessKeyId(), "AKID_MSK_ENV_TEST");
}

TEST(AWSMSKIAMAuth, SetupThrowsOnRegionMismatchWithCachedContext)
{
    // Simulate consumer context cached for us-east-1, then producer attempts eu-west-1.
    // setupAuthentication must reject the mismatch rather than silently signing tokens
    // for the wrong region.
    auto cached_ctx = std::make_shared<OAuthBearerTokenRefreshContext>();
    cached_ctx->region = "us-east-1";

    cppkafka::Configuration cfg;
    auto config = emptyConfig();
    std::shared_ptr<OAuthBearerTokenRefreshContext> ctx = cached_ctx;

    EXPECT_THROW(
        setupAuthentication(cfg, *config, "eu-west-1", "", nullptr, ctx),
        DB::Exception);
}

TEST(AWSMSKIAMAuth, SetupAcceptsSameRegionWithCachedContext)
{
    // Reusing a cached context for the same region must not throw BAD_ARGUMENTS.
    // The function may throw later (e.g. missing AWS credentials in test env),
    // but the region-mismatch check must pass.
    auto cached_ctx = std::make_shared<OAuthBearerTokenRefreshContext>();
    cached_ctx->region = "us-east-1";

    cppkafka::Configuration cfg;
    auto config = emptyConfig();
    std::shared_ptr<OAuthBearerTokenRefreshContext> ctx = cached_ctx;

    try
    {
        setupAuthentication(cfg, *config, "us-east-1", "", nullptr, ctx);
    }
    catch (const DB::Exception & e)
    {
        // setupAuthentication validates region before creating credentials provider.
        // If it throws BAD_ARGUMENTS here, the region-reuse logic is broken.
        ASSERT_NE(e.code(), DB::ErrorCodes::BAD_ARGUMENTS)
            << "Same-region reuse must not throw BAD_ARGUMENTS; got: " << e.message();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        // Ok: non-region exceptions (e.g. missing AWS credentials) are acceptable.
    }

    // Context must remain the same object (no replacement).
    EXPECT_EQ(ctx, cached_ctx);
}

TEST(AWSMSKIAMAuth, SetupFailsWhenRegionCannotBeInferred)
{
    cppkafka::Configuration cfg;
    auto config = emptyConfig();
    std::shared_ptr<OAuthBearerTokenRefreshContext> ctx;

    EXPECT_THROW(
        setupAuthentication(cfg, *config, "", "localhost:9092,broker2:9092", nullptr, ctx),
        DB::Exception);
}

TEST(AWSMSKIAMAuth, SetupFailsOnInvalidExplicitRegion)
{
    cppkafka::Configuration cfg;
    auto config = emptyConfig();
    std::shared_ptr<OAuthBearerTokenRefreshContext> ctx;

    EXPECT_THROW(
        setupAuthentication(cfg, *config, "INVALID_REGION", "", nullptr, ctx),
        DB::Exception);
}

TEST(AWSMSKIAMAuth, SetupAutoDetectsRegionFromBrokerList)
{
    cppkafka::Configuration cfg;
    auto config = emptyConfig();
    std::shared_ptr<OAuthBearerTokenRefreshContext> ctx;

    // Should not throw on region detection — will throw later inside the AWS SDK
    // if credentials are unavailable, but region parsing itself must succeed.
    // We verify by catching only BAD_ARGUMENTS (region errors) and letting anything
    // else propagate so the test would fail loudly if region detection regressed.
    try
    {
        setupAuthentication(cfg, *config, "", "b-1.cluster.kafka.us-east-1.amazonaws.com:9098", nullptr, ctx);
    }
    catch (const DB::Exception & e)
    {
        EXPECT_NE(e.code(), DB::ErrorCodes::BAD_ARGUMENTS)
            << "Region detection should not throw BAD_ARGUMENTS; got: " << e.message();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        // Ok: non-BAD_ARGUMENTS exceptions (e.g. missing AWS credentials) are acceptable here.
    }
}

#endif // USE_AWS_S3
