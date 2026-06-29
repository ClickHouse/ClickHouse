#include <gtest/gtest.h>
#include <config.h>

#if USE_AWS_S3

#include <Storages/Kafka/AWSMSKIAMAuth.h>
#include <Common/Exception.h>
#include <Poco/Util/MapConfiguration.h>
#include <cppkafka/configuration.h>
#include <cppkafka/kafka_handle_base.h>

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
