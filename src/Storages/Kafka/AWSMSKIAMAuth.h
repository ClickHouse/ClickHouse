#pragma once

#include "config.h"

#if USE_AWS_S3

#include <base/types.h>
#include <Common/Logger.h>
#include <cppkafka/configuration.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <chrono>

namespace DB::S3 { class S3CredentialsProviderChain; }

namespace DB::AWSMSKIAMAuth
{

struct OAuthBearerTokenRefreshContext
{
    LoggerPtr log;
    std::shared_ptr<S3::S3CredentialsProviderChain> provider;
    String region;
};

/// Longest lifetime advertised to librdkafka for one OAUTHBEARER token, used when the
/// credentials that sign it have at least this much validity left.
inline constexpr std::chrono::seconds TOKEN_LIFETIME{300};

/// How much validity credentials expiring at `expiration` have left. Non-positive when they
/// have already expired, which the caller must reject instead of signing a token with them.
/// Truncates towards zero, so it never reports more time than the credentials really have.
/// Credentials with no expiry (`Aws::Auth::AWSCredentials` defaults `expiration` to
/// `std::chrono::system_clock::time_point::max()`) report a large positive value rather than
/// overflowing.
std::chrono::seconds credentialsRemainingValidity(
    std::chrono::system_clock::time_point expiration,
    std::chrono::system_clock::time_point now);

/// Lifetime to advertise to librdkafka for a token signed with credentials that have
/// `remaining` validity left. Requires `remaining` to be positive.
///
/// librdkafka schedules the next refresh at 80% of the lifetime it is given, not at the end of
/// it, so a lifetime longer than `remaining` produces a token still advertised as valid after
/// the credentials that signed it have died. The broker then rejects every request from the
/// credentials' expiry until the refresh after that one. Capping by `remaining` keeps the next
/// refresh strictly before the expiry, where the credentials provider hands out a fresh set.
std::chrono::seconds advertisedTokenLifetime(std::chrono::seconds remaining);

/// Extract AWS region from MSK broker hostname.
/// Matches patterns: *.kafka[-serverless].<region>[.vpce].amazonaws.com
String extractRegionFromBroker(const String & broker_address);

/// Returns true if region looks like a valid AWS region (e.g. us-east-1, us-gov-west-1).
bool isValidAWSRegion(const String & region);

/// Setup AWS MSK IAM authentication for Kafka
/// This configures librdkafka to use OAUTHBEARER with a callback
/// that generates AWS MSK IAM tokens on-demand from configuration
///
/// @param kafka_config cppkafka Configuration object to modify
/// @param config ClickHouse server configuration
/// @param region AWS region (if empty, will auto-detect from broker_list)
/// @param broker_list Comma-separated broker addresses (for region auto-detection)
/// @param log Logger instance
/// @param context_holder Shared pointer to hold the context, ensuring its lifetime matches the storage
void setupAuthentication(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & region,
    const String & broker_list,
    LoggerPtr log,
    std::shared_ptr<OAuthBearerTokenRefreshContext> & context_holder);

}

#endif // USE_AWS_S3
