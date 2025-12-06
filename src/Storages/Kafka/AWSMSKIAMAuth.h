#pragma once

#include <base/types.h>
#include <Common/Logger.h>
#include <memory>

namespace cppkafka
{
class Configuration;
}

namespace DB
{

/// AWS MSK IAM SASL/OAUTHBEARER authentication handler
///
/// Implements AWS MSK IAM authentication by intercepting rdkafka.sasl.mechanism=AWS_MSK_IAM
/// configuration and converting it to OAUTHBEARER with AWS IAM token generation.
///
/// Implementation:
/// - Uses AWS SDK AWSAuthV4Signer for SigV4 presigned URLs
/// - Token format: Base64-URL encoded presigned URL per AWS MSK IAM spec
/// - Token lifetime: 5 minutes with auto-refresh at 80% (librdkafka handles refresh)
/// - Memory safe: shared_ptr for context shared across multiple rd_kafka_t instances
class AWSMSKIAMAuth
{
public:
    /// Configure Kafka client with AWS MSK IAM OAuth callbacks
    /// Called when rdkafka.sasl.mechanism=AWS_MSK_IAM is detected
    /// @param config Kafka configuration object to update
    /// @param region AWS region for the MSK cluster
    /// @param broker_list Comma-separated list of broker addresses
    /// @param log Logger instance
    static void configureOAuthCallbacks(
        cppkafka::Configuration & config,
        const String & region,
        const String & broker_list,
        LoggerPtr log);

    /// Extract AWS region from MSK broker address
    /// @param broker_address Broker address like "b-1.cluster.kafka.us-east-1.amazonaws.com:9098"
    /// @return Extracted region (e.g., "us-east-1") or empty string if not found
    static String extractRegionFromBroker(const String & broker_address);
};

}
