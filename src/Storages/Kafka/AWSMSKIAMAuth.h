#pragma once

#include "config.h"

#if USE_AWS_S3

#include <base/types.h>
#include <Common/Logger.h>
#include <cppkafka/configuration.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::AWSMSKIAMAuth
{

/// Extract AWS region from MSK broker hostname
/// Matches patterns: *.kafka[-serverless].<region>[.vpce].amazonaws.com
String extractRegionFromBroker(const String & broker_address);

/// Setup AWS MSK IAM authentication for Kafka
/// This configures librdkafka to use OAUTHBEARER with a callback
/// that generates AWS MSK IAM tokens on-demand from configuration
///
/// @param kafka_config cppkafka Configuration object to modify
/// @param config ClickHouse server configuration
/// @param region AWS region (if empty, will auto-detect from broker_list)
/// @param broker_list Comma-separated broker addresses (for region auto-detection)
/// @param log Logger instance
void setupAuthentication(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & region,
    const String & broker_list,
    LoggerPtr log);

}

#endif // USE_AWS_S3
