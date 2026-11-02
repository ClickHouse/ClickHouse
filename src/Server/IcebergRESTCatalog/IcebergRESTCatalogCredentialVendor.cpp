#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCredentialVendor.h>

#include "config.h"

#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#if USE_AWS_S3
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3Defines.h>
#endif

namespace DB
{

using namespace IcebergRESTCatalog;

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// A session policy that limits the assumed role to one table prefix.
/// `ListBucket` is a bucket-level action, so it needs an explicit prefix condition.
String buildSessionPolicy(const String & bucket, const String & key, IcebergRESTCatalogVendedAccess access)
{
    const auto bucket_arn = fmt::format("arn:aws:s3:::{}", bucket);
    const auto prefix = key.empty() ? String() : key + "/";

    Poco::JSON::Array object_actions;
    object_actions.add("s3:GetObject");
    if (access == IcebergRESTCatalogVendedAccess::ReadWrite)
    {
        object_actions.add("s3:PutObject");
        object_actions.add("s3:DeleteObject");
        object_actions.add("s3:AbortMultipartUpload");
    }
    Poco::JSON::Object object_statement;
    object_statement.set("Effect", "Allow");
    object_statement.set("Action", object_actions);
    object_statement.set("Resource", fmt::format("{}/{}*", bucket_arn, prefix));

    Poco::JSON::Object prefix_condition;
    prefix_condition.set("s3:prefix", fmt::format("{}*", prefix));
    Poco::JSON::Object condition;
    condition.set("StringLike", prefix_condition);
    Poco::JSON::Object list_statement;
    list_statement.set("Effect", "Allow");
    list_statement.set("Action", "s3:ListBucket");
    list_statement.set("Resource", bucket_arn);
    list_statement.set("Condition", condition);

    /// Clients call `GetBucketLocation` to find the region before the first read.
    Poco::JSON::Object location_statement;
    location_statement.set("Effect", "Allow");
    location_statement.set("Action", "s3:GetBucketLocation");
    location_statement.set("Resource", bucket_arn);

    Poco::JSON::Array statements;
    statements.add(object_statement);
    statements.add(list_statement);
    statements.add(location_statement);

    Poco::JSON::Object policy;
    policy.set("Version", "2012-10-17");
    policy.set("Statement", statements);
    return toJSONString(policy);
}

}

IcebergRESTCatalogCredentialVendor::IcebergRESTCatalogCredentialVendor(
    IcebergRESTCatalogVendingConfig config_, std::shared_ptr<const S3::Client> s3_client_)
    : config(std::move(config_)), s3_client(std::move(s3_client_)), log(getLogger("IcebergRESTCatalogCredentialVendor"))
{
#if USE_AWS_S3
    /// The server assumes the role as itself, with the identity of the warehouse storage.
    sts_client = S3::AWSAssumeRoleClient::create(s3_client->getCredentialsProvider(), s3_client->getClientConfiguration(), config.sts_endpoint);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Credential vending needs S3 support, but this build has none");
#endif
}

IcebergRESTCatalogVendedCredentials IcebergRESTCatalogCredentialVendor::vend(
    [[maybe_unused]] const String & location,
    [[maybe_unused]] const String & bucket,
    [[maybe_unused]] const String & key,
    [[maybe_unused]] IcebergRESTCatalogVendedAccess access) const
{
#if USE_AWS_S3
    const CacheKey cache_key{.location = location, .access = access};
    /// Credentials this close to expiry count as expired, so a client gets time to use them.
    const auto expiry_cutoff_ms = nowMs() + static_cast<Int64>(S3::DEFAULT_EXPIRATION_WINDOW_SECONDS) * 1000;

    {
        std::lock_guard lock(mutex);
        if (const auto it = cache.find(cache_key); it != cache.end() && it->second.expires_at_ms > expiry_cutoff_ms)
            return it->second;
    }

    /// Concurrent misses for one key each call STS. The last one wins the cache slot, all of them are valid.
    /// TODO: `DurationSeconds` is not sent. STS uses the default of one hour.
    S3::AssumeRoleRequest request(config.role_arn, config.role_session_name, config.external_id, buildSessionPolicy(bucket, key, access));
    const auto outcome = sts_client->assumeRole(request);
    if (!outcome.IsSuccess())
        throw Exception(
            ErrorCodes::S3_ERROR,
            "Cannot assume role {} to vend credentials for {}: {}",
            config.role_arn,
            location,
            outcome.GetError().GetMessage());

    const auto & result = outcome.GetResult();
    IcebergRESTCatalogVendedCredentials credentials;
    credentials.prefix = location;
    credentials.expires_at_ms = result.getExpiration().Millis();
    credentials.config["s3.access-key-id"] = result.getAccessKeyID();
    credentials.config["s3.secret-access-key"] = result.getSecretAccessKey();
    credentials.config["s3.session-token"] = result.getSessionToken();
    credentials.config["s3.session-token-expires-at-ms"] = toString(credentials.expires_at_ms);
    if (!config.s3_endpoint.empty())
    {
        /// A custom endpoint is not AWS, so clients must address the bucket in the path.
        credentials.config["s3.endpoint"] = config.s3_endpoint;
        credentials.config["s3.path-style-access"] = "true";
    }
    if (const auto & region = s3_client->getRegion(); !region.empty())
        credentials.config["s3.region"] = region;

    LOG_DEBUG(
        log,
        "Assumed role {} for {} with {} access, credentials expire at {} ms",
        config.role_arn,
        location,
        access == IcebergRESTCatalogVendedAccess::ReadOnly ? "read-only" : "read-write",
        credentials.expires_at_ms);

    std::lock_guard lock(mutex);
    std::erase_if(cache, [&](const auto & entry) { return entry.second.expires_at_ms <= expiry_cutoff_ms; });
    cache[cache_key] = credentials;
    return credentials;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Credential vending needs S3 support, but this build has none");
#endif
}

}
