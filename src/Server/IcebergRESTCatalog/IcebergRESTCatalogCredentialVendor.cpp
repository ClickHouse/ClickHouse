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

#include <aws/core/Region.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace IcebergRESTCatalog
{

CredentialVendor::CredentialVendor(
    const std::shared_ptr<S3::AWSAssumeRoleClient> & sts_client_,
    IcebergRESTCatalogWarehouseCredential warehouse_role_,
    std::optional<String> s3_endpoint_,
    const std::shared_ptr<const S3::Client> & warehouse_s3_client_)
    : sts_client(sts_client_)
    , warehouse_role(std::move(warehouse_role_))
    , s3_endpoint(std::move(s3_endpoint_))
    , warehouse_s3_client(warehouse_s3_client_)
{
}

std::optional<VendedCredentials> CredentialVendor::Cache::get(const String & location, VendedAccessLevel access)
{
    const auto now_ms = nowMs();

    std::lock_guard lock(mutex);
    const auto it = entries.find(Key{.location = location, .access = access});
    if (it == entries.end() || it->second.reuse_until_ms <= now_ms)
        return std::nullopt;
    return it->second.credentials;
}

void CredentialVendor::Cache::put(
    const String & location, VendedAccessLevel access, const VendedCredentials & credentials, Int64 expires_at_ms)
{
    const auto now_ms = nowMs();
    /// Credentials are reused for first half of their lifetime, so a client always gets other half.
    const auto reuse_until_ms = now_ms + (expires_at_ms - now_ms) / 2;

    std::lock_guard lock(mutex);
    std::erase_if(entries, [&](const auto & entry) { return entry.second.reuse_until_ms <= now_ms; });
    entries[Key{.location = location, .access = access}] = Entry{.credentials = credentials, .reuse_until_ms = reuse_until_ms};
}

VendedCredentials CredentialVendor::vend(const String & location, VendedAccessLevel access) const
{
#if USE_AWS_S3
    if (auto cached = credentials_cache.get(location, access))
        return *cached;

    /// Concurrent misses for one key each call STS. The last one wins the cache slot. All of them are valid.
    const auto sts_result = assumeRole(location, access);
    auto credentials = toVendedCredentials(location, sts_result);
    credentials_cache.put(location, access, credentials, sts_result.getExpiration().Millis());
    return credentials;
#else
    UNUSED(location, access);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Credential vending needs S3 support, but this build has none");
#endif
}

#if USE_AWS_S3
/// TODO: A bucket with SSE-KMS also needs `kms:Decrypt` and `kms:GenerateDataKey` in session policy.
String CredentialVendor::buildSessionPolicy(const String & partition, const String & bucket, const String & key, VendedAccessLevel access)
{
    /// Bucket is in same partition as role.
    const auto bucket_arn = fmt::format("arn:{}:s3:::{}", partition, bucket);
    const auto prefix = key.empty() ? String() : key + "/";

    /// Read objects of table. Write them too with `ReadWrite`.
    Poco::JSON::Array object_actions;
    object_actions.add("s3:GetObject");
    if (access == VendedAccessLevel::ReadWrite)
    {
        object_actions.add("s3:PutObject");
        object_actions.add("s3:DeleteObject");
        object_actions.add("s3:AbortMultipartUpload");
    }

    Poco::JSON::Object object_statement;
    object_statement.set("Effect", "Allow");
    object_statement.set("Action", object_actions);
    object_statement.set("Resource", fmt::format("{}/{}*", bucket_arn, prefix));

    /// List objects of table. `ListBucket` is a bucket-level action, so table prefix goes to a condition.
    Poco::JSON::Object prefix_condition;
    prefix_condition.set("s3:prefix", fmt::format("{}*", prefix));
    Poco::JSON::Object condition;
    condition.set("StringLike", prefix_condition);

    Poco::JSON::Object list_statement;
    list_statement.set("Effect", "Allow");
    list_statement.set("Action", "s3:ListBucket");
    list_statement.set("Resource", bucket_arn);
    list_statement.set("Condition", condition);

    /// Clients call `GetBucketLocation` to find region before first read.
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

S3::AssumeRoleResult CredentialVendor::assumeRole(const String & location, VendedAccessLevel access) const
{
    const auto [bucket, key] = IcebergRESTCatalogS3Location::parse(location);

    /// Build request.
    S3::AssumeRoleRequest request(
        Aws::String(warehouse_role.role_arn),
        Aws::String(warehouse_role.getSessionName()),
        Aws::String(warehouse_role.external_id),
        Aws::String(buildSessionPolicy(warehouse_role.getPartition(), bucket, key, access)));

    /// Send request.
    const auto outcome = sts_client->assumeRole(request);
    if (!outcome.IsSuccess())
        throw Exception(
            ErrorCodes::S3_ERROR,
            "Cannot assume role {} to vend credentials for {}: {}",
            warehouse_role.role_arn,
            location,
            outcome.GetError().GetMessage());
    const auto & result = outcome.GetResult();

    /// Verify.
    if (result.getAccessKeyID().empty() || result.getSecretAccessKey().empty())
        throw Exception(
            ErrorCodes::S3_ERROR, "STS returned no credentials for role {} to vend credentials for {}", warehouse_role.role_arn, location);
    if (result.getExpiration().Millis() == 0)
        throw Exception(
            ErrorCodes::S3_ERROR, "STS returned no expiration time for role {} to vend credentials for {}", warehouse_role.role_arn, location);

    return result;
}

VendedCredentials CredentialVendor::toVendedCredentials(const String & location, const S3::AssumeRoleResult & sts_result) const
{
    VendedCredentials credentials;
    credentials.prefix = location;
    credentials.config["s3.access-key-id"] = sts_result.getAccessKeyID();
    credentials.config["s3.secret-access-key"] = sts_result.getSecretAccessKey();
    if (!sts_result.getSessionToken().empty())
        credentials.config["s3.session-token"] = sts_result.getSessionToken();
    credentials.config["s3.session-token-expires-at-ms"] = toString(sts_result.getExpiration().Millis());
    if (s3_endpoint)
    {
        credentials.config["s3.endpoint"] = *s3_endpoint;
        credentials.config["s3.path-style-access"] = "true";
    }

    const auto bucket = IcebergRESTCatalogS3Location::parse(location).bucket;
    /// Region is `AWS_GLOBAL` by default. Admin can set a real one in `<s3>` server config, but usually does not.
    auto region = warehouse_s3_client->getRegion();
    if (region.empty() || region == Aws::Region::AWS_GLOBAL)
        region = warehouse_s3_client->getRegionForBucket(bucket, /* force_detect */ true);
    if (!region.empty())
        credentials.config["client.region"] = region;

    return credentials;
}
#endif

}

}
