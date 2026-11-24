#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCredentialVendor.h>

#include "config.h"

#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <optional>
#include <sstream>

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
    const std::shared_ptr<const S3::Client> & warehouse_s3_client_)
    : sts_client(sts_client_)
    , warehouse_role(std::move(warehouse_role_))
    , warehouse_s3_client(warehouse_s3_client_)
{
}

VendedCredentials CredentialVendor::vend(const String & location, VendedAccessLevel access) const
{
#if USE_AWS_S3
    const auto sts_result = assumeRole(location, access);
    return toVendedCredentials(location, sts_result);
#else
    UNUSED(location, access);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Credential vending needs S3 support, but this build has none");
#endif
}

#if USE_AWS_S3
namespace
{

/// IAM reads `*` and `?` as wildcards and `${...}` as a variable. `${*}`, `${?}` and `${$}` mean the plain characters.
String escapeForIAM(const String & value)
{
    String result;
    for (const char c : value)
    {
        if (c == '*' || c == '?' || c == '$')
            result += fmt::format("${{{}}}", c);
        else
            result += c;
    }
    return result;
}

/// One statement of a policy. `NotAction` and `NotResource` as keys mean "all except these".
Poco::JSON::Object makeStatement(
    const String & effect,
    const String & action_key,
    const Poco::Dynamic::Var & actions,
    const String & resource_key,
    const Poco::Dynamic::Var & resources,
    const std::optional<Poco::JSON::Object> & condition = {})
{
    Poco::JSON::Object statement;
    statement.set("Effect", effect);
    statement.set(action_key, actions);
    statement.set(resource_key, resources);
    if (condition)
        statement.set("Condition", *condition);
    return statement;
}

}

/// Session policy has three `Allow` statements:
///   1. `GetObject` (and write actions with `ReadWrite`) on `<bucket>/<key>/*`.
///   2. `ListBucket` on bucket, limited to prefix `<key>/`.
///   3. `GetBucketLocation` on bucket.
/// And three `Deny` statements for everything else:
///   4. Actions outside allowed set.
///   5. Other buckets and objects outside table.
///   6. Listing outside prefix `<key>/`.
String CredentialVendor::buildSessionPolicy(const String & bucket, const String & key, VendedAccessLevel access) const
{
    /// Bucket is in same partition as role.
    const auto bucket_arn = fmt::format("arn:{}:s3:::{}", warehouse_role.getPartition(), bucket);
    /// Namespace and table names can have any characters.
    const auto table_prefix = key.empty() ? String() : escapeForIAM(key) + "/";
    const auto table_objects_arn = fmt::format("{}/{}*", bucket_arn, table_prefix);

    /// Actions on objects of table.
    /// TODO: `kms:Decrypt` and `kms:GenerateDataKey` for a bucket with SSE-KMS.
    Poco::JSON::Array object_actions;
    object_actions.add("s3:GetObject");
    if (access == VendedAccessLevel::ReadWrite)
    {
        object_actions.add("s3:PutObject");
        object_actions.add("s3:DeleteObject");
        object_actions.add("s3:AbortMultipartUpload");
    }

    /// All actions a client can do.
    Poco::JSON::Array all_actions = object_actions;
    all_actions.add("s3:ListBucket");
    all_actions.add("s3:GetBucketLocation");

    /// All resources a client can touch. Bucket ARN names bucket itself, not objects in it.
    Poco::JSON::Array all_resources;
    all_resources.add(bucket_arn);
    all_resources.add(table_objects_arn);

    /// `ListBucket` acts on bucket, so table prefix goes to a condition on prefix that request asks for.
    auto prefix_condition = [&](const String & comparison)
    {
        Poco::JSON::Object prefix;
        prefix.set("s3:prefix", table_prefix + "*");
        Poco::JSON::Object condition;
        condition.set(comparison, prefix);
        return condition;
    };

    Poco::JSON::Array statements;

    /// 1. Allow reading (and writing) objects of table.
    statements.add(makeStatement("Allow", "Action", object_actions, "Resource", table_objects_arn));
    /// 2. Allow listing inside table.
    statements.add(makeStatement("Allow", "Action", "s3:ListBucket", "Resource", bucket_arn, prefix_condition("StringLike")));
    /// 3. Allow finding region of bucket. Clients do it before first read.
    statements.add(makeStatement("Allow", "Action", "s3:GetBucketLocation", "Resource", bucket_arn));

    /// A bucket policy can allow more than statements above. `Deny` always wins over `Allow`.
    /// 4. Deny all other actions.
    statements.add(makeStatement("Deny", "NotAction", all_actions, "Resource", "*"));
    /// 5. Deny all other buckets and objects.
    statements.add(makeStatement("Deny", "Action", "s3:*", "NotResource", all_resources));
    /// 6. Deny listing outside table. Listing without prefix is denied too.
    statements.add(makeStatement("Deny", "Action", "s3:ListBucket", "Resource", bucket_arn, prefix_condition("StringNotLike")));

    Poco::JSON::Object policy;
    policy.set("Version", "2012-10-17");
    policy.set("Statement", statements);

    /// STS accepts only characters up to U+00FF in a policy. IAM decodes `\uXXXX` back to original characters.
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(policy, oss, 0, -1, Poco::JSON_WRAP_STRINGS | Poco::JSON_ESCAPE_UNICODE);
    return oss.str();
}

S3::AssumeRoleResult CredentialVendor::assumeRole(const String & location, VendedAccessLevel access) const
{
    const auto [bucket, key] = IcebergRESTCatalogS3Location::parse(location);

    /// Build request.
    S3::AssumeRoleRequest request(
        Aws::String(warehouse_role.role_arn),
        Aws::String(warehouse_role.getSessionName()),
        Aws::String(warehouse_role.external_id),
        Aws::String(buildSessionPolicy(bucket, key, access)));

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
