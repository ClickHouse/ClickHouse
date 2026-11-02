#pragma once

#include <base/types.h>
#include <Common/logger_useful.h>

#include <map>
#include <memory>
#include <mutex>

namespace DB::S3
{
class Client;
class AWSAssumeRoleClient;
}

namespace DB
{

/// The `vended_credentials` section of the catalog config.
struct IcebergRESTCatalogVendingConfig
{
    String role_arn;
    String role_session_name;
    String external_id;
    /// STS endpoint for non-AWS storage, such as MinIO.
    String sts_endpoint;
    /// S3 endpoint that clients use. The server's own endpoint may be internal, so it is never advertised.
    String s3_endpoint;
};

enum class IcebergRESTCatalogVendedAccess : uint8_t
{
    ReadOnly,
    ReadWrite,
};

/// Temporary S3 credentials for one table prefix, in the shape of the Iceberg REST `config` map.
struct IcebergRESTCatalogVendedCredentials
{
    /// The table location, also the `prefix` of a `StorageCredential`.
    String prefix;
    std::map<String, String> config;
    Int64 expires_at_ms = 0;
};

/// Issues table-scoped S3 credentials through STS `AssumeRole` with a session policy (RFC: issue #114697).
/// The server assumes the configured role as itself and narrows the session to the table location.
class IcebergRESTCatalogCredentialVendor
{
public:
    IcebergRESTCatalogCredentialVendor(IcebergRESTCatalogVendingConfig config_, std::shared_ptr<const S3::Client> s3_client_);

    /// Reuses credentials for the same prefix and access while they are not about to expire.
    IcebergRESTCatalogVendedCredentials vend(const String & location, const String & bucket, const String & key, IcebergRESTCatalogVendedAccess access) const;

private:
    struct CacheKey
    {
        String location;
        IcebergRESTCatalogVendedAccess access;
        auto operator<=>(const CacheKey &) const = default;
    };

    const IcebergRESTCatalogVendingConfig config;
    const std::shared_ptr<const S3::Client> s3_client;
    std::shared_ptr<S3::AWSAssumeRoleClient> sts_client;

    mutable std::mutex mutex;
    mutable std::map<CacheKey, IcebergRESTCatalogVendedCredentials> cache;

    LoggerPtr log;
};

}
