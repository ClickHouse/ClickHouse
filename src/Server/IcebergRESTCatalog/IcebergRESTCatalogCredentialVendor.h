#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>

namespace DB::S3
{
class Client;
class AWSAssumeRoleClient;
class AssumeRoleResult;
}

namespace DB::IcebergRESTCatalog
{

enum class VendedAccessLevel : uint8_t
{
    ReadOnly,
    ReadWrite,
};

/// Temporary S3 credentials for one table, shaped as an Iceberg REST `StorageCredential`.
struct VendedCredentials
{
    /// Table location.
    String prefix;
    /// S3 properties with key names from Iceberg REST spec, such as `s3.access-key-id`.
    std::map<String, String> config;
};

/// Issues temporary S3 credentials for tables of one warehouse (RFC: issue #114697).
class CredentialVendor
{
public:
    /// `sts_client_` contains AWS credentials of server. Needed to assume warehouse role.
    CredentialVendor(
        const std::shared_ptr<S3::AWSAssumeRoleClient> & sts_client_,
        IcebergRESTCatalogWarehouseCredential warehouse_role_,
        const std::shared_ptr<const S3::Client> & warehouse_s3_client_);

    /// Reuses credentials of same location and access level for first half of their lifetime.
    VendedCredentials vend(const String & location, VendedAccessLevel access) const;

private:
    /// Keeps credentials per location and access level for first half of their lifetime.
    class Cache
    {
    public:
        std::optional<VendedCredentials> get(const String & location, VendedAccessLevel access);
        void put(const String & location, VendedAccessLevel access, const VendedCredentials & credentials, Int64 expires_at_ms);

    private:
        struct Key
        {
            String location;
            VendedAccessLevel access;
            auto operator<=>(const Key &) const = default;
        };

        struct Entry
        {
            VendedCredentials credentials;
            Int64 reuse_until_ms = 0;
        };

        std::mutex mutex;
        std::map<Key, Entry> entries TSA_GUARDED_BY(mutex);
    };

    S3::AssumeRoleResult assumeRole(const String & location, VendedAccessLevel access) const;
    String buildSessionPolicy(const String & bucket, const String & key, VendedAccessLevel access) const;
    VendedCredentials toVendedCredentials(const String & location, const S3::AssumeRoleResult & sts_result) const;

    const std::shared_ptr<S3::AWSAssumeRoleClient> sts_client;
    const IcebergRESTCatalogWarehouseCredential warehouse_role;
    const std::shared_ptr<const S3::Client> warehouse_s3_client;

    mutable Cache credentials_cache;
};

using CredentialVendorPtr = std::shared_ptr<const CredentialVendor>;

}
