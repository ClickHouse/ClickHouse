#pragma once

#include <base/types.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>

#include <map>
#include <memory>

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

    /// Calls STS for new credentials each time.
    VendedCredentials vend(const String & location, VendedAccessLevel access) const;

private:
    S3::AssumeRoleResult assumeRole(const String & location, VendedAccessLevel access) const;
    String buildSessionPolicy(const String & bucket, const String & key, VendedAccessLevel access) const;
    VendedCredentials toVendedCredentials(const String & location, const S3::AssumeRoleResult & sts_result) const;

    const std::shared_ptr<S3::AWSAssumeRoleClient> sts_client;
    const IcebergRESTCatalogWarehouseCredential warehouse_role;
    const std::shared_ptr<const S3::Client> warehouse_s3_client;
};

using CredentialVendorPtr = std::shared_ptr<const CredentialVendor>;

}
