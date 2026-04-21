#pragma once

#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <Databases/DataLake/RestCatalog.h>
#include <IO/S3/Credentials.h>

#include <aws/core/auth/signer/AWSAuthV4Signer.h>

#include <memory>

namespace Aws::Auth
{
class AWSCredentialsProvider;
}

namespace DataLake
{

/// Iceberg REST catalog for Amazon S3 Tables (SigV4, signing name `s3tables`).
/// https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html
class S3TablesCatalog final : public RestCatalog
{
public:
    S3TablesCatalog(
        const String & warehouse_,
        const String & base_url_,
        const String & region_,
        const DataLake::CatalogSettings & catalog_settings_,
        DB::ContextPtr context_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override { return DB::DatabaseDataLakeCatalogType::S3_TABLES; }

    DB::Names getTables() const override;

    bool tryGetTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    void dropTable(const String & namespace_name, const String & table_name) const override;

protected:
    DB::HTTPHeaderEntries getAuthHeaders(
        bool update_token,
        const String & method = {},
        const Poco::URI & url = {},
        const DB::HTTPHeaderEntries & extra_headers = {},
        const String & body = {}) const override;

private:
    const String region;
    const String storage_endpoint;
    const String signing_service;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer;
};

}

#endif
