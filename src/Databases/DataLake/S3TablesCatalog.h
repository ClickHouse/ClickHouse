#pragma once

#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <Databases/DataLake/RestCatalog.h>
#include <IO/S3/Credentials.h>

#include <Poco/Net/HTTPRequest.h>

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
        DB::ContextPtr context_,
        bool allow_server_credentials_in_user_queries_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override { return DB::DatabaseDataLakeCatalogType::S3_TABLES; }

    CatalogTables getTables() const override;

    bool managesTableLocation() const override { return true; }

    bool tryGetTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    void dropTable(const String & namespace_name, const String & table_name, bool delete_data) const override;

protected:
    /// Override the network primitives instead of `getAuthHeaders` so the SigV4 signer has
    /// access to the final URL, method, and request body for canonicalisation.
    /// `catalog_state` and `auth_headers` are unused here: authentication is derived from the
    /// SigV4 signer rather than from the catalog state snapshot.
    DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
        const CatalogState & catalog_state,
        const std::string & endpoint,
        const Poco::URI::QueryParameters & params,
        const DB::HTTPHeaderEntries & headers,
        const std::optional<DB::HTTPHeaderEntries> & auth_headers) const override;

    void sendRequest(
        const CatalogState & catalog_state,
        const String & endpoint,
        Poco::JSON::Object::Ptr request_body,
        const String & method,
        bool ignore_result,
        const std::optional<DB::ReadSettings> & read_settings,
        bool * request_body_written) const override;

private:
    const String region;
    const String storage_endpoint;
    const String signing_service;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer;
};

}

#endif
