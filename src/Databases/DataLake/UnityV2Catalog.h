#pragma once
#include "config.h"

#if USE_AVRO && USE_PARQUET

#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/HTTPBasedCatalogUtils.h>
#include <Databases/DataLake/RestCatalog.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>
#include <chrono>
#include <mutex>

namespace DataLake
{

/// Unified Unity Catalog that supports both Delta and Iceberg tables
/// in a single database, with auto-detection of table format.
/// Supports both PAT and OAuth (client_id:client_secret) authentication.
class UnityV2Catalog final : public ICatalog, private DB::WithContext
{
public:
    /// catalog_credential_ is either a bearer token or "client_id:client_secret".
    UnityV2Catalog(
        const std::string & catalog_,
        const std::string & base_url_,
        const std::string & catalog_credential_,
        const std::string & auth_scope_,
        const std::string & oauth_server_uri_,
        DB::ContextPtr context_);

    ~UnityV2Catalog() override;

    bool empty() const override;
    CatalogTables getTables() const override;
    Namespaces getNamespaces() const override;
    CatalogTables listTablesInNamespaceDirect(const std::string & namespace_name) const override;
    bool existsTable(const std::string & schema_name, const std::string & table_name) const override;

    void getTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    bool tryGetTableMetadata(
        const std::string & schema_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override { return std::nullopt; }

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::UNITY_V2;
    }

    /// Serves both Delta and Iceberg tables, so the format is detected per table.
    DataLakeTableFormat getTableFormat(const TableMetadata & table_metadata) const override
    {
        return table_metadata.getTableFormat();
    }

    ICatalog::CredentialsRefreshCallback getCredentialsConfigurationCallback(
        const DB::StorageID & table_id, const TableMetadata & table_metadata) override;

private:
    const std::string base_url_str;
    const std::filesystem::path base_url;
    const LoggerPtr log;

    /// Auth state: always resolved to a Bearer token for the standard Unity API.
    std::string client_id;
    std::string client_secret;
    std::string auth_scope;
    std::string oauth_server_uri;
    bool use_oauth = false;

    /// Guards the token and everything derived from it, because `iceberg_rest_catalog` embeds the token in its auth header.
    mutable std::mutex token_mutex;
    mutable std::optional<AccessToken> access_token TSA_GUARDED_BY(token_mutex);

    /// Lazy-initialized RestCatalog for Iceberg table metadata,
    /// pointing to {base_url}/iceberg-rest.
    mutable std::shared_ptr<RestCatalog> iceberg_rest_catalog TSA_GUARDED_BY(token_mutex);

    /// Retries `make_request` once with a fresh token when the catalog rejects the cached one.
    template <typename Func>
    auto requestWithRetry(Func && make_request) const;

    std::pair<Poco::Dynamic::Var, std::string> getJSONRequest(
        const std::string & route,
        const Poco::URI::QueryParameters & params = {}) const;

    std::pair<Poco::Dynamic::Var, std::string> postJSONRequest(
        const std::string & route,
        std::function<void(std::ostream &)> out_stream_callback) const;

    std::string getBearerToken(bool force_refresh = false) const;

    /// Fetches a token from the OAuth server.
    AccessToken retrieveAccessToken() const;

    /// `force_refresh` mints a new token even when the cached one has not expired yet.
    void ensureBearerToken(bool force_refresh = false) const TSA_REQUIRES(token_mutex);

    ICatalog::Namespaces getSchemas(const std::string & base_prefix, size_t limit = 0) const;
    CatalogTables getTablesForSchema(const std::string & schema, size_t limit = 0) const;

    DataLakeTableFormat detectTableFormat(const Poco::JSON::Object::Ptr & table_json) const;

    bool tryGetDeltaTableMetadata(
        const std::string & full_table_name,
        const Poco::JSON::Object::Ptr & table_json,
        TableMetadata & result) const;

    /// Asks the catalog for temporary read credentials for Delta tables.
    std::shared_ptr<IStorageCredentials> getDeltaCredentials(const std::string & table_id, StorageType storage_type) const;

    /// Return `nullptr` when the response carries no credentials of that kind.
    std::shared_ptr<IStorageCredentials> parseS3Credentials(const Poco::JSON::Object::Ptr & response) const;
    std::shared_ptr<IStorageCredentials> parseAzureCredentials(const Poco::JSON::Object::Ptr & response) const;

    /// `force_refresh` rebuilds the catalog around a freshly minted token.
    std::shared_ptr<RestCatalog> getIcebergRestCatalog(bool force_refresh = false) const;
};

}

#endif
