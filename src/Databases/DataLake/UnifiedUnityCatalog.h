#pragma once
#include "config.h"

#if USE_AVRO && USE_PARQUET

#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/HTTPBasedCatalogUtils.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>
#include <chrono>
#include <mutex>

namespace DataLake
{

class RestCatalog;

/// Unified Unity Catalog that supports both Delta and Iceberg tables
/// in a single database, with auto-detection of table format.
/// Supports both PAT and OAuth (client_id:client_secret) authentication.
class UnifiedUnityCatalog final : public ICatalog, private DB::WithContext
{
public:
    UnifiedUnityCatalog(
        const std::string & catalog_,
        const std::string & base_url_,
        const std::string & catalog_credential_,
        const std::string & auth_scope_,
        const std::string & oauth_server_uri_,
        bool oauth_server_use_request_body_,
        DB::ContextPtr context_);

    ~UnifiedUnityCatalog() override;

    bool empty() const override;
    DB::Names getTables() const override;
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
        return DB::DatabaseDataLakeCatalogType::UNITY_CATALOG;
    }

private:
    const std::string base_url_str;
    const std::filesystem::path base_url;
    const LoggerPtr log;

    /// Auth state: always resolved to a Bearer token for the standard Unity API.
    std::string client_id;
    std::string client_secret;
    std::string auth_scope;
    std::string oauth_server_uri;
    bool oauth_server_use_request_body;
    bool use_oauth = false;
    mutable std::optional<std::string> bearer_token;
    mutable std::chrono::system_clock::time_point token_expires_at{};
    Poco::Net::HTTPBasicCredentials credentials{};

    /// Original credential for passing to the internal RestCatalog.
    std::string catalog_credential;

    /// Lazy-initialized RestCatalog for Iceberg table metadata,
    /// pointing to {base_url}/iceberg-rest.
    mutable std::shared_ptr<RestCatalog> iceberg_rest_catalog;

    /// Cache of table JSON from listing, keyed by "schema.table".
    /// Populated during getTables()/getTablesForSchema() and consumed
    /// by tryGetTableMetadata() to avoid per-table API round-trips.
    mutable std::mutex table_cache_mutex;
    mutable std::unordered_map<std::string, Poco::JSON::Object::Ptr> table_json_cache;

    /// Cache of getTables() result to avoid repeated schema/table listing.
    mutable DB::Names cached_table_names;
    mutable std::chrono::system_clock::time_point table_names_cached_at{};

    std::pair<Poco::Dynamic::Var, std::string> getJSONRequest(
        const std::string & route,
        const Poco::URI::QueryParameters & params = {}) const;

    std::pair<Poco::Dynamic::Var, std::string> postJSONRequest(
        const std::string & route,
        std::function<void(std::ostream &)> out_stream_callback) const;

    DB::HTTPHeaderEntries getAuthHeaders() const;
    std::string retrieveAccessToken() const;
    void ensureBearerToken() const;

    ICatalog::Namespaces getSchemas(const std::string & base_prefix, size_t limit = 0) const;
    DB::Names getTablesForSchema(const std::string & schema, size_t limit = 0) const;

    DataLakeTableFormat detectTableFormat(const Poco::JSON::Object::Ptr & table_json) const;

    bool tryGetDeltaTableMetadata(
        const std::string & full_table_name,
        const Poco::JSON::Object::Ptr & table_json,
        TableMetadata & result) const;

    void getDeltaCredentials(const std::string & table_id, TableMetadata & metadata) const;

    std::shared_ptr<RestCatalog> getIcebergRestCatalog() const;
};

}

#endif
