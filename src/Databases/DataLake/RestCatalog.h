#pragma once
#include "config.h"

#if USE_AVRO
#include <Databases/DataLake/ICatalog.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Common/MultiVersion.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>
#include <unordered_set>
#include <Poco/JSON/Object.h>

namespace DB
{
class ReadBuffer;
}

namespace DataLake
{

struct AccessToken
{
    std::string token;
    std::optional<std::chrono::system_clock::time_point> expires_at;

    bool isExpired() const
    {
        if (!expires_at.has_value())
            return false;
        return std::chrono::system_clock::now() >= expires_at.value();
    }
};

class RestCatalog : public ICatalog, public DB::WithContext
{
public:
    explicit RestCatalog(
        const std::string & warehouse_,
        const std::string & base_url_,
        const std::string & catalog_credential_,
        const std::string & auth_scope_,
        const std::string & auth_header_,
        const std::string & oauth_server_uri_,
        bool oauth_server_use_request_body_,
        DB::ContextPtr context_);

    ~RestCatalog() override = default;

    bool empty() const override;

    DB::Names getTables() const override;

    Namespaces getNamespaces() const override;

    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;

    void getTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    bool tryGetTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override;

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_REST;
    }

    void createTable(const String & namespace_name, const String & table_name, const String & new_metadata_path, Poco::JSON::Object::Ptr metadata_content) const override;

    bool updateMetadata(const String & namespace_name, const String & table_name, const String & new_metadata_path, Poco::JSON::Object::Ptr new_snapshot) const override;

    bool updateSchema(
        const String & namespace_name,
        const String & table_name,
        const String & new_metadata_path,
        Poco::JSON::Object::Ptr new_schema,
        Int32 previous_schema_id) const override;

    bool isTransactional() const override { return true; }

    void dropTable(const String & namespace_name, const String & table_name) const override;

    ICatalog::CredentialsRefreshCallback getCredentialsConfigurationCallback(const DB::StorageID & storage_id) override;

    struct Config
    {
        /// Prefix is a path of the catalog endpoint,
        /// e.g. /v1/{prefix}/namespaces/{namespace}/tables/{table}
        std::filesystem::path prefix;
        /// Base location is location of data in storage
        /// (in filesystem or object storage).
        std::string default_base_location;

        std::string toString() const;
    };

    /// Credentials together with the catalog configuration they resolve to
    /// (the /v1/config response depends on the credentials), published as one
    /// atomic snapshot so readers never see a torn combination of them.
    struct CatalogState
    {
        std::optional<DB::HTTPHeaderEntry> auth_header;
        std::string client_id;
        std::string client_secret;
        std::string tenant_id;
        std::string bearer_token;
        Config config;
    };
    using CatalogStateVersion = MultiVersion<CatalogState>::Version;

    CatalogStateVersion getStateSnapshot() const { return state.get(); }

    ICatalog::PreparedSettingsChangesPtr prepareSettingsChanges(const DB::SettingsChanges & changes) override;

    void commitSettingsChanges(ICatalog::PreparedSettingsChangesPtr prepared) override;

    /// Check that we actually support these settings alter
    static void validateSettingsChangesImpl(
        const DB::SettingsChanges & changes,
        const std::unordered_set<std::string> & alterable_settings,
        const std::string & auth_mode_description);

    /// `credential_mode` means the catalog authenticates with `catalog_credential`,
    /// `header_mode` with `auth_header`. The mode is fixed when the database is created.
    static void validateSettingsChanges(const DB::SettingsChanges & changes, bool credential_mode, bool header_mode);

protected:
    RestCatalog(
        const std::string & warehouse_,
        const std::string & base_url_,
        const std::string & auth_scope_,
        const std::string & oauth_server_uri_,
        bool oauth_server_use_request_body_,
        DB::ContextPtr context_);

    void createNamespaceIfNotExists(const String & namespace_name, const String & location) const;

    const std::filesystem::path base_url;
    const LoggerPtr log;

    MultiVersion<CatalogState> state{std::make_unique<const CatalogState>()};

    /// Parameters for OAuth (common for REST catalog).
    bool update_token_if_expired = false;
    std::string auth_scope;
    std::string oauth_server_uri;
    bool oauth_server_use_request_body;
    mutable MultiVersion<AccessToken> access_token;

    Poco::Net::HTTPBasicCredentials credentials{};

    /// `catalog_state` is the snapshot the caller derived the endpoint from, so that one
    /// request never mixes the endpoint of one state version with the auth of another.
    DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
        const CatalogState & catalog_state,
        const std::string & endpoint,
        const Poco::URI::QueryParameters & params = {},
        const DB::HTTPHeaderEntries & headers = {},
        const std::optional<DB::HTTPHeaderEntries> & auth_headers = std::nullopt) const;

    Poco::URI::QueryParameters createParentNamespaceParams(const std::string & base_namespace) const;

    using StopCondition = std::function<bool(const std::string & namespace_name)>;
    using ExecuteFunc = std::function<void(const std::string & namespace_name)>;

    void getNamespacesRecursive(
        const std::string & base_namespace,
        Namespaces & result,
        StopCondition stop_condition,
        ExecuteFunc func) const;

    /// Whether this catalog has flat (single-level) namespaces and ignores the `parent` filter when
    /// listing namespaces. Such catalogs (BigLake, Databricks Delta Sharing) echo the same namespaces
    /// for any parent; treating those echoes as children would recurse without bound, so sub-namespace
    /// listing is skipped for them (see `parseNamespaces`).
    bool hasFlatNamespaces() const;

    /// List the immediate child namespaces directly under `base_namespace`
    /// (single level, not recursive). An empty base lists the root namespaces.
    Namespaces listChildNamespaces(const std::string & base_namespace) const;

    Namespaces parseNamespaces(DB::ReadBuffer & buf, const std::string & base_namespace, String & next_page_token) const;

    /// Non-recursive list of tables directly in `base_namespace` (not in sub-namespaces).
    /// `limit` is a soft cap on the number of returned names; 0 means no limit.
    DB::Names listTablesInNamespace(const std::string & base_namespace, size_t limit = 0) const;

    DB::Names listTablesInNamespaceDirect(const std::string & namespace_name) const override;

    DB::Names parseTables(DB::ReadBuffer & buf, const std::string & base_namespace, size_t limit, String & next_page_token) const;

    bool getTableMetadataImpl(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const;

    /// Load catalog config (special http handler) utilizing information from catalog_state and auth_headers.
    Config loadConfig(const CatalogState & catalog_state, const std::optional<DB::HTTPHeaderEntries> & auth_headers = std::nullopt);
    virtual DB::HTTPHeaderEntries getAuthHeaders(const CatalogState & catalog_state, bool update_token) const;

    void validateAuthHeaders(const DB::HTTPHeaderEntry & header) const;
    static void parseCatalogConfigurationSettings(const Poco::JSON::Object::Ptr & object, Config & result);

    void sendRequest(
        const CatalogState & catalog_state,
        const String & endpoint,
        Poco::JSON::Object::Ptr request_body,
        const String & method = Poco::Net::HTTPRequest::HTTP_POST,
        bool ignore_result = false) const;

    std::pair<std::shared_ptr<IStorageCredentials>, String> getCredentialsAndEndpoint(Poco::JSON::Object::Ptr object, const String & location) const;

    AccessToken retrieveAccessToken(const std::string & client_id, const std::string & client_secret) const;

    struct PreparedAuthChanges;

    /// Hook for `prepareSettingsChanges`: validate `changes` and apply them to `new_state`,
    /// building the new auth artifacts, without publishing anything. When the OAuth
    /// credentials change, the eagerly fetched token goes into `new_access_token` and
    /// `new_auth_headers`, so that wrong credentials fail the ALTER right here and the
    /// config reload authenticates with the new token instead of the cached one.
    virtual void applySettingsChangesToState(
        const DB::SettingsChanges & changes,
        const CatalogState & old_state,
        CatalogState & new_state,
        std::optional<DB::HTTPHeaderEntries> & new_auth_headers,
        std::unique_ptr<AccessToken> & new_access_token);
};

class OneLakeCatalog : public RestCatalog
{
public:
    explicit OneLakeCatalog(
        const std::string & warehouse_,
        const std::string & base_url_,
        const std::string & onelake_tenant_id,
        const std::string & onelake_client_id,
        const std::string & onelake_client_secret,
        const std::string & bearer_token_,
        const std::string & auth_scope_,
        const std::string & oauth_server_uri_,
        bool oauth_server_use_request_body_,
        DB::ContextPtr context_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_ONELAKE;
    }

    DB::HTTPHeaderEntries getAuthHeaders(const CatalogState & catalog_state, bool update_token) const override;

    /// `bearer_mode` means the catalog authenticates with `onelake_bearer_token`,
    /// otherwise with the `onelake_client_id` + `onelake_client_secret` pair.
    /// The mode is fixed when the database is created.
    static void validateSettingsChanges(const DB::SettingsChanges & changes, bool bearer_mode);

protected:
    void applySettingsChangesToState(
        const DB::SettingsChanges & changes,
        const CatalogState & old_state,
        CatalogState & new_state,
        std::optional<DB::HTTPHeaderEntries> & new_auth_headers,
        std::unique_ptr<AccessToken> & new_access_token) override;
};

class BigLakeCatalog : public RestCatalog
{
public:
    explicit BigLakeCatalog(
        const std::string & warehouse_,
        const std::string & base_url_,
        const std::string & google_project_id_,
        const std::string & google_service_account_,
        const std::string & google_metadata_service_,
        const std::string & google_adc_client_id_,
        const std::string & google_adc_client_secret_,
        const std::string & google_adc_refresh_token_,
        const std::string & google_adc_quota_project_id_,
        DB::ContextPtr context_,
        bool allow_server_credentials_in_user_queries_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE;
    }

    DB::HTTPHeaderEntries getAuthHeaders(const CatalogState & catalog_state, bool update_token) const override;

    const std::string & getGoogleADCClientId() const { return google_adc_client_id; }
    const std::string & getGoogleADCClientSecret() const { return google_adc_client_secret; }
    const std::string & getGoogleADCRefreshToken() const { return google_adc_refresh_token; }

private:
    /// Parameters for Google Cloud OAuth2 (BigLake).
    const std::string google_project_id;
    const std::string google_service_account;
    const std::string google_metadata_service;
    const std::string google_adc_client_id;
    const std::string google_adc_client_secret;
    const std::string google_adc_refresh_token;
    const std::string google_adc_quota_project_id;
    /// Effective `s3_allow_server_credentials_in_user_queries` captured when the database was created; the
    /// catalog is cached and holds the global context, whose settings never reflect the creating session.
    const bool allow_server_credentials_in_user_queries;

    AccessToken retrieveGoogleCloudAccessToken() const;
    AccessToken retrieveGoogleCloudAccessTokenFromRefreshToken() const;
};

/// Databricks Delta Sharing exposes an Iceberg REST catalog with a flat, single-level namespace model
/// (share -> namespace/schema -> table) and ignores the `parent` filter when listing namespaces. It is
/// otherwise a plain REST catalog, so it reuses RestCatalog's behaviour and only reports a distinct type
/// so `hasFlatNamespaces()` applies the same top-level-only listing used for BigLake.
class DeltaSharingCatalog : public RestCatalog
{
public:
    using RestCatalog::RestCatalog;

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_DELTA_SHARING;
    }
};

}

#endif
