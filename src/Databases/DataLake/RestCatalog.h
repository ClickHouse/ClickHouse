#pragma once
#include "config.h"

#if USE_AVRO
#include <Databases/DataLake/ICatalog.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>
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

    bool isTransactional() const override { return true; }

    void dropTable(const String & namespace_name, const String & table_name) const override;

    ICatalog::CredentialsRefreshCallback getCredentialsConfigurationCallback(const DB::StorageID & storage_id) override;

    String getClientId() const { return client_id; }
    String getClientSecret() const { return client_secret; }

protected:
    RestCatalog(
        const std::string & warehouse_,
        const std::string & base_url_,
        const std::string & auth_scope_,
        const std::string & oauth_server_uri_,
        bool oauth_server_use_request_body_,
        DB::ContextPtr context_);

    void createNamespaceIfNotExists(const String & namespace_name, const String & location) const;

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

    const std::filesystem::path base_url;
    const LoggerPtr log;

    /// Catalog configuration settings from /v1/config endpoint.
    Config config;

    /// Auth headers of format: "Authorization": "<auth_scheme> <token>"
    std::optional<DB::HTTPHeaderEntry> auth_header;

    /// Parameters for OAuth (common for REST catalog).
    bool update_token_if_expired = false;
    std::string client_id;
    std::string client_secret;
    std::string auth_scope;
    std::string oauth_server_uri;
    bool oauth_server_use_request_body;
    mutable std::optional<AccessToken> access_token;

    Poco::Net::HTTPBasicCredentials credentials{};

    DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
        const std::string & endpoint,
        const Poco::URI::QueryParameters & params = {},
        const DB::HTTPHeaderEntries & headers = {}) const;

    Poco::URI::QueryParameters createParentNamespaceParams(const std::string & base_namespace) const;

    using StopCondition = std::function<bool(const std::string & namespace_name)>;
    using ExecuteFunc = std::function<void(const std::string & namespace_name)>;

    void getNamespacesRecursive(
        const std::string & base_namespace,
        Namespaces & result,
        StopCondition stop_condition,
        ExecuteFunc func) const;

    Namespaces getNamespaces(const std::string & base_namespace) const;

    Namespaces parseNamespaces(DB::ReadBuffer & buf, const std::string & base_namespace) const;

    DB::Names getTables(const std::string & base_namespace, size_t limit = 0) const;

    DB::Names parseTables(DB::ReadBuffer & buf, const std::string & base_namespace, size_t limit) const;

    bool getTableMetadataImpl(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const;

    Config loadConfig();
    virtual DB::HTTPHeaderEntries getAuthHeaders(bool update_token) const;
    static void parseCatalogConfigurationSettings(const Poco::JSON::Object::Ptr & object, Config & result);

    void sendRequest(
        const String & endpoint,
        Poco::JSON::Object::Ptr request_body,
        const String & method = Poco::Net::HTTPRequest::HTTP_POST,
        bool ignore_result = false) const;

    std::pair<std::shared_ptr<IStorageCredentials>, String> getCredentialsAndEndpoint(Poco::JSON::Object::Ptr object, const String & location) const;

    AccessToken retrieveAccessToken() const;
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
        const std::string & auth_scope_,
        const std::string & oauth_server_uri_,
        bool oauth_server_use_request_body_,
        DB::ContextPtr context_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_ONELAKE;
    }

    String getTenantId() const { return tenant_id; }

protected:
    /// Parameters for OneLake OAuth.
    const std::string tenant_id;
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
        DB::ContextPtr context_);

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE;
    }

    DB::HTTPHeaderEntries getAuthHeaders(bool update_token) const override;

private:
    /// Parameters for Google Cloud OAuth2 (BigLake).
    const std::string google_project_id;
    const std::string google_service_account;
    const std::string google_metadata_service;
    const std::string google_adc_client_id;
    const std::string google_adc_client_secret;
    const std::string google_adc_refresh_token;
    const std::string google_adc_quota_project_id;

    AccessToken retrieveGoogleCloudAccessToken() const;
    AccessToken retrieveGoogleCloudAccessTokenFromRefreshToken() const;
};

}

#endif
