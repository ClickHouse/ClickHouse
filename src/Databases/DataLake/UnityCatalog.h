#pragma once
#include <Interpreters/StorageID.h>
#include "config.h"

#if USE_PARQUET

#include <Databases/DataLake/ICatalog.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>
#include <Poco/JSON/Object.h>
#include <Databases/DataLake/HTTPBasedCatalogUtils.h>

namespace DataLake
{

class UnityCatalog final : public ICatalog, private DB::WithContext
{
public:
    UnityCatalog(
        const std::string & catalog_,
        const std::string & base_url_,
        const std::string & catalog_credential_,
        DB::ContextPtr context_);

    ~UnityCatalog() override = default;

    bool empty() const override;

    CatalogTables getTables() const override;

    Namespaces getNamespaces() const override;

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
        return DB::DatabaseDataLakeCatalogType::UNITY;
    }

    /// Register a freshly created external DELTA table with Unity; `metadata_content` holds the Delta schema from `createInitial`.
    void createTable(
        const String & namespace_name,
        const String & table_name,
        const String & new_metadata_path,
        Poco::JSON::Object::Ptr metadata_content) const override;

private:
    const std::filesystem::path base_url;
    const LoggerPtr log;

    const std::string bearer_token;

    std::pair<Poco::Dynamic::Var, std::string> getJSONRequest(const std::string & route, const Poco::URI::QueryParameters & params = {}) const;
    std::pair<Poco::Dynamic::Var, std::string> postJSONRequest(const std::string & route, std::function<void(std::ostream &)> out_stream_callaback) const;

    DataLake::ICatalog::Namespaces getSchemas(const std::string & base_prefix, size_t limit = 0) const;

    /// Throw if the catalog `warehouse` has no schema `schema_name` (or the catalog itself does not exist), so
    /// a misconfigured namespace stays an error instead of being reported as an absent table by `existsTable`.
    void checkNamespaceExists(const std::string & schema_name) const;

    CatalogTables getTablesForSchema(const std::string & schema, size_t limit = 0) const;
    CatalogTables listTablesInNamespaceDirect(const std::string & namespace_name) const override;
    void getCredentials(const String & table_id, TableMetadata & metadata) const;

    Poco::JSON::Object::Ptr requestReadCredentials(const String & table_id) const;

    std::shared_ptr<IStorageCredentials> parseS3Credentials(const Poco::JSON::Object::Ptr & response) const;
    std::shared_ptr<IStorageCredentials> parseAzureCredentials(const Poco::JSON::Object::Ptr & response) const;

    bool getTableMetadataImpl(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const;

    ICatalog::CredentialsRefreshCallback getCredentialsConfigurationCallback(const DB::StorageID & table_id) override;
};

}

#endif
