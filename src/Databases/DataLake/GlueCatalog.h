#pragma once
#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <aws/core/auth/AWSCredentials.h>
#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/LRUCache.h>

#include <Common/CacheBase.h>
#include <Databases/DataLake/DatabaseDataLakeSettings.h>

namespace Aws::Glue
{
    class GlueClient;
}

namespace DataLake
{

class GlueCatalog final : public ICatalog, private DB::WithContext
{
public:
    GlueCatalog(
        const String & endpoint,
        DB::ContextPtr context_,
        const CatalogSettings & settings_,
        DB::ASTPtr table_engine_definition_);

    ~GlueCatalog() override;

    bool empty() const override;

    DB::Names getTables() const override;

    bool existsTable(const std::string & database_name, const std::string & table_name) const override;

    void getTableMetadata(
        const std::string & database_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    bool tryGetTableMetadata(
        const std::string & database_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override
    {
        /// Glue catalog is AWS service, so it supports only data in S3
        return StorageType::S3;
    }

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::GLUE;
    }

    void createTable(const String & namespace_name, const String & table_name, const String & new_metadata_path, Poco::JSON::Object::Ptr metadata_content) const override;

    bool updateMetadata(const String & namespace_name, const String & table_name, const String & new_metadata_path, Poco::JSON::Object::Ptr new_snapshot) const override;
    void dropTable(const String & namespace_name, const String & table_name) const override;

private:
    void createNamespaceIfNotExists(const String & namespace_name) const;

    std::unique_ptr<Aws::Glue::GlueClient> glue_client;
    const LoggerPtr log;
    Aws::Auth::AWSCredentials credentials;
    std::string region;
    CatalogSettings settings;
    DB::ASTPtr table_engine_definition;

    DataLake::ICatalog::Namespaces getDatabases(const std::string & prefix, size_t limit = 0) const;
    DB::Names getTablesForDatabase(const std::string & db_name, size_t limit = 0) const;
    void setCredentials(TableMetadata & metadata) const;

    /// The Glue catalog does not store detailed information about the types of timestamp columns, such as whether the column is timestamp or timestamptz.
    /// This method allows to clarify the actual type of the timestamp column.
    bool classifyTimestampTZ(const String & column_name, const TableMetadata & table_metadata) const;

    mutable DB::CacheBase<String, Poco::JSON::Object::Ptr> metadata_objects;
};

}

#endif
