#pragma once
#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <aws/core/auth/AWSCredentials.h>
#include <aws/glue/model/Column.h>
#include <aws/glue/model/Table.h>
#include <aws/glue/model/TableInput.h>
#include <Databases/DataLake/ICatalog.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/LRUCache.h>

#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>

namespace Aws::Glue
{
    class GlueClient;
}

namespace Aws::Auth
{
    class AWSCredentialsProvider;
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
        DB::ASTPtr table_engine_definition_,
        DB::LoadingStrictnessLevel table_definition_mode_,
        bool allow_server_credentials_in_user_queries_);

    ~GlueCatalog() override;

    bool empty() const override;

    CatalogTables getTables() const override;

    Namespaces getNamespaces() const override;

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

    DataLakeTableFormat getTableFormat(const TableMetadata &) const override { return DataLakeTableFormat::ICEBERG; }

    std::optional<std::string> getDefaultTableLocation(
        const std::string & namespace_name,
        const std::string & table_name) const override;

    void createTable(const String & namespace_name, const String & table_name, const String & new_metadata_path, Poco::JSON::Object::Ptr metadata_content) const override;

    void createNamespaceIfNotExists(const String & namespace_name) const override;

    bool updateMetadata(const String & namespace_name, const String & table_name, const String & new_metadata_path, const String & base_catalog_metadata_location, Poco::JSON::Object::Ptr new_snapshot, Poco::JSON::Object::Ptr new_metadata) const override;

    bool updateSchema(
        const String & namespace_name,
        const String & table_name,
        const String & new_metadata_path,
        const String & base_catalog_metadata_location,
        Poco::JSON::Object::Ptr new_schema,
        Int32 previous_schema_id) const override;

    void dropTable(const String & namespace_name, const String & table_name, bool delete_data) const override;

    /// Returns a callback that re-vends fresh AWS credentials from the configured
    /// credentials provider chain. Invoked by `ReadBufferFromS3` when an S3 call
    /// fails with `ExpiredToken`, so that a long-running read can recover without
    /// the user having to restart the query.
    ICatalog::CredentialsRefreshCallback getCredentialsConfigurationCallback(
        const DB::StorageID & storage_id, const TableMetadata & table_metadata) override;

    /// Converts top-level Iceberg fields to Glue columns with Hive types and `iceberg.field.*` parameters.
    static Aws::Vector<Aws::Glue::Model::Column> glueColumnsFromIcebergSchema(const Poco::JSON::Object::Ptr & schema);

    /// Selects the `schemas` entry referenced by `current-schema-id`, or the top-level v1 `schema` when there is no `schemas`.
    static Poco::JSON::Object::Ptr getCurrentSchema(const Poco::JSON::Object::Ptr & metadata_object);

private:
    /// Updates `metadata_location` and columns while preserving the rest of the Glue table `existing`.
    /// Columns are rebuilt from `new_schema` if given, otherwise from the current schema of `new_metadata`.
    static Aws::Glue::Model::TableInput buildUpdatedTableInput(
        const Aws::Glue::Model::Table & existing,
        const String & new_metadata_path,
        const Poco::JSON::Object::Ptr & new_schema,
        const Poco::JSON::Object::Ptr & new_metadata);

    /// Moves `metadata_location` to `new_metadata_path` with `UpdateTable` guarded by `VersionId`.
    /// Returns `false` if the pointer no longer names `base_catalog_metadata_location`.
    bool commitTableUpdate(
        const String & namespace_name,
        const String & table_name,
        const String & new_metadata_path,
        const String & base_catalog_metadata_location,
        const Poco::JSON::Object::Ptr & new_schema,
        const Poco::JSON::Object::Ptr & new_metadata) const;

    std::unique_ptr<Aws::Glue::GlueClient> glue_client;
    const LoggerPtr log;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    std::string region;
    CatalogSettings settings;
    DB::ASTPtr table_engine_definition;
    /// See `DatabaseDataLake::table_definition_mode`.
    DB::LoadingStrictnessLevel table_definition_mode;

    DataLake::ICatalog::Namespaces getDatabases(const std::string & prefix, size_t limit = 0) const;
    CatalogTables getTablesForDatabase(const std::string & db_name, size_t limit = 0) const;
    CatalogTables listTablesInNamespaceDirect(const std::string & namespace_name) const override;
    void setCredentials(TableMetadata & metadata) const;

    String resolveMetadataPathFromTableLocation(const String & table_location, const TableMetadata & table_metadata) const;

    struct ObjectStorageWithPath
    {
        DB::ObjectStoragePtr object_storage;
        String bucket_name;
        String table_path;  /// Path within bucket
    };

    ObjectStorageWithPath createObjectStorageForEarlyTableAccess(const String & s3_location, const TableMetadata & table_metadata) const;
};

}

#endif
