#pragma once
#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <aws/core/auth/AWSCredentials.h>
#include <aws/glue/model/Column.h>
#include <aws/glue/model/TableInput.h>
#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/LRUCache.h>

#include <Common/CacheBase.h>
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

    bool updateMetadata(const String & namespace_name, const String & table_name, const String & new_metadata_path, Poco::JSON::Object::Ptr new_snapshot) const override;

    bool updateSchema(
        const String & namespace_name,
        const String & table_name,
        const String & new_metadata_path,
        Poco::JSON::Object::Ptr new_schema,
        Int32 previous_schema_id) const override;

    void dropTable(const String & namespace_name, const String & table_name, bool delete_data) const override;

    /// Returns a callback that re-vends fresh AWS credentials from the configured
    /// credentials provider chain. Invoked by `ReadBufferFromS3` when an S3 call
    /// fails with `ExpiredToken`, so that a long-running read can recover without
    /// the user having to restart the query.
    ICatalog::CredentialsRefreshCallback getCredentialsConfigurationCallback(
        const DB::StorageID & storage_id, const TableMetadata & table_metadata) override;

    /// Resolves the precise Iceberg timestamp type for `column_name` by searching the current schema
    /// in the Iceberg `metadata_object`. Falls back to `"timestamp_ns"` when `glue_column_type` is
    /// `"timestamp_nano"`, or `"timestamp"` otherwise, when the column is not found in the metadata.
    static String resolveTimestampTypeFromMetadata(
        const Poco::JSON::Object::Ptr & metadata_object,
        const String & column_name,
        const String & glue_column_type);

    /// Translates an Iceberg schema object (`{"schema-id": .., "fields": [..]}`) into the Glue
    /// `StorageDescriptor.Columns` representation used by Apache Iceberg's Glue catalog: one column
    /// per top-level field, with a Hive-style type string and the `iceberg.field.id`,
    /// `iceberg.field.optional` and `iceberg.field.current` parameters that `getTableMetadata`
    /// reads back. Athena and other Glue consumers require these columns to be present.
    static Aws::Vector<Aws::Glue::Model::Column> glueColumnsFromIcebergSchema(const Poco::JSON::Object::Ptr & schema);

    /// Returns the schema referenced by `current-schema-id` in an Iceberg `metadata.json` object.
    static Poco::JSON::Object::Ptr getCurrentSchema(const Poco::JSON::Object::Ptr & metadata_object);

private:
    /// Builds the `TableInput` for an `UpdateTable` call that moves `metadata_location` to
    /// `new_metadata_path`. Glue's `UpdateTable` replaces the whole table definition, so the
    /// existing definition is fetched first and only the metadata pointer (and, when `columns`
    /// is given, the column list) is changed; everything else, including columns written by a
    /// previous commit, is preserved.
    Aws::Glue::Model::TableInput buildUpdatedTableInput(
        const String & namespace_name,
        const String & table_name,
        const String & new_metadata_path,
        const std::optional<Aws::Vector<Aws::Glue::Model::Column>> & columns) const;

    std::unique_ptr<Aws::Glue::GlueClient> glue_client;
    const LoggerPtr log;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    std::string region;
    CatalogSettings settings;
    DB::ASTPtr table_engine_definition;

    DataLake::ICatalog::Namespaces getDatabases(const std::string & prefix, size_t limit = 0) const;
    CatalogTables getTablesForDatabase(const std::string & db_name, size_t limit = 0) const;
    CatalogTables listTablesInNamespaceDirect(const std::string & namespace_name) const override;
    void setCredentials(TableMetadata & metadata) const;

    /// The Glue catalog does not store detailed information about the types of timestamp columns, such as whether the column is timestamp or timestamptz.
    /// This method allows to clarify the actual type of the timestamp column.
    /// `glue_column_type` is the raw Glue type (`"timestamp"` or `"timestamp_nano"`) used as a fallback when the column is not found in Iceberg metadata.
    String getActualTimestampType(const String & column_name, const TableMetadata & table_metadata, const String & glue_column_type) const;

    String resolveMetadataPathFromTableLocation(const String & table_location, const TableMetadata & table_metadata) const;

    struct ObjectStorageWithPath
    {
        DB::ObjectStoragePtr object_storage;
        String bucket_name;
        String table_path;  /// Path within bucket
    };

    ObjectStorageWithPath createObjectStorageForEarlyTableAccess(const String & s3_location, const TableMetadata & table_metadata) const;

    mutable DB::CacheBase<String, Poco::JSON::Object::Ptr> metadata_objects;
};

}

#endif
