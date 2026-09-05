#pragma once
#include "config.h"

#if USE_LIBPQXX && USE_AVRO && USE_AWS_S3

#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/CacheBase.h>
#include <Common/Logger.h>

#include <cstdint>
#include <memory>
#include <string>

namespace postgres
{
class PoolWithFailover;
using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;
}

namespace DataLake
{

/// Read-only client for the Apache Iceberg `JdbcCatalog` V0/V1 schema
/// (`iceberg_tables` / `iceberg_namespace_properties` in Postgres), instead
/// of going through an Iceberg REST HTTP API.
///
/// Table pointers resolve with a single indexed `SELECT` on the primary key
/// `(catalog_name, table_namespace, table_name)` without a REST catalog request.
/// The table schema is parsed from the referenced
/// `metadata.json` through the same `parseTableSchema` path the REST catalog
/// uses, and cached per metadata location.
///
/// PostgreSQL backends and S3 metadata locations are supported. V1 adds the
/// `iceberg_type` column; future schema changes require compatibility testing.
/// Writes, views and credential vending are not supported. Use read-only
/// PostgreSQL and object-storage credentials.
class IcebergJdbcCatalog final : public ICatalog, private DB::WithContext
{
public:
    struct ConnectionParams
    {
        String host = "localhost";
        UInt16 port = 5432;
        String database = "postgres";
        /// Postgres schema holding the `iceberg_tables` /
        /// `iceberg_namespace_properties` tables.
        String schema = "public";
        String user = "postgres";
        String password;
    };

    explicit IcebergJdbcCatalog(
        const std::string & catalog_name_,
        ConnectionParams params_,
        const DB::DatabaseDataLakeSettings & database_settings_,
        DB::ASTPtr table_engine_definition_,
        DB::ContextPtr context_);

    ~IcebergJdbcCatalog() override = default;

    bool empty() const override;

    CatalogTables getTables() const override;

    Namespaces getNamespaces() const override;

    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;

    void getTableMetadata(
        const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    bool tryGetTableMetadata(
        const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override { return std::nullopt; }

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_JDBC;
    }

protected:
    CatalogTables listTablesInNamespaceDirect(const std::string & namespace_name) const override;

private:
    struct ObjectStorageWithPath
    {
        DB::ObjectStoragePtr object_storage;
        String metadata_path; /// Path of the metadata file within the bucket
    };

    /// `"<schema>"."<table>"` with the settings-provided schema quoted.
    String qualified(const std::string & table) const;

    /// `metadata.json` for `metadata_location`, via object storage, cached.
    /// Returns nullptr and marks the table unreadable when the location
    /// scheme cannot be read directly (only S3-family locations supported).
    Poco::JSON::Object::Ptr getMetadataJSON(const String & metadata_location, TableMetadata & result) const;

    ObjectStorageWithPath createObjectStorageForMetadataAccess(const String & metadata_location) const;

    /// ` AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)` on V1 schemas,
    /// empty on V0 schemas (which have no `iceberg_type` column).
    String tableTypePredicate() const;

    ConnectionParams params;
    DB::DatabaseDataLakeSettings database_settings;
    DB::ASTPtr table_engine_definition;
    const LoggerPtr log;

    postgres::PoolWithFailoverPtr pool;
    /// Whether `iceberg_tables` has the V1 `iceberg_type` column.
    bool has_iceberg_type = false;

    mutable DB::CacheBase<String, Poco::JSON::Object::Ptr> metadata_objects;
};

}

#endif
