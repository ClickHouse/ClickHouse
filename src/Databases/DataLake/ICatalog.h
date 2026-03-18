#pragma once
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>
#include <Databases/DataLake/StorageCredentials.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Databases/DataLake/DatabaseDataLakeStorageType.h>

namespace DataLake
{

using StorageType = DB::DatabaseDataLakeStorageType;
StorageType parseStorageTypeFromLocation(const std::string & location);

struct DataLakeSpecificProperties
{
    std::string iceberg_metadata_file_location;
};

/// A class representing table metadata,
/// which was received from Catalog.
class TableMetadata
{
public:
    TableMetadata() = default;

    TableMetadata & withLocation() { with_location = true; return *this; }
    TableMetadata & withSchema() { with_schema = true; return *this; }
    TableMetadata & withStorageCredentials() { with_storage_credentials = true; return *this; }
    TableMetadata & withDataLakeSpecificProperties() { with_datalake_specific_metadata = true; return *this; }

    bool hasLocation() const;
    bool hasSchema() const;
    bool hasStorageCredentials() const;
    bool hasDataLakeSpecificProperties() const;

    void setLocation(const std::string & location_);
    std::string getLocation() const;
    std::string getLocationWithEndpoint(const std::string & endpoint_) const;

    void setEndpoint(const std::string & endpoint_);
    std::string getEndpoint() const { return endpoint; }

    void setSchema(const DB::NamesAndTypesList & schema_);
    const DB::NamesAndTypesList & getSchema() const;

    void setStorageCredentials(std::shared_ptr<IStorageCredentials> credentials_);
    std::shared_ptr<IStorageCredentials> getStorageCredentials() const;

    void setDataLakeSpecificProperties(std::optional<DataLakeSpecificProperties> && metadata);
    std::optional<DataLakeSpecificProperties> getDataLakeSpecificProperties() const;

    bool requiresLocation() const { return with_location; }
    bool requiresSchema() const { return with_schema; }
    bool requiresCredentials() const { return with_storage_credentials; }
    bool requiresDataLakeSpecificProperties() const { return with_datalake_specific_metadata; }

    StorageType getStorageType() const;

    /// Some catalogs (Unity or Glue) may store not only Iceberg/DeltaLake tables but other kinds of "tables"
    /// as simple files or some in-memory tables, or even DataLake tables but in some private storages.
    /// ClickHouse can see these tables via catalog, but obviously cannot read them.
    /// So we use these methods to identify such tables and show them in SHOW TABLES and
    /// SHOW CREATE TABLE queries.
    void setTableIsNotReadable(const std::string & reason)
    {
        if (is_default_readable_table)
        {
            is_default_readable_table = false;
            reason_why_table_is_not_readable = reason;
        }
    }
    bool isDefaultReadableTable() const { return is_default_readable_table; }
    std::string getReasonWhyTableIsUnreadable() const { return reason_why_table_is_not_readable; }

private:
    /// Starts with s3://, file://, etc.
    /// For example, `s3://bucket/`
    std::string location_without_path;
    /// Path to table's data: `/path/to/table/data/`
    std::string path;
    DB::NamesAndTypesList schema;

    std::string bucket;
    /// Endpoint is set and used in case we have non-AWS storage implementation, for example, Minio.
    /// Also not all catalogs support non-AWS storages.
    std::string endpoint;

    /// Storage credentials, which are called "vended credentials".
    std::shared_ptr<IStorageCredentials> storage_credentials;

    /// Specific settings for iceberg and datalake
    std::optional<DataLakeSpecificProperties> data_lake_specific_metadata;

    std::string reason_why_table_is_not_readable;

    bool is_default_readable_table = true;

    bool with_location = false;
    bool with_schema = false;
    bool with_storage_credentials = false;
    bool with_datalake_specific_metadata = false;

    std::string constructLocation(const std::string & endpoint_) const;
};


/// Base class for catalog implementation.
/// Used for communication with the catalog.
class ICatalog
{
public:
    using Namespaces = std::vector<std::string>;

    explicit ICatalog(const std::string & warehouse_) : warehouse(warehouse_) {}

    virtual DB::DatabaseDataLakeCatalogType getCatalogType() const = 0;
    virtual ~ICatalog() = default;

    /// Does catalog have any tables?
    virtual bool empty() const = 0;

    /// Fetch tables' names list.
    /// Contains full namespaces in names.
    virtual DB::Names getTables() const = 0;

    /// Check that a table exists in a given namespace.
    virtual bool existsTable(
        const std::string & namespace_naem,
        const std::string & table_name) const = 0;

    /// Get table metadata in the given namespace.
    /// Throw exception if table does not exist.
    virtual void getTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const = 0;

    /// Get table metadata in the given namespace.
    /// Return `false` if table does not exist, `true` otherwise.
    virtual bool tryGetTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const = 0;

    /// Get storage type, where Iceberg tables' data is stored.
    /// E.g. one of S3, Azure, Local, HDFS.
    virtual std::optional<StorageType> getStorageType() const = 0;

protected:
    /// Name of the warehouse,
    /// which is sometimes also called "catalog name".
    const std::string warehouse;
};


}
