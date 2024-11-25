#pragma once
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>

namespace Iceberg
{
using StorageType = DB::DatabaseIcebergStorageType;

/// A class representing table metadata,
/// which was received from Catalog.
class TableMetadata
{
public:
    TableMetadata() = default;

    TableMetadata & withLocation() { with_location = true; return *this; }
    TableMetadata & withSchema() { with_schema = true; return *this; }

    void setLocation(const std::string & location_);
    std::string getLocation(bool path_only) const;

    void setSchema(const DB::NamesAndTypesList & schema_);
    const DB::NamesAndTypesList & getSchema() const;

    bool requiresLocation() const { return with_location; }
    bool requiresSchema() const { return with_schema; }

private:
    /// Starts with s3://, file://, etc.
    /// For example, `s3://bucket/`
    std::string location_without_path;
    /// Path to table's data: `/path/to/table/data/`
    std::string path;
    DB::NamesAndTypesList schema;

    std::string credentials;

    bool with_location = false;
    bool with_schema = false;
};


/// Base class for catalog implementation.
/// Used for communication with the catalog.
class ICatalog
{
public:
    using Namespaces = std::vector<std::string>;

    explicit ICatalog(const std::string & warehouse_) : warehouse(warehouse_) {}

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
