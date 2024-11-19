#pragma once
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>

namespace Iceberg
{
using StorageType = DB::DatabaseIcebergStorageType;

class TableMetadata
{
public:
    TableMetadata() = default;

    TableMetadata & withLocation() { with_location = true; return *this; }
    TableMetadata & withSchema() { with_schema = true; return *this; }

    std::string getLocation(bool path_only) const;
    std::string getLocation() const;
    std::string getLocationWithoutPath() const;

    const DB::NamesAndTypesList & getSchema() const;

    bool requiresLocation() const { return with_location; }
    bool requiresSchema() const { return with_schema; }

    void setLocation(const std::string & location_);
    void setSchema(const DB::NamesAndTypesList & schema_);

private:
    /// starts with s3://, file://, etc
    std::string location_without_path;
    std::string path;
    /// column names and types
    DB::NamesAndTypesList schema;

    bool with_location = false;
    bool with_schema = false;
};


class ICatalog
{
public:
    using Namespaces = std::vector<std::string>;
    using Tables = std::vector<std::string>;

    explicit ICatalog(const std::string & warehouse_) : warehouse(warehouse_) {}

    virtual ~ICatalog() = default;

    virtual bool empty() const = 0;

    virtual Tables getTables() const = 0;

    virtual bool existsTable(
        const std::string & namespace_naem,
        const std::string & table_name) const = 0;

    virtual void getTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const = 0;

    virtual bool tryGetTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const = 0;

    virtual std::optional<StorageType> getStorageType() const = 0;

protected:
    const std::string warehouse;

    static StorageType getStorageType(const std::string & location);
};

}
