#pragma once
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>

namespace Iceberg
{

class TableMetadata
{
friend class RestCatalog;

public:
    TableMetadata() = default;

    std::string getPath() const;

    const DB::NamesAndTypesList & getSchema() const;

    TableMetadata & withLocation() { with_location = true; return *this; }
    TableMetadata & withSchema() { with_schema = true; return *this; }

private:
    /// starts with s3://, file://, etc
    std::string location;
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

    explicit ICatalog(const std::string & catalog_name_) : catalog_name(catalog_name_) {}

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

protected:
    const std::string catalog_name;
};

}
