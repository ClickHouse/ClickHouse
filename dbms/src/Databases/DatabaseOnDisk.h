#pragma once
#include <Databases/DatabasesCommon.h>


namespace DB
{

namespace detail
{
    String getTableMetadataPath(const String & base_path, const String & table_name);
    String getDatabaseMetadataPath(const String & base_path);
}

/* Class to provide basic operations with tables.
 */
class DatabaseOnDisk : public IDatabase
{
public:
    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void removeTable(
        const Context & context,
        const String & table_name) override;

    void renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        TableStructureWriteLockHolder &) override;

    ASTPtr getCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    void drop() override;

    String getTableMetadataPath(const String & table_name) const override;

protected:
    virtual const Poco::Logger * getLogger() const = 0;

private:
    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;
};
}
