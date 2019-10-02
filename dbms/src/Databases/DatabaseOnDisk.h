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
class DatabaseOnDisk : public IDatabase
{
public:
    static void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query);

    static void removeTable(
        const Context & context,
        const String & table_name);

    static void renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        TableStructureWriteLockHolder &);

    static ASTPtr getCreateTableQuery(
        const Context & context,
        const String & table_name) const;

    static ASTPtr tryGetCreateTableQuery(
        const Context & context,
        const String & table_name) const;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    void drop() override;

    String getTableMetadataPath(const String & table_name) const override;

private:
    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;
};*/
}
