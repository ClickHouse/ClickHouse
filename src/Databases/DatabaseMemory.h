#pragma once

#include <Databases/DatabasesCommon.h>
#include <Common/escapeForFileName.h>
#include <Parsers/ASTCreateQuery.h>


namespace Poco { class Logger; }


namespace DB
{

/** A non-persistent database to store temporary data.
  * It doesn't make any manipulations with filesystem.
  * All tables are created by calling code.
  * TODO: Maybe DatabaseRuntime is more suitable class name.
  */
class DatabaseMemory final : public DatabaseWithOwnTablesBase
{
public:
    DatabaseMemory(const String & name_, ContextPtr context);

    String getEngineName() const override { return "Memory"; }

    void createTable(
        ContextPtr context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void dropTable(
        ContextPtr context,
        const String & table_name,
        bool no_delay) override;

    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override;
    ASTPtr getCreateDatabaseQuery() const override;

    /// DatabaseMemory allows to create tables, which store data on disk.
    /// It's needed to create such tables in default database of clickhouse-local.
    /// TODO May be it's better to use DiskMemory for such tables.
    ///      To save data on disk it's possible to explicitly CREATE DATABASE db ENGINE=Ordinary in clickhouse-local.
    String getTableDataPath(const String & table_name) const override { return data_path + escapeForFileName(table_name) + "/"; }
    String getTableDataPath(const ASTCreateQuery & query) const override { return getTableDataPath(query.table); }

    UUID tryGetTableUUID(const String & table_name) const override;

    void drop(ContextPtr context) override;

    void alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

private:
    String data_path;
    using NameToASTCreate = std::unordered_map<String, ASTPtr>;
    NameToASTCreate create_queries;
};

}
