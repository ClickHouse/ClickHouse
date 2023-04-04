#pragma once

#include <Databases/DatabasesCommon.h>
#include <Common/escapeForFileName.h>
#include <Parsers/ASTCreateQuery.h>


namespace Poco { class Logger; }


namespace DB
{

/** A non-persistent database to store temporary data.
  * It doesn't save tables' metadata in filesystem.
  * However it stores symlinks to the data of the tables, if they store data on disk.
  * All tables are created by calling code.
  * TODO: Maybe DatabaseRuntime is more suitable class name.
  */
class DatabaseMemory final : public DatabaseWithOwnTablesBase, public TableDataMapping<true>
{
public:
    DatabaseMemory(const String & name_, UUID uuid, ContextPtr context_);

    String getEngineName() const override { return "Memory"; }
    UUID getUUID() const override { return db_uuid; }

    void createTable(
        ContextPtr context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void dropTable(
        ContextPtr context,
        const String & table_name,
        bool sync) override;

   void renameTable(
            ContextPtr context,
            const String & table_name,
            IDatabase & to_database,
            const String & to_table_name,
            bool exchange,
            bool dictionary) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) const override;

    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override;
    ASTPtr getCreateDatabaseQuery() const override;

    /// DatabaseMemory allows to create tables, which store data on disk.
    /// It's needed to create such tables in default database of clickhouse-local.
    /// TODO May be it's better to use DiskMemory for such tables.
    ///      To save data on disk it's possible to explicitly CREATE DATABASE db ENGINE=Ordinary in clickhouse-local.
    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void drop(ContextPtr context) override;

    void alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata) override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const override;

private:
    void removeDataPath();

    const String data_path;
    using NameToASTCreate = std::unordered_map<String, ASTPtr>;
    NameToASTCreate create_queries TSA_GUARDED_BY(mutex);

    const UUID db_uuid;
};

}
