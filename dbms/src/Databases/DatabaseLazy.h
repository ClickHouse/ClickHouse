#pragma once

#include <Poco/DirectoryIterator.h>
#include <Databases/DatabasesCommon.h>
#include <Common/ThreadPool.h>


namespace DB
{

class DatabaseLazyIterator;


/** Lazy engine of databases.
  * Works like DatabaseOrdinary, but stores in memory only cached tables.
  */
class DatabaseLazy : public DatabaseWithOwnTablesBase
{
public:
    DatabaseLazy(String name_, const String & metadata_path_, const Context & context);

    String getEngineName() const override { return "Lazy"; }

    void loadTables(
        Context & context,
        bool has_force_restore_data_flag) override;

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

    void alterTable(
        const Context & context,
        const String & name,
        const ColumnsDescription & columns,
        const IndicesDescription & indices,
        const ConstraintsDescription & constraints,
        const ASTModifier & engine_modifier) override;

    time_t getTableMetadataModificationTime(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(
        const Context & context,
        const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    String getDataPath() const override;
    String getDatabaseName() const override;
    String getMetadataPath() const override;
    String getTableMetadataPath(const String & table_name) const override;

    void drop() override;

    bool isTableExist(
        const Context & context,
        const String & table_name) const override;

    StoragePtr tryGetTable(
        const Context & context,
        const String & table_name) const override;

    bool empty(const Context & context) const override;

    DatabaseIteratorPtr getIterator(const Context & context, const FilterByNameFunction & filter_by_table_name = {}) override;

private:
    const String metadata_path;
    const String data_path;
    bool has_force_restore_data_flag = false;

    Poco::Logger * log;

    ASTPtr getCreateTableQueryImpl(const Context & context, const String & table_name, bool throw_on_error) const;

    friend class DatabaseLazyIterator;
};


class DatabaseLazyIterator final : public IDatabaseIterator
{
public:
    DatabaseLazyIterator(DatabaseLazy & database_, bool has_force_restore_data_flag_ = false);

    void next() override;
    bool isValid() const override;
    const String & name() const override;
    StoragePtr & table() const override;

private:
    void moveToTable();

    DatabaseLazy & database;
    bool has_force_restore_data_flag;
    Poco::DirectoryIterator dir_it;
};

}
