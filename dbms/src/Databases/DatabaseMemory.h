#pragma once

#include <Databases/DatabasesCommon.h>


namespace Poco { class Logger; }


namespace DB
{

/** A non-persistent database to store temporary data.
  * It doesn't make any manipulations with filesystem.
  * All tables are created by calling code.
  * TODO: Maybe DatabaseRuntime is more suitable class name.
  */
class DatabaseMemory : public DatabaseWithOwnTablesBase
{
public:
    DatabaseMemory(String name_);

    String getEngineName() const override { return "Memory"; }

    void loadTables(
        Context & context,
        ThreadPool * thread_pool,
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
        const String & to_table_name) override;

    void alterTable(
        const Context & context,
        const String & name,
        const ColumnsDescription & columns,
        const ASTModifier & engine_modifier) override;

    time_t getTableMetadataModificationTime(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateTableQuery(const Context & context, const String & table_name) const override;
    ASTPtr tryGetCreateTableQuery(const Context &, const String &) const override { return nullptr; }

    ASTPtr getCreateDatabaseQuery(const Context & context) const override;

    void drop() override;

private:
    Poco::Logger * log;
};

}
