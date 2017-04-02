#pragma once

#include <Databases/DatabaseMemory.h>


namespace DB
{

/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseMemory
{
protected:
    const String path;

public:
    DatabaseOrdinary(const String & name_, const String & path_);

    String getEngineName() const override { return "Ordinary"; }

    void loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag) override;

    void createTable(
        const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine, const Settings & settings) override;

    void removeTable(const String & table_name) override;

    void renameTable(
        const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name, const Settings & settings) override;

    time_t getTableMetadataModificationTime(const String & table_name) override;

    ASTPtr getCreateQuery(const String & table_name) const override;

    void shutdown() override;
    void drop() override;

    void alterTable(
        const Context & context,
        const String & name,
        const NamesAndTypesList & columns,
        const NamesAndTypesList & materialized_columns,
        const NamesAndTypesList & alias_columns,
        const ColumnDefaults & column_defaults,
        const ASTModifier & engine_modifier) override;
};

}
