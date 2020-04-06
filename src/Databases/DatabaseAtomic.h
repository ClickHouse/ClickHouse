#pragma once

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePool.h>

#include <Databases/DatabaseOrdinary.h>

namespace DB
{

class DatabaseAtomic : public DatabaseOrdinary
{
public:

    DatabaseAtomic(String name_, String metadata_path_, Context & context_);

    String getEngineName() const override { return "Atomic"; }

    void renameTable(
            const Context & context,
            const String & table_name,
            IDatabase & to_database,
            const String & to_table_name,
            bool exchange) override;

    void dropTable(const Context & context, const String & table_name, bool no_delay) override;

    void attachTable(const String & name, const StoragePtr & table, const String & relative_table_path) override;
    StoragePtr detachTable(const String & name) override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    void drop(const Context & /*context*/) override;

    DatabaseTablesIteratorPtr getTablesIterator(const FilterByNameFunction & filter_by_table_name) override;
    DatabaseTablesIteratorPtr getTablesWithDictionaryTablesIterator(const FilterByNameFunction & filter_by_dictionary_name) override;

private:
    void commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path) override;

    void assertDetachedTableNotInUse(const UUID & uuid);
    typedef std::map<UUID, StoragePtr> DetachedTables;
    DetachedTables cleenupDetachedTables();

    //TODO store path in DatabaseWithOwnTables::tables
    std::map<String, String> table_name_to_path;

    DetachedTables detached_tables;
};

}
