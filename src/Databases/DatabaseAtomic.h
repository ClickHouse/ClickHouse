#pragma once

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePool.h>

#include <Databases/DatabaseOrdinary.h>

namespace DB
{

/// All tables in DatabaseAtomic have persistent UUID and store data in
/// /clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
/// where xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy is UUID of the table.
/// RENAMEs are performed without changing UUID and moving table data.
/// Tables in Atomic databases can be accessed by UUID through DatabaseCatalog.
/// On DROP TABLE no data is removed, DatabaseAtomic just marks table as dropped
/// by moving metadata to /clickhouse_path/metadata_dropped/ and notifies DatabaseCatalog.
/// Running queries still may use dropped table. Table will be actually removed when it's not in use.
/// Allows to execute RENAME and DROP without IStorage-level RWLocks
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

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name) override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag) override;

    /// Atomic database cannot be detached if there is detached table which still in use
    void assertCanBeDetached(bool cleenup);

    UUID tryGetTableUUID(const String & table_name) const override;

private:
    void commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path) override;

    void assertDetachedTableNotInUse(const UUID & uuid);
    typedef std::unordered_map<UUID, StoragePtr> DetachedTables;
    [[nodiscard]] DetachedTables cleenupDetachedTables();

    void tryCreateSymlink(const String & table_name, const String & actual_data_path);
    void tryRemoveSymlink(const String & table_name);

    //TODO store path in DatabaseWithOwnTables::tables
    typedef std::unordered_map<String, String> NameToPathMap;
    NameToPathMap table_name_to_path;

    DetachedTables detached_tables;
    const String path_to_table_symlinks;
};

}
