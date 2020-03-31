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

    inline static String getPathForUUID(const UUID & uuid)
    {
        const size_t uuid_prefix_len = 3;
        return toString(uuid).substr(0, uuid_prefix_len) + '/' + toString(uuid) + '/';
    }

    void drop(const Context & /*context*/) override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag) override;
    void shutdown() override;

private:
    void commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path) override;

    void assertDetachedTableNotInUse(const UUID & uuid);
    void cleenupDetachedTables();

    //TODO store path in DatabaseWithOwnTables::tables
    std::map<String, String> table_name_to_path;

    std::map<UUID, StoragePtr> detached_tables;
};

}
