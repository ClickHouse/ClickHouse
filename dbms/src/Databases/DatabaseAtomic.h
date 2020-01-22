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
            const String & to_table_name) override;

    //void removeTable(const Context & context, const String & table_name) override;
    void dropTable(const Context & context, const String & table_name) override;

    void attachTable(const String & name, const StoragePtr & table, const String & relative_table_path = {}) override;
    StoragePtr detachTable(const String & name) override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    void drop(const Context & /*context*/) override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag) override;
    void shutdown() override;

private:
    void dropTableDataTask();

private:
    static constexpr time_t drop_delay_s = 10;
    static constexpr size_t reschedule_time_ms = 5000;

    //TODO store path in DatabaseWithOwnTables::tables
    std::map<String, String> table_name_to_path;
    struct TableToDrop
    {
        StoragePtr table;
        String data_path;
        time_t drop_time;
        //time_t last_attempt_time;
    };
    using TablesToDrop = std::list<TableToDrop>;
    TablesToDrop tables_to_drop;
    std::mutex tables_to_drop_mutex;

    BackgroundSchedulePoolTaskHolder drop_task;

};

}
