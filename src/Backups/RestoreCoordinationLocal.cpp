#include <Backups/RestoreCoordinationLocal.h>
#include <Common/logger_useful.h>


namespace DB
{

RestoreCoordinationLocal::RestoreCoordinationLocal() : log(&Poco::Logger::get("RestoreCoordinationLocal"))
{
}

RestoreCoordinationLocal::~RestoreCoordinationLocal() = default;

void RestoreCoordinationLocal::setStage(const String &, const String &)
{
}

void RestoreCoordinationLocal::setError(const Exception &)
{
}

Strings RestoreCoordinationLocal::waitForStage(const String &)
{
    return {};
}

Strings RestoreCoordinationLocal::waitForStage(const String &, std::chrono::milliseconds)
{
    return {};
}

bool RestoreCoordinationLocal::acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name)
{
    std::lock_guard lock{mutex};
    return acquired_tables_in_replicated_databases.emplace(std::pair<String, String>{database_zk_path, table_name}).second;
}

bool RestoreCoordinationLocal::acquireInsertingDataIntoReplicatedTable(const String & table_zk_path)
{
    std::lock_guard lock{mutex};
    return acquired_data_in_replicated_tables.emplace(table_zk_path).second;
}

bool RestoreCoordinationLocal::acquireReplicatedAccessStorage(const String &)
{
    return true;
}

bool RestoreCoordinationLocal::acquireReplicatedSQLObjects(const String &, UserDefinedSQLObjectType)
{
    return true;
}

bool RestoreCoordinationLocal::hasConcurrentRestores(const std::atomic<size_t> & num_active_restores) const
{
    if (num_active_restores > 1)
    {
        LOG_WARNING(log, "Found concurrent backups: num_active_restores={}", num_active_restores);
        return true;
    }
    return false;
}

}
