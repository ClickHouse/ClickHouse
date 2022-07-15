#include <Backups/RestoreCoordinationLocal.h>


namespace DB
{

RestoreCoordinationLocal::RestoreCoordinationLocal() = default;
RestoreCoordinationLocal::~RestoreCoordinationLocal() = default;

void RestoreCoordinationLocal::setStatus(const String &, const String &, const String &)
{
}

Strings RestoreCoordinationLocal::setStatusAndWait(const String &, const String &, const String &, const Strings &)
{
    return {};
}

Strings RestoreCoordinationLocal::setStatusAndWaitFor(const String &, const String &, const String &, const Strings &, UInt64)
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

}
