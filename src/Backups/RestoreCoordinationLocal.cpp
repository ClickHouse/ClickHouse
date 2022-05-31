#include <Backups/RestoreCoordinationLocal.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Interpreters/StorageID.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RestoreCoordinationLocal::RestoreCoordinationLocal() = default;
RestoreCoordinationLocal::~RestoreCoordinationLocal() = default;

void RestoreCoordinationLocal::syncStage(const String &, int, const Strings &, std::chrono::seconds)
{
}

void RestoreCoordinationLocal::syncStageError(const String &, const String &)
{
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

}
