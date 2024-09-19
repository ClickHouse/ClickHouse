#include <Backups/RestoreCoordinationLocal.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Common/logger_useful.h>


namespace DB
{

RestoreCoordinationLocal::RestoreCoordinationLocal() : log(getLogger("RestoreCoordinationLocal"))
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

bool RestoreCoordinationLocal::acquireInsertingDataForKeeperMap(const String & root_zk_path, const String & /*table_unique_id*/)
{
    std::lock_guard lock{mutex};
    return acquired_data_in_keeper_map_tables.emplace(root_zk_path).second;
}

void RestoreCoordinationLocal::generateUUIDForTable(ASTCreateQuery & create_query)
{
    String query_str = serializeAST(create_query);

    auto find_in_map = [&]
    {
        auto it = create_query_uuids.find(query_str);
        if (it != create_query_uuids.end())
        {
            it->second.copyToQuery(create_query);
            return true;
        }
        return false;
    };

    {
        std::lock_guard lock{mutex};
        if (find_in_map())
            return;
    }

    CreateQueryUUIDs new_uuids{create_query, /* generate_random= */ true, /* force_random= */ true};
    new_uuids.copyToQuery(create_query);

    {
        std::lock_guard lock{mutex};
        if (find_in_map())
            return;
        create_query_uuids[query_str] = new_uuids;
    }
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
