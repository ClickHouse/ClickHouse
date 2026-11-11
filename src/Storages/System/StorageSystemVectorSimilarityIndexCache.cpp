#include <Storages/System/StorageSystemVectorSimilarityIndexCache.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

ColumnsDescription StorageSystemVectorSimilarityIndexCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the table the cached granule belongs to."},
        {"table", std::make_shared<DataTypeString>(), "Table the cached granule belongs to."},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "UUID of the table. Stable across renames. Nil for tables in an Ordinary database."},
        {"disk_name", std::make_shared<DataTypeString>(), "Disk of the cached data part."},
        {"part_name", std::make_shared<DataTypeString>(), "Name of the cached data part."},
        {"index_name", std::make_shared<DataTypeString>(), "DDL name of the vector similarity index."},
        {"index_mark", std::make_shared<DataTypeUInt64>(), "Mark of the cached vector similarity index granule."},
        {"size", std::make_shared<DataTypeUInt64>(), "Accounted size of the cache entry in bytes."},
    };
}

StorageSystemVectorSimilarityIndexCache::StorageSystemVectorSimilarityIndexCache(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemVectorSimilarityIndexCache::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto cache = context->getVectorSimilarityIndexCache();
    if (!cache)
        return;

    const auto access = context->getAccess();
    const bool has_global_show_tables = access->isGranted(AccessType::SHOW_TABLES);
    const bool has_global_show_columns = access->isGranted(AccessType::SHOW_COLUMNS);

    for (const auto & [key, cell] : cache->dump())
    {
        const UUID table_uuid = cell ? cell->table_uuid : UUIDHelpers::Nil;
        String database_name;
        String table_name;
        DatabaseAndTable database_and_table;
        if (table_uuid != UUIDHelpers::Nil)
            database_and_table = DatabaseCatalog::instance().tryGetByUUID(table_uuid);
        const auto & [database, table] = database_and_table;
        if (database && table)
        {
            database_name = database->getDatabaseName();
            table_name = table->getStorageID().table_name;
            if (!access->isGranted(AccessType::SHOW_TABLES, database_name, table_name)
                || !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name))
                continue;
        }
        else if (!has_global_show_tables || !has_global_show_columns)
        {
            continue;
        }

        String disk_name;
        if (const auto pos = key.path_to_data_part.find(':'); pos != String::npos)
            disk_name = key.path_to_data_part.substr(0, pos);

        size_t i = 0;
        res_columns[i++]->insert(database_name);
        res_columns[i++]->insert(table_name);
        res_columns[i++]->insert(table_uuid);
        res_columns[i++]->insert(disk_name);
        res_columns[i++]->insert(cell ? cell->part_name : "");
        res_columns[i++]->insert(cell ? cell->logical_index_name : key.index_name);
        res_columns[i++]->insert(key.index_mark);
        res_columns[i++]->insert(cell ? cell->memory_bytes : 0);
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemVectorSimilarityIndexCache) }
