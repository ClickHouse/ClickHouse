#include <Storages/System/StorageSystemPartAggregationCache.h>
#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

ColumnsDescription StorageSystemPartAggregationCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"query_hash_low", std::make_shared<DataTypeUInt64>(), "Low 64 bits of the query hash (GROUP BY keys, aggregates, WHERE)."},
        {"query_hash_high", std::make_shared<DataTypeUInt64>(), "High 64 bits of the query hash."},
        {"table_id", std::make_shared<DataTypeString>(), "Identity of the table the cached part belongs to (UUID for Atomic databases, full name otherwise)."},
        {"part_name", std::make_shared<DataTypeString>(), "Name of the MergeTree data part."},
        {"result_size_bytes", std::make_shared<DataTypeUInt64>(), "Size of the cached aggregation state in bytes."},
        {"result_rows", std::make_shared<DataTypeUInt64>(), "Number of rows in the cached aggregation state."},
    };
}

StorageSystemPartAggregationCache::StorageSystemPartAggregationCache(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemPartAggregationCache::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    PartAggregationCachePtr cache = context->getPartAggregationCache();

    if (!cache)
        return;

    /// `table_id` and `part_name` identify a data part of a user table, so a user who is not
    /// allowed to see that table must not learn its `UUID` and part names from here. Resolve the
    /// table the entry belongs to and check `SHOW TABLES` on it, the same grant that makes a table
    /// visible in `system.tables` and `system.parts`. An entry whose table cannot be resolved -
    /// dropped after the state was cached, or a table identity that is not a `UUID` - is hidden as
    /// well, so the check fails closed.
    const auto access = context->getAccess();
    const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_TABLES);

    auto is_visible = [&](const String & table_id) -> bool
    {
        UUID uuid;
        if (!tryParse(uuid, table_id))
            return false;

        StoragePtr storage = DatabaseCatalog::instance().tryGetByUUID(uuid).second;
        if (!storage)
            return false;

        const StorageID storage_id = storage->getStorageID();
        return access->isGranted(AccessType::SHOW_TABLES, storage_id.database_name, storage_id.table_name);
    };

    auto entries = cache->dump();

    for (const auto & entry : entries)
    {
        if (check_access_for_tables && !is_visible(entry.key.table_id))
            continue;

        res_columns[0]->insert(entry.key.query_hash.low64);
        res_columns[1]->insert(entry.key.query_hash.high64);
        res_columns[2]->insert(entry.key.table_id);
        res_columns[3]->insert(entry.key.part_name);
        res_columns[4]->insert(entry.size_in_bytes);
        res_columns[5]->insert(entry.rows);
    }
}

}
