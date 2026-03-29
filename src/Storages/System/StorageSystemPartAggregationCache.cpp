#include <Storages/System/StorageSystemPartAggregationCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemPartAggregationCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"query_hash", std::make_shared<DataTypeUInt64>(), "Hash of the query (GROUP BY keys, aggregates, WHERE)."},
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

    auto entries = cache->dump();

    for (const auto & entry : entries)
    {
        res_columns[0]->insert(entry.key.query_hash.low64);
        res_columns[1]->insert(entry.key.part_name);
        res_columns[2]->insert(entry.size_in_bytes);
        res_columns[3]->insert(entry.rows);
    }
}

}
