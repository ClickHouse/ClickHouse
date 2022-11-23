#include <filesystem>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageUniqueMergeTree.h>
#include <Storages/UniqueMergeTree/PrimaryIndex.h>
#include <Storages/UniqueMergeTree/PrimaryKeysEncoder.h>
#include <Common/PODArray.h>
#include <Common/filesystemHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
}

PrimaryIndex::PrimaryIndex(const MergeTreePartition & partition_, size_t max_size, StorageUniqueMergeTree & storage_)
    : LRULockFree(max_size)
    , partition(partition_)
    , storage(storage_)
    , log(&Poco::Logger::get("PrimaryIndex (" + partition_.getID(storage) + ")"))
{
}

void PrimaryIndex::setState(PrimaryIndex::State state_)
{
    state = state_;
}

void PrimaryIndex::update(
    Int64 min_block,
    const ColumnPtr & key_column,
    const ColumnPtr & version_column,
    const ColumnPtr & delete_key_column,
    const std::vector<Field> & min_key_values,
    const std::vector<Field> & max_key_values,
    DeletesMap & deletes_map,
    DeletesKeys & deletes_keys,
    ContextPtr context)
{
    assert(state == State::VALID);
    bool insert_not_found = false;
    bool delete_not_found = false;

    std::unordered_map<String, RecordPositionPtr> not_found_record_in_cache;
    // First, check in cache
    if (!key_column || !version_column)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "key column or version column is Null when update primary index");
    }

    if (key_column->size() != version_column->size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "key column and version column should have same size, but key column has size {} while version column has size {}",
            key_column->size(),
            version_column->size());
    }

    for (size_t row_id = 0; row_id < key_column->size(); ++row_id)
    {
        auto key = key_column->getDataAt(row_id).toString();
        auto version = version_column->getUInt(row_id);
        if (auto it = get(key))
        {
            if (it->version < version)
            {
                deletes_map[it->min_block].push_back(it->row_id);
                deletes_keys[it->min_block].push_back(key);
                it->min_block = min_block;
                it->row_id = row_id;
                it->version = version;
            }
            /// The data insert failed
            else
            {
                deletes_map[min_block].push_back(row_id);
                deletes_keys[it->min_block].push_back(key);
            }
        }
        else
        {
            insert_not_found = true;
            auto value = std::make_shared<RecordPosition>(min_block, row_id, version);
            not_found_record_in_cache.emplace(key, value);
        }
    }

    std::unordered_set<String> not_found_deleted_keys_in_cache;
    /// delete_key_column can be nullptr due to no delete
    if (delete_key_column)
    {
        for (size_t i = 0; i < delete_key_column->size(); ++i)
        {
            auto delete_key = key_column->getDataAt(i).toString();
            if (auto [value, found] = removeWithReturn(delete_key); found)
            {
                deletes_map[value->min_block].push_back(value->row_id);
                deletes_keys[value->min_block].push_back(delete_key);
            }
            else
            {
                delete_not_found = true;
                not_found_deleted_keys_in_cache.emplace(delete_key);
            }
        }
    }

    /// All cache hits
    if (!insert_not_found && !delete_not_found)
    {
        LOG_INFO(log, "All unique key hits in cache");
        return;
    }

    LOG_INFO(log, "Starting fetch and construct primary index");

    auto query = storage.getFetchIndexQuery(partition, min_key_values, max_key_values);
    InterpreterSelectQuery fetch_index(query, context, SelectQueryOptions(QueryProcessingStage::Complete));

    auto builder = fetch_index.buildQueryPipeline();
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;
        processOneBlock(block, deletes_map, deletes_keys, not_found_record_in_cache, not_found_deleted_keys_in_cache, true);
    }
}

void PrimaryIndex::update(
    Int64 min_block,
    const ColumnPtr & key_column,
    const ColumnPtr & delete_key_column,
    const std::vector<Field> & min_key_values,
    const std::vector<Field> & max_key_values,
    DeletesMap & deletes_map,
    DeletesKeys & deletes_keys,
    ContextPtr context)
{
    assert(state == State::VALID);
    bool insert_not_found = false;
    bool delete_not_found = false;

    std::unordered_map<String, RecordPositionPtr> not_found_record_in_cache;
    // First, check in cache
    if (!key_column)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "key column is Null when update primary index");
    }

    for (size_t row_id = 0; row_id < key_column->size(); ++row_id)
    {
        auto key = key_column->getDataAt(row_id).toString();
        if (auto it = get(key))
        {
            deletes_map[it->min_block].push_back(it->row_id);
            deletes_keys[it->min_block].push_back(key);
            it->min_block = min_block;
            it->row_id = row_id;
        }
        else
        {
            insert_not_found = true;
            auto value = std::make_shared<RecordPosition>(min_block, row_id);
            not_found_record_in_cache.emplace(key, value);
        }
    }

    std::unordered_set<String> not_found_deleted_keys_in_cache;
    /// delete_key_column can be nullptr due to no delete
    if (delete_key_column)
    {
        for (size_t i = 0; i < delete_key_column->size(); ++i)
        {
            auto delete_key = key_column->getDataAt(i).toString();
            if (auto [value, found] = removeWithReturn(delete_key); found)
            {
                deletes_map[value->min_block].push_back(value->row_id);
                deletes_keys[value->min_block].push_back(delete_key);
            }
            else
            {
                delete_not_found = true;
                not_found_deleted_keys_in_cache.emplace(delete_key);
            }
        }
    }

    /// All cache hits
    if (!insert_not_found && !delete_not_found)
    {
        LOG_INFO(log, "All unique key hits in cache");
        return;
    }

    LOG_INFO(log, "Starting fetch and construct primary index");

    auto query = storage.getFetchIndexQuery(partition, min_key_values, max_key_values);
    InterpreterSelectQuery fetch_index(query, context, SelectQueryOptions(QueryProcessingStage::Complete));

    auto builder = fetch_index.buildQueryPipeline();
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;
        processOneBlock(block, deletes_map, deletes_keys, not_found_record_in_cache, not_found_deleted_keys_in_cache, false);
    }
}

bool PrimaryIndex::update(Int64 min_block, const ColumnPtr & key_column, const std::unordered_set<UInt32> & deleted_rows_set)
{
    bool has_update = false;
    for (size_t i = 0; i < key_column->size(); ++i)
    {
        if (deleted_rows_set.contains(i))
            continue;
        auto key = key_column->getDataAt(i).toString();
        if (auto it = get(key))
        {
            it->min_block = min_block;
            it->row_id = i;
            has_update = true;
        }
    }
    return has_update;
}

void PrimaryIndex::deleteKeys(
    const ColumnPtr & delete_key_column,
    const std::vector<Field> & delete_min_values,
    const std::vector<Field> & delete_max_values,
    DeletesMap & deletes_map,
    DeletesKeys & deletes_keys,
    ContextPtr context)
{
    assert(state == State::VALID);

    bool delete_not_found = false;
    std::unordered_set<String> not_found_deleted_keys_in_cache;
    for (size_t i = 0; i < delete_key_column->size(); ++i)
    {
        auto delete_key = delete_key_column->getDataAt(i).toString();
        if (auto [value, found] = removeWithReturn(delete_key); found)
        {
            deletes_map[value->min_block].push_back(value->row_id);
            deletes_keys[value->min_block].push_back(delete_key);
        }
        else
        {
            delete_not_found = true;
            not_found_deleted_keys_in_cache.emplace(delete_key);
        }
    }

    /// All cache hits
    if (!delete_not_found)
    {
        LOG_INFO(log, "All unique key hits in cache");
        return;
    }

    LOG_INFO(log, "Starting fetch and construct primary index");

    auto query = storage.getFetchIndexQuery(partition, delete_min_values, delete_max_values);
    InterpreterSelectQuery fetch_index(query, context, SelectQueryOptions(QueryProcessingStage::Complete));

    auto builder = fetch_index.buildQueryPipeline();
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    PullingPipelineExecutor executor(pipeline);

    std::unordered_map<String, RecordPositionPtr> not_found_record_in_cache;
    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;
        processOneBlock(block, deletes_map, deletes_keys, not_found_record_in_cache, not_found_deleted_keys_in_cache, false);
    }
}
void PrimaryIndex::processOneBlock(
    Block & block,
    DeletesMap & deletes_map,
    DeletesKeys & deletes_keys,
    const std::unordered_map<String, RecordPositionPtr> & not_found_record_in_cache,
    const std::unordered_set<String> & not_found_deleted_keys_in_cache,
    bool has_version)
{
    auto metadata = storage.getInMemoryMetadataPtr();
    storage.getUniqueKeyExpression(metadata)->execute(block);

    auto unique_keys = metadata->getUniqueKey().column_names;
    ColumnRawPtrs key_columns;
    for (const auto & name : unique_keys)
    {
        key_columns.emplace_back(block.getByName(name).column.get());
    }

    auto key_column = PrimaryKeysEncoder::encode(key_columns, block.rows(), unique_keys.size());
    auto min_block_column = block.getByName("_part_min_block").column;
    auto row_id_column = block.getByName("_part_offset").column;
    if (has_version)
    {
        auto version_column = block.getByName(storage.merging_params.version_column).column;
        processWithVersion(
            key_column,
            version_column,
            min_block_column,
            row_id_column,
            deletes_map,
            deletes_keys,
            not_found_record_in_cache,
            not_found_deleted_keys_in_cache);
    }
    else
    {
        processWithoutVersion(
            key_column,
            min_block_column,
            row_id_column,
            deletes_map,
            deletes_keys,
            not_found_record_in_cache,
            not_found_deleted_keys_in_cache);
    }
}

void PrimaryIndex::processWithoutVersion(
    const ColumnPtr & key_column,
    const ColumnPtr & min_block_column,
    const ColumnPtr & row_id_column,
    DeletesMap & deletes_map,
    DeletesKeys & deletes_keys,
    const std::unordered_map<String, RecordPositionPtr> & not_found_record_in_cache,
    const std::unordered_set<String> & not_found_deleted_keys_in_cache)
{
    for (size_t i = 0; i < key_column->size(); ++i)
    {
        auto key = key_column->getDataAt(i).toString();
        auto min_block = min_block_column->getInt(i);
        auto row_id = row_id_column->getUInt(i);

        /// Its new key not found in cache
        if (auto it = not_found_record_in_cache.find(key); it != not_found_record_in_cache.end())
        {
            deletes_map[min_block].push_back(row_id);
            deletes_keys[min_block].push_back(key);
            set(key, it->second);
        }
        /// Its delete
        else if (not_found_deleted_keys_in_cache.count(key))
        {
            deletes_map[min_block].push_back(row_id);
            deletes_keys[min_block].push_back(key);
        }
        /// Put into cache
        else
        {
            if (!get(key))
            {
                auto value = std::make_shared<RecordPosition>(min_block, row_id);
                set(key, value);
            }
        }
    }
}

void PrimaryIndex::processWithVersion(
    const ColumnPtr & key_column,
    const ColumnPtr & version_column,
    const ColumnPtr & min_block_column,
    const ColumnPtr & row_id_column,
    DeletesMap & deletes_map,
    DeletesKeys & deletes_keys,
    const std::unordered_map<String, RecordPositionPtr> & not_found_record_in_cache,
    const std::unordered_set<String> & not_found_deleted_keys_in_cache)
{
    for (size_t i = 0; i < key_column->size(); ++i)
    {
        auto key = key_column->getDataAt(i).toString();
        auto version = version_column->getUInt(i);
        auto min_block = min_block_column->getInt(i);
        auto row_id = row_id_column->getUInt(i);

        /// Its new key not found in cache
        if (auto it = not_found_record_in_cache.find(key); it != not_found_record_in_cache.end())
        {
            if (it->second->version > version)
            {
                deletes_map[min_block].push_back(row_id);
                deletes_keys[min_block].push_back(key);
            }
            else
            {
                deletes_map[it->second->min_block].push_back(it->second->row_id);
                deletes_keys[it->second->min_block].push_back(key);
            }
        }
        /// Its delete
        else if (not_found_deleted_keys_in_cache.contains(key))
        {
            deletes_map[min_block].push_back(row_id);
            deletes_keys[min_block].push_back(key);
        }
        /// Put into cache
        else
        {
            if (!get(key))
            {
                auto value = std::make_shared<RecordPosition>(min_block, row_id, version);
                set(key, value);
            }
        }
    }
}
}
