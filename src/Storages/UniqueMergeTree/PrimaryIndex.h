#pragma once

#include <Core/Block.h>
#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/UniqueMergeTree/LRULockFree.h>
#include <base/types.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>

namespace DB
{

class StorageUniqueMergeTree;

struct RecordPosition
{
    RecordPosition() = default;
    RecordPosition(RecordPosition && position) = default;
    RecordPosition(const RecordPosition & position) = default;
    RecordPosition & operator=(const RecordPosition & rhs) = default;
    RecordPosition(Int64 min_block_, UInt32 row_id_, UInt64 version_ = 0) : min_block(min_block_), row_id(row_id_), version(version_) { }
    Int64 min_block;
    UInt32 row_id;
    UInt64 version;
};

using RecordPositionPtr = std::shared_ptr<RecordPosition>;

class PrimaryIndex : public LRULockFree<String, RecordPosition>
{
public:
    using DeletesMap = std::unordered_map<Int64, std::vector<UInt32>>;
    using DeletesKeys = std::unordered_map<Int64, std::vector<String>>;
    enum class State
    {
        VALID,
        UPDATED
    };

    State state = State::VALID;

    PrimaryIndex(const MergeTreePartition & partition_, size_t max_size, StorageUniqueMergeTree & storage_);

    void setState(State state_);

    void update(
        Int64 min_block,
        const ColumnPtr & key_column,
        const ColumnPtr & version_column,
        const ColumnPtr & delete_key_column,
        const std::vector<Field> & min_key_values,
        const std::vector<Field> & max_key_values,
        DeletesMap & deletes_map,
        DeletesKeys & deletes_keys,
        ContextPtr context);

    void update(
        Int64 min_block,
        const ColumnPtr & key_column,
        const ColumnPtr & delete_key_column,
        const std::vector<Field> & min_key_values,
        const std::vector<Field> & max_key_values,
        DeletesMap & deletes_map,
        DeletesKeys & deletes_keys,
        ContextPtr context);

    bool update(Int64 min_block, const ColumnPtr & key_column, const std::unordered_set<UInt32> & deleted_rows_set);

    void deleteKeys(
        const ColumnPtr & delete_key_column,
        const std::vector<Field> & delete_min_values,
        const std::vector<Field> & delete_max_values,
        DeletesMap & deletes_map,
        DeletesKeys & deletes_keys,
        ContextPtr context);

    void processOneBlock(
        Block & block,
        DeletesMap & deletes_map,
        DeletesKeys & deletes_keys,
        const std::unordered_map<String, RecordPositionPtr> & not_found_record_in_cache,
        const std::unordered_set<String> & not_found_deleted_keys_in_cache,
        bool has_version);

    void processWithoutVersion(
        const ColumnPtr & key_column,
        const ColumnPtr & min_block_column,
        const ColumnPtr & row_id_column,
        DeletesMap & deletes_map,
        DeletesKeys & deletes_keys,
        const std::unordered_map<String, RecordPositionPtr> & not_found_record_in_cache,
        const std::unordered_set<String> & not_found_deleted_keys_in_cache);

    void processWithVersion(
        const ColumnPtr & key_column,
        const ColumnPtr & version_column,
        const ColumnPtr & min_block_column,
        const ColumnPtr & row_id_column,
        DeletesMap & deletes_map,
        DeletesKeys & deletes_keys,
        const std::unordered_map<String, RecordPositionPtr> & not_found_record_in_cache,
        const std::unordered_set<String> & not_found_deleted_keys_in_cache);

private:
    MergeTreePartition partition;
    StorageUniqueMergeTree & storage;
    Poco::Logger * log;
};

using PrimaryIndexPtr = std::shared_ptr<PrimaryIndex>;
}
