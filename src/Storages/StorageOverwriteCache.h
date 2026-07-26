#pragma once

#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Storages/IStorage.h>

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

class StorageFactory;

struct OverwriteCacheSettings
{
    UInt64 max_memory_bytes = 0;
    Names equal_version_tiebreak_columns;
    Names secondary_index_columns;
    std::optional<String> secondary_index_segment_column;
    UInt64 max_secondary_index_rows = 0;
};

class StorageOverwriteCache final : public IStorage, public IKeyValueEntity
{
public:
    using EntryId = UInt64;

    struct RowData
    {
        std::vector<Field> values;
        String primary_key;
        String secondary_key;
        String segmented_secondary_key;
        UInt64 accounted_bytes = 0;
    };

    using RowDataPtr = std::shared_ptr<const RowData>;
    using RowDataPtrs = std::vector<RowDataPtr>;

    StorageOverwriteCache(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        String comment_,
        String version_column_,
        Names key_columns_,
        OverwriteCacheSettings settings_,
        ASTPtr settings_changes_);

    String getName() const override { return "OverwriteCache"; }

    bool prefersLargeBlocks() const override { return false; }
    bool supportsParallelInsert() const override { return false; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableExclusiveLockHolder & table_lock_holder) override;

    void drop() override;

    std::optional<UInt64> totalRows(ContextPtr) const override;
    std::optional<UInt64> totalBytes(ContextPtr) const override;

    Names getPrimaryKey() const override { return key_columns; }
    Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        const Names & required_columns,
        PaddedPODArray<UInt8> & out_null_map,
        IColumn::Offsets & out_offsets) const override;
    Block getSampleBlock(const Names & required_columns) const override;

    const Names & getKeyColumns() const { return key_columns; }
    const DataTypes & getKeyColumnTypes() const { return key_column_types; }
    const Names & getSecondaryIndexColumns() const { return settings.secondary_index_columns; }
    const DataTypes & getSecondaryIndexColumnTypes() const { return secondary_index_column_types; }
    const Names & getSegmentedSecondaryIndexColumns() const { return segmented_secondary_index_columns; }
    const DataTypes & getSegmentedSecondaryIndexColumnTypes() const { return segmented_secondary_index_column_types; }
    UInt64 getMaxSecondaryIndexRows() const { return settings.max_secondary_index_rows; }

    RowDataPtrs getRowsForPrimaryKeys(const std::vector<String> & serialized_keys) const;
    RowDataPtrs getRowsForSecondaryKeys(const std::vector<String> & serialized_keys) const;
    RowDataPtrs getRowsForSegmentedSecondaryKeys(const std::vector<String> & serialized_keys) const;
    size_t getColumnPosition(const String & column_name) const;

    void insertBlock(const Block & block);

private:
    struct Candidate;

    enum class PublicationState : UInt8
    {
        Preparing,
        Committed,
        Aborted,
    };

    struct Publication
    {
        explicit Publication(UInt64 generation_) : generation(generation_) {}

        const UInt64 generation;
        std::atomic<PublicationState> state = PublicationState::Preparing;
    };

    struct Entry
    {
        EntryId id = 0;
        RowDataPtr committed;
        RowDataPtr pending;
        std::shared_ptr<Publication> pending_publication;
    };

    using EntryPtr = std::shared_ptr<Entry>;

    static constexpr size_t primary_shard_count = 256;
    static constexpr size_t posting_shard_count = 256;
    static constexpr size_t row_lock_count = 4096;

    struct PrimaryShard
    {
        mutable std::shared_mutex mutex;
        std::unordered_map<String, EntryPtr> entries;
    };

    struct PostingShard
    {
        mutable std::shared_mutex mutex;
        std::unordered_map<String, std::vector<EntryPtr>> postings;
    };

    class ReadGuard
    {
    public:
        explicit ReadGuard(const StorageOverwriteCache & storage_);
        ~ReadGuard();

        UInt64 generation() const { return snapshot_generation; }

    private:
        const StorageOverwriteCache & storage;
        UInt8 epoch = 0;
        UInt64 snapshot_generation = 0;
    };

    String serializeColumns(const Block & block, size_t row, const std::vector<size_t> & positions) const;
    String serializeFields(const std::vector<Field> & fields, const std::vector<size_t> & positions) const;
    int compareWinner(const RowData & lhs, const RowData & rhs) const;
    bool rowsEqual(const RowData & lhs, const RowData & rhs) const;
    UInt64 estimateRowBytes(const Block & block, size_t row, const RowData & data) const;
    size_t primaryShardIndex(const String & key) const;
    size_t postingShardIndex(const String & key) const;
    size_t rowLockIndex(EntryId entry_id) const;
    EntryPtr findEntry(const String & key) const;
    RowDataPtr resolveEntry(const EntryPtr & entry, UInt64 snapshot_generation) const;
    RowDataPtrs getRowsForPostingKeys(
        const std::vector<String> & serialized_keys,
        const std::array<PostingShard, posting_shard_count> & index,
        const char * index_name) const;
    void clearData();

    const String version_column;
    const Names key_columns;
    const OverwriteCacheSettings settings;

    Block sample_block;
    Serializations serializations;
    DataTypes column_types;
    std::unordered_map<String, size_t> column_positions;
    std::vector<size_t> key_positions;
    DataTypes key_column_types;
    size_t version_position = 0;
    std::vector<size_t> tiebreak_positions;
    std::vector<size_t> secondary_index_positions;
    DataTypes secondary_index_column_types;
    Names segmented_secondary_index_columns;
    std::vector<size_t> segmented_secondary_index_positions;
    DataTypes segmented_secondary_index_column_types;

    mutable std::mutex writer_mutex;
    std::array<PrimaryShard, primary_shard_count> primary_shards;
    std::array<PostingShard, posting_shard_count> secondary_shards;
    std::array<PostingShard, posting_shard_count> segmented_secondary_shards;
    mutable std::array<std::mutex, row_lock_count> row_mutexes;
    EntryId next_entry_id = 1;

    std::atomic<UInt64> published_generation = 0;
    mutable std::atomic<UInt8> active_reader_epoch = 0;
    mutable std::array<std::atomic<UInt64>, 2> active_readers{};

    std::atomic<UInt64> total_size_bytes = 0;
    std::atomic<UInt64> total_size_rows = 0;
};

void registerStorageOverwriteCache(StorageFactory & factory);

}
