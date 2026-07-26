#pragma once

#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Storages/IStorage.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
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
};

class StorageOverwriteCache final : public IStorage, public IKeyValueEntity
{
public:
    using EntryId = UInt64;
    struct RowSegment
    {
        Columns columns;
        UInt64 allocated_bytes = 0;
        std::atomic<UInt64> live_rows{0};
    };

    struct RowData
    {
        std::shared_ptr<RowSegment> segment;
        UInt32 segment_row = 0;
    };

    struct CandidateRow
    {
        UInt32 source_row = 0;
        String encoded_values;
        std::vector<UInt32> value_offsets;
        String primary_key;
        std::vector<String> lookup_keys;
    };

    using RowDataPtr = std::optional<RowData>;
    using RowDataPtrs = std::vector<RowData>;
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
    using ReadGuardPtr = std::shared_ptr<ReadGuard>;
    struct ReadResult
    {
        ReadGuardPtr guard;
        RowDataPtrs rows;
    };
    struct LookupIndex;
    using LookupIndexPtr = std::shared_ptr<LookupIndex>;
    struct LookupIndexSnapshot
    {
        ReadGuardPtr guard;
        Names columns;
        DataTypes types;
        LookupIndexPtr index;
    };
    struct LookupRequest
    {
        ReadGuardPtr guard;
        LookupIndexPtr index;
        std::vector<String> serialized_keys;
    };

    StorageOverwriteCache(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        String comment_,
        String version_column_,
        Names key_columns_,
        std::vector<Names> lookup_indexes_,
        ASTPtr lookup_indexes_ast_,
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
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & lock_holder) override;

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
    std::vector<LookupIndexSnapshot> getLookupIndexSnapshot() const;

    ReadResult getRowsForPrimaryKeys(const std::vector<String> & serialized_keys) const;
    ReadResult getRowsForLookupRequests(const std::vector<LookupRequest> & requests) const;
    size_t getColumnPosition(const String & column_name) const;
    void insertValueIntoColumn(const RowData & row, size_t position, IColumn & column) const;

    void insertBlock(const Block & block);

private:
    struct Candidate;

    struct Entry
    {
        std::optional<RowData> committed;
        std::unique_ptr<RowData> pending;
        UInt64 pending_generation = 0;
    };


    static constexpr size_t primary_shard_count = 256;
    static constexpr size_t posting_shard_count = 256;
    static constexpr size_t row_lock_count = 4096;

    struct PrimaryShard
    {
        mutable std::shared_mutex mutex;
        std::unordered_map<String, EntryId> entries;
    };

public:
    struct PostingShard
    {
        struct Posting
        {
            size_t size() const { return wide.empty() ? narrow.size() : wide.size(); }

            void reserve(size_t capacity, EntryId max_entry_id)
            {
                if (wide.empty() && max_entry_id > std::numeric_limits<UInt32>::max())
                {
                    wide.reserve(capacity);
                    wide.assign(narrow.begin(), narrow.end());
                    std::vector<UInt32>().swap(narrow);
                }
                else if (wide.empty())
                    narrow.reserve(capacity);
                else
                    wide.reserve(capacity);
            }

            void push_back(EntryId entry_id)
            {
                if (wide.empty() && entry_id <= std::numeric_limits<UInt32>::max())
                {
                    narrow.push_back(static_cast<UInt32>(entry_id));
                    return;
                }
                if (wide.empty())
                {
                    wide.reserve(narrow.size() + 1);
                    wide.assign(narrow.begin(), narrow.end());
                    std::vector<UInt32>().swap(narrow);
                }
                wide.push_back(entry_id);
            }

            void resize(size_t new_size)
            {
                if (wide.empty())
                    narrow.resize(new_size);
                else
                    wide.resize(new_size);
            }

            template <typename Callback>
            void forEach(Callback && callback) const
            {
                if (wide.empty())
                {
                    for (const auto entry_id : narrow)
                        callback(static_cast<EntryId>(entry_id));
                }
                else
                {
                    for (const auto entry_id : wide)
                        callback(entry_id);
                }
            }

            bool contains(EntryId entry_id) const
            {
                if (wide.empty())
                {
                    return entry_id <= std::numeric_limits<UInt32>::max()
                        && std::ranges::binary_search(narrow, static_cast<UInt32>(entry_id));
                }
                return std::ranges::binary_search(wide, entry_id);
            }

            UInt64 allocatedBytes() const
            {
                return static_cast<UInt64>(narrow.capacity()) * sizeof(UInt32)
                    + static_cast<UInt64>(wide.capacity()) * sizeof(EntryId);
            }

            std::vector<UInt32> narrow;
            std::vector<EntryId> wide;
        };

        mutable std::shared_mutex mutex;
        std::unordered_map<String, Posting> postings;
    };


    struct LookupIndex
    {
        std::array<PostingShard, posting_shard_count> shards;
        std::atomic<UInt64> accounted_bytes = 0;
    };

private:
    String serializeColumns(const Block & block, size_t row, const std::vector<size_t> & positions) const;
    String serializeRowColumns(const RowData & row, const std::vector<size_t> & positions) const;
    Field getRowField(const RowData & row, size_t position) const;
    Field getCandidateField(const CandidateRow & row, size_t position) const;
    int compareWinner(const CandidateRow & lhs, const CandidateRow & rhs) const;
    int compareWinner(const CandidateRow & lhs, const RowData & rhs) const;
    bool rowsEqual(const CandidateRow & lhs, const RowData & rhs) const;

    size_t primaryShardIndex(const String & key) const;
    size_t postingShardIndex(const String & key) const;
    size_t rowLockIndex(EntryId entry_id) const;
    std::optional<EntryId> findEntry(const String & key) const;
    RowDataPtr resolveEntry(EntryId entry_id, UInt64 snapshot_generation) const;
    std::vector<EntryId> getPostingIds(const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const;
    UInt64 getPostingCardinality(const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const;
    void intersectPostingIds(
        std::vector<EntryId> & entry_ids, const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const;
    void clearData();

    const String version_column;
    const Names key_columns;
    std::vector<Names> lookup_index_columns;
    const OverwriteCacheSettings settings;

    Block sample_block;
    Serializations serializations;
    DataTypes column_types;
    std::unordered_map<String, size_t> column_positions;
    std::vector<size_t> key_positions;
    DataTypes key_column_types;
    size_t version_position = 0;
    std::vector<size_t> tiebreak_positions;
    std::vector<std::vector<size_t>> lookup_index_positions;
    std::vector<DataTypes> lookup_index_column_types;

    mutable std::mutex writer_mutex;
    mutable std::shared_mutex lookup_catalog_mutex;
    std::array<PrimaryShard, primary_shard_count> primary_shards;
    std::vector<LookupIndexPtr> lookup_indexes;
    mutable std::shared_mutex entries_by_id_mutex;
    std::vector<Entry> entries_by_id;
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
