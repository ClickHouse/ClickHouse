#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Storages/IStorage.h>
#include <Storages/OverwriteCachePersistence.h>

#include <base/StringViewHash.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Common/SharedMutex.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <condition_variable>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

class StorageFactory;
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;

struct OverwriteCacheSettings
{
    UInt64 max_memory_bytes = 0;
    UInt64 max_pending_insert_bytes = 0;
    UInt64 max_concurrent_insert_preparations = 8;
    UInt64 max_insert_publication_threads = 8;
    UInt64 background_compaction_target_segment_bytes = 64ULL * 1024 * 1024;
    UInt64 background_compaction_min_segment_count = 8;
    Names equal_version_tiebreak_columns;
    bool compress_segments = false;
    OverwriteCachePersistMode persist_mode = OverwriteCachePersistMode::Async;
    String disk_name = "default";
};

class StorageOverwriteCache final : public IStorage, public IKeyValueEntity, WithContext
{
public:
    using EntryId = UInt64;
    struct RowSegment
    {
        Columns columns;
        /// One entry identifier per stored row. A row is dead when its entry no longer points back here,
        /// which makes compaction proportional to the segment instead of to the whole table.
        std::vector<EntryId> entry_ids;
        UInt64 allocated_bytes = 0;
        std::atomic<UInt64> live_rows{0};
        /// The file this segment is stored in, or zero while the table is not persisted. A segment is
        /// immutable, so it is written once and its file is removed when the segment is released.
        UInt64 persistent_id = 0;
    };

    struct RowData
    {
        std::shared_ptr<RowSegment> segment;
        UInt32 segment_row = 0;
    };

    /// Memoizes decompressed segment columns for the duration of one operation. Without it every row
    /// access decompresses a whole segment column, which makes reads and replacements quadratic.
    class SegmentColumnCache
    {
    public:
        const IColumn & get(const RowSegment & segment, size_t position)
        {
            auto & columns = cache[&segment];
            if (columns.empty())
                columns.resize(segment.columns.size());
            auto & column = columns[position];
            if (!column)
                column = segment.columns[position]->decompress();
            return *column;
        }

    private:
        std::unordered_map<const RowSegment *, Columns> cache;
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
        UInt8 snapshot_shard = 0;
        UInt64 snapshot_generation = 0;
    };
    using ReadGuardPtr = std::shared_ptr<ReadGuard>;
    struct ReadResult
    {
        ReadGuardPtr guard;
        std::vector<EntryId> entry_ids;
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
        ASTPtr settings_changes_,
        DiskPtr disk_,
        const String & relative_data_path_,
        ContextPtr context_);

    String getName() const override { return "OverwriteCache"; }

    void startup() override;

    bool prefersLargeBlocks() const override { return false; }
    bool supportsParallelInsert() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context, bool async_insert) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr query_context,
        TableExclusiveLockHolder & table_lock_holder) override;
    void drop() override;
    void shutdown(bool is_drop) override;
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    void backupData(
        BackupEntriesCollector & backup_entries_collector,
        const String & data_path_in_backup,
        const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(
        RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr query_context) const override;
    void alter(const AlterCommands & commands, ContextPtr query_context, AlterLockHolder & lock_holder) override;

    /// `DELETE FROM` is executed by the storage itself rather than translated into a lightweight
    /// update, because there is no part to rewrite and no `_row_exists` column to mask.
    bool supportsDelete() const override { return true; }
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr query_context) override;

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
    RowDataPtr resolveEntry(EntryId entry_id, UInt64 snapshot_generation) const;
    size_t getColumnPosition(const String & column_name) const;
    void insertValueIntoColumn(const RowData & row, size_t position, IColumn & column, SegmentColumnCache & cache) const;

    void insertBlock(const Block & block);

private:
    /// While replaying the persisted log, `replay_segment_id` names the file the block came from, so that
    /// the segment it produces keeps mirroring that file instead of being written out a second time.
    void insertBlock(const Block & block, UInt64 replay_segment_id);
    /// Removes the keys of `block` from the cache. The rows to remove are produced by the read path,
    /// so a `DELETE` accepts exactly the predicates a `SELECT` accepts.
    void deleteBlock(const Block & block);
    size_t deleteKeys(const std::vector<String> & serialized_keys);

    /// Replays the persisted log, then starts persisting. Runs from the constructor, so an `ATTACH` that
    /// cannot restore the table fails instead of exposing an incomplete cache.
    void loadPersistedData();
    void restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup);
    /// Builds the identity a persisted log is checked against. A log written for different columns, keys,
    /// version or tie-break columns would be reinterpreted rather than rejected.
    String getPersistenceFingerprint() const;

    struct EntryVersion
    {
        RowData row;
        /// The publication generation at which this version became visible.
        UInt64 generation = 0;
        std::unique_ptr<EntryVersion> older;
    };

    /// Chains are normally one version long. They grow only while a reader holds an older snapshot,
    /// which is what lets a writer publish without ever waiting for readers to finish.
    static void releaseVersions(std::unique_ptr<EntryVersion> version)
    {
        /// Iteratively, because a chain can outgrow the stack if a reader lags far behind.
        while (version)
            version = std::move(version->older);
    }

    /// A publication takes one version per row and usually hands it straight back once the previous
    /// one becomes unreachable, so recycling keeps that off the allocator. Post-publication pruning
    /// may return versions while the next writer takes them, so the pool has its own mutex.
    std::unique_ptr<EntryVersion> takeVersion();
    void recycleVersions(std::unique_ptr<EntryVersion> chain);
    static constexpr size_t max_recycled_versions = 1 << 16;

    struct Entry
    {
        Entry() = default;
        Entry(Entry &&) = default;
        Entry & operator=(Entry && rhs) noexcept
        {
            releaseVersions(std::move(head));
            head = std::move(rhs.head);
            return *this;
        }
        ~Entry() { releaseVersions(std::move(head)); }

        /// Newest first. A reader takes the first version at or below its snapshot generation.
        std::unique_ptr<EntryVersion> head;
    };

    /// Chunked, never-relocating entry array. Chunk sizes double, so a tiny table costs a few entries
    /// while a large one needs only a handful of chunks. Readers reach an entry without any lock.
    class EntryTable
    {
    public:
        ~EntryTable() { clear(); }

        size_t size() const { return entry_count.load(std::memory_order_acquire); }

        Entry & at(EntryId entry_id) const
        {
            const size_t index = static_cast<size_t>(entry_id) - 1;
            if (index < base_size)
                return chunks[0].load(std::memory_order_acquire)[index];
            const size_t level = std::bit_width(index) - base_shift;
            return chunks[level].load(std::memory_order_acquire)[index - (1ULL << (level + base_shift - 1))];
        }

        /// Runs under the writer lock.
        void grow(size_t new_size);
        void clear();

        UInt64 allocatedBytes() const { return static_cast<UInt64>(allocated_entries) * sizeof(Entry); }

    private:
        static constexpr size_t base_shift = 2;
        static constexpr size_t base_size = 1ULL << base_shift;
        static constexpr size_t max_chunks = 8 * sizeof(size_t) - base_shift + 1;

        static size_t chunkCapacity(size_t level) { return level ? 1ULL << (level + base_shift - 1) : base_size; }

        std::array<std::atomic<Entry *>, max_chunks> chunks{};
        size_t chunk_count = 0;
        size_t allocated_entries = 0;
        std::atomic<size_t> entry_count = 0;
    };

    /// Serialized keys of one input block for one column tuple, packed into a single buffer.
    struct SerializedKeys
    {
        PODArray<char> data;
        PODArray<UInt64> offsets;
        PODArray<UInt64> hashes;

        std::string_view at(size_t row) const
        {
            const UInt64 begin = row ? offsets[row - 1] : 0;
            return {data.data() + begin, offsets[row] - begin};
        }
    };

    static constexpr size_t primary_shard_count = 256;
    static constexpr size_t posting_shard_count = 256;
    static constexpr size_t row_lock_count = 4096;
    static constexpr size_t snapshot_shard_count = 64;
    static_assert(primary_shard_count == 256 && posting_shard_count == 256, "shardIndex takes the top eight hash bits");

    /// A small initial capacity keeps the fixed cost of a nearly empty shard low.
    using PrimaryMap = HashMapWithSavedHash<std::string_view, EntryId, StringViewHash, HashTableGrowerWithPrecalculation<3>>;

    struct PrimaryShard
    {
        mutable SharedMutex mutex;
        PrimaryMap entries;
        /// Owns the key bytes referenced by `entries`.
        std::unique_ptr<Arena> arena = std::make_unique<Arena>();
    };

    struct SnapshotRegistryShard
    {
        mutable std::mutex mutex;
        std::map<UInt64, size_t> generations;
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

            bool insert(EntryId entry_id)
            {
                if (wide.empty() && entry_id <= std::numeric_limits<UInt32>::max())
                {
                    const auto value = static_cast<UInt32>(entry_id);
                    if (narrow.empty() || narrow.back() < value)
                    {
                        narrow.push_back(value);
                        return true;
                    }
                    if (narrow.back() == value)
                        return false;
                    const auto it = std::ranges::lower_bound(narrow, value);
                    if (it != narrow.end() && *it == value)
                        return false;
                    narrow.insert(it, value);
                    return true;
                }
                if (wide.empty())
                {
                    wide.reserve(narrow.size() + 1);
                    wide.assign(narrow.begin(), narrow.end());
                    std::vector<UInt32>().swap(narrow);
                }
                if (wide.empty() || wide.back() < entry_id)
                {
                    wide.push_back(entry_id);
                    return true;
                }
                if (wide.back() == entry_id)
                    return false;
                const auto it = std::ranges::lower_bound(wide, entry_id);
                if (it != wide.end() && *it == entry_id)
                    return false;
                wide.insert(it, entry_id);
                return true;
            }

            template <typename Iterator>
            bool insertSorted(Iterator additions_begin, Iterator additions_end)
            {
                if (additions_begin == additions_end)
                    return true;

                const EntryId max_entry_id = *(additions_end - 1);
                if (wide.empty() && max_entry_id > std::numeric_limits<UInt32>::max())
                {
                    wide.reserve(narrow.size() + static_cast<size_t>(additions_end - additions_begin));
                    wide.assign(narrow.begin(), narrow.end());
                    std::vector<UInt32>().swap(narrow);
                }

                const auto add = [&](auto & values)
                {
                    /// A publication allocates entry identifiers above every identifier already stored, so
                    /// every addition follows the tail in the common case. Appending keeps publication
                    /// proportional to the batch, where the merge below would scan the whole posting while
                    /// the shard is locked exclusively and readers of that shard cannot proceed.
                    if (values.empty() || static_cast<EntryId>(values.back()) < *additions_begin)
                    {
                        if (std::adjacent_find(additions_begin, additions_end) != additions_end)
                            return false;

                        values.insert(values.end(), additions_begin, additions_end);
                        return true;
                    }

                    /// A resurrected key reuses the identifier it had before its tombstone, so its addition
                    /// can precede identifiers already stored and the general merge is required.
                    auto existing = values.begin();
                    auto addition = additions_begin;
                    while (existing != values.end() && addition != additions_end)
                    {
                        const EntryId existing_id = static_cast<EntryId>(*existing);
                        if (existing_id < *addition)
                            ++existing;
                        else if (*addition < existing_id)
                            ++addition;
                        else
                            return false;
                    }
                    if (std::adjacent_find(additions_begin, additions_end) != additions_end)
                        return false;

                    const size_t old_size = values.size();
                    values.insert(values.end(), additions_begin, additions_end);
                    std::inplace_merge(values.begin(), values.begin() + old_size, values.end());
                    return true;
                };

                if (!wide.empty() || max_entry_id > std::numeric_limits<UInt32>::max())
                    return add(wide);
                return add(narrow);
            }

            bool erase(EntryId entry_id)
            {
                if (wide.empty())
                {
                    if (entry_id > std::numeric_limits<UInt32>::max())
                        return false;
                    const auto value = static_cast<UInt32>(entry_id);
                    const auto it = std::ranges::lower_bound(narrow, value);
                    if (it == narrow.end() || *it != value)
                        return false;
                    narrow.erase(it);
                    return true;
                }
                const auto it = std::ranges::lower_bound(wide, entry_id);
                if (it == wide.end() || *it != entry_id)
                    return false;
                wide.erase(it);
                return true;
            }

            template <typename Iterator, typename GetEntryId>
            void eraseSortedAndCompact(Iterator removals_begin, Iterator removals_end, GetEntryId && get_entry_id)
            {
                const auto compact = [&](auto & values)
                {
                    using Value = typename std::decay_t<decltype(values)>::value_type;
                    size_t survivors = 0;
                    auto removal = removals_begin;
                    for (const auto value : values)
                    {
                        const auto entry_id = static_cast<EntryId>(value);
                        while (removal != removals_end && get_entry_id(*removal) < entry_id)
                            ++removal;
                        if (removal == removals_end || get_entry_id(*removal) != entry_id)
                            ++survivors;
                    }

                    std::vector<Value> compacted;
                    compacted.reserve(survivors);
                    removal = removals_begin;
                    for (const auto value : values)
                    {
                        const auto entry_id = static_cast<EntryId>(value);
                        while (removal != removals_end && get_entry_id(*removal) < entry_id)
                            ++removal;
                        if (removal == removals_end || get_entry_id(*removal) != entry_id)
                            compacted.push_back(value);
                    }
                    values.swap(compacted);
                };

                if (wide.empty())
                    compact(narrow);
                else
                    compact(wide);
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
                return static_cast<UInt64>(narrow.capacity()) * sizeof(UInt32) + static_cast<UInt64>(wide.capacity()) * sizeof(EntryId);
            }

            std::vector<UInt32> narrow;
            std::vector<EntryId> wide;
        };

        /// The hash map stores only a position, so its cells stay trivially relocatable while the
        /// postings themselves keep stable addresses in the deque.
        using PostingMap = HashMapWithSavedHash<std::string_view, UInt32, StringViewHash, HashTableGrowerWithPrecalculation<3>>;

        const Posting * find(std::string_view key, size_t hash) const
        {
            const auto * it = index.find(key, hash);
            return it ? &postings[it->getMapped()] : nullptr;
        }

        Posting * find(std::string_view key, size_t hash)
        {
            auto * it = index.find(key, hash);
            return it ? &postings[it->getMapped()] : nullptr;
        }

        mutable SharedMutex mutex;
        PostingMap index;
        std::deque<Posting> postings;
        std::unique_ptr<Arena> arena = std::make_unique<Arena>();
    };


    struct LookupIndex
    {
        std::array<PostingShard, posting_shard_count> shards;
        std::atomic<UInt64> accounted_bytes = 0;
    };

private:
    struct PendingPostingRemoval
    {
        std::weak_ptr<LookupIndex> index;
        EntryId entry_id = 0;
        UInt64 tombstone_generation = 0;
        UInt32 posting_position = 0;
        UInt8 shard_index = 0;
    };

    void serializeKeys(const Block & block, const std::vector<size_t> & positions, SerializedKeys & result) const;
    String serializeColumns(const Block & block, size_t row, const std::vector<size_t> & positions) const;
    String serializeRowColumns(const RowData & row, const std::vector<size_t> & positions, SegmentColumnCache & cache) const;

    /// Winner selection compares values in place: the version and tie-break columns are never compressed,
    /// so no row payload is ever materialized to pick a winner. A row that ties on both never replaces the
    /// row already stored, whatever its payload is.
    int compareWinner(const Block & block, size_t lhs_row, size_t rhs_row) const;
    int compareWinner(const Block & block, size_t lhs_row, const RowData & rhs) const;

    /// The hash tables index themselves by the low bits, so the shard is chosen from the high ones.
    static size_t shardIndex(size_t hash) { return hash >> 56; }
    size_t rowLockIndex(EntryId entry_id) const;
    /// The oldest snapshot any live reader can still observe. Versions below it are unreachable.
    UInt64 oldestLiveGeneration() const;
    void drainReaders();
    std::optional<EntryId> findEntry(std::string_view key, size_t hash) const;
    std::vector<EntryId> getPostingIds(const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const;
    UInt64 getPostingCardinality(const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const;
    void
    intersectPostingIds(std::vector<EntryId> & entry_ids, const LookupIndexPtr & index, const std::vector<String> & serialized_keys) const;
    void clearData();
    /// Runs under `writer_mutex`. Posting membership cannot disappear while an older snapshot may still
    /// need it to reach the row version preceding a tombstone.
    void pruneLookupTombstones();
    void tryPruneLookupTombstones() const;
    void acquireInsertPreparation(UInt64 bytes);
    void releaseInsertPreparation(UInt64 bytes);

    enum class SmallSegmentCompactionResult : uint8_t
    {
        NoWork,
        Retry,
        Compacted,
    };
    SmallSegmentCompactionResult compactSmallSegments();
    void runSmallSegmentCompaction();
    void scheduleSmallSegmentCompaction();
    void stopSmallSegmentCompaction();

    const String version_column;
    const Names key_columns;
    std::vector<Names> lookup_index_columns;
    const OverwriteCacheSettings settings;
    LoggerPtr log;

    Block sample_block;
    Serializations serializations;
    FormatSettings format_settings;
    DataTypes column_types;
    std::unordered_map<String, size_t> column_positions;
    std::vector<size_t> key_positions;
    DataTypes key_column_types;
    size_t version_position = 0;
    std::vector<size_t> tiebreak_positions;
    /// The version and tie-break columns must stay directly comparable inside a segment.
    std::vector<bool> keep_uncompressed;
    std::vector<std::vector<size_t>> lookup_index_positions;
    std::vector<DataTypes> lookup_index_column_types;

    mutable std::mutex writer_mutex;
    /// Insert callers prepare independently, then enter publication in ticket order. Other operations
    /// still use `writer_mutex` directly because they already hold an exclusive table-level lock.
    mutable std::mutex publication_order_mutex;
    mutable std::condition_variable publication_order_changed;
    UInt64 next_publication_ticket = 0;
    UInt64 serving_publication_ticket = 0;
    mutable std::mutex insert_admission_mutex;
    mutable std::condition_variable insert_admission_changed;
    UInt64 active_insert_preparations = 0;
    UInt64 pending_insert_bytes = 0;
    /// Keeps entry storage alive while post-publication version pruning runs outside `writer_mutex`.
    mutable SharedMutex entry_lifetime_mutex;
    mutable SharedMutex lookup_catalog_mutex;
    std::array<PrimaryShard, primary_shard_count> primary_shards;
    std::vector<LookupIndexPtr> lookup_indexes;
    std::vector<PendingPostingRemoval> pending_posting_removals;
    std::atomic<bool> has_pending_posting_removals = false;
    std::atomic<UInt64> min_pending_posting_removal_generation = std::numeric_limits<UInt64>::max();
    EntryTable entries;
    mutable std::array<SharedMutex, row_lock_count> row_mutexes;
    mutable std::mutex recycled_versions_mutex;
    std::unique_ptr<EntryVersion> recycled_versions;
    size_t recycled_version_count = 0;
    EntryId next_entry_id = 1;

    std::atomic<UInt64> published_generation = 0;
    /// Snapshot generations of the live read guards, so a writer knows which versions it may drop.
    /// A reader touches only the shard selected by its thread, while a writer scans all shards to
    /// compute the reclamation watermark.
    mutable std::array<SnapshotRegistryShard, snapshot_shard_count> snapshot_registry;
    /// Only the paths that release entry storage outright - `TRUNCATE`, `DROP` and `DROP INDEX` -
    /// wait for readers. Publishing a block never does.
    mutable std::atomic<UInt8> active_reader_epoch = 0;
    mutable std::array<std::atomic<UInt64>, 2> active_readers{};

    std::atomic<UInt64> total_size_bytes = 0;
    std::atomic<UInt64> total_size_rows = 0;

    /// Weak references enumerate current row segments without extending their lifetime after every row
    /// has moved away. Only publications and the background compactor touch this under `writer_mutex`.
    std::deque<std::weak_ptr<RowSegment>> row_segments;

    OverwriteCachePersistencePtr persistence;
    BackgroundSchedulePoolTaskHolder small_segment_compaction_task;
    /// Set while the log is being replayed. Segment compaction is left alone during replay, so that the
    /// segments in memory keep mirroring the files one to one and no rewritten segment goes unrecorded.
    bool loading = false;
    /// Files whose every row lost during replay. They are retired once persistence has started.
    std::vector<UInt64> retired_during_load;
};

void registerStorageOverwriteCache(StorageFactory & factory);

}
