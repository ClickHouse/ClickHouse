#pragma once

#include <algorithm>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>
#include <vector>

#include <Interpreters/IJoin.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/RowRefs.h>

#include <Core/Block_fwd.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>

namespace DB
{

class TableJoin;
class ExpressionActions;
class JoinSource;
using Sizes = std::vector<size_t>;

class MatchedRowsStats;

namespace JoinStuff
{
/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags;
}

/// Which flavour of the join maps a join runs on.
///  - `Default` is the smallest map the strictness allows: `HashJoin::MapsOne`, which stores a single
///    right row per key, wherever one row is enough (LEFT ANY/SEMI/ANTI), `HashJoin::MapsAll` otherwise.
///  - `All` forces `HashJoin::MapsAll`, which stores every right row of a key. It is required when there
///    is a mixed inequal condition in the join condition, for example `t1.a = t2.a AND t1.b > t2.b`: we
///    select all matched rows from the map and filter them by `t1.b > t2.b`.
///  - `Set` is `HashJoin::MapsSet`, which stores no right row at all. It is only valid for joins whose
///    result never contains a value taken from a right row, so the map only has to answer whether a key
///    is present. See `HashJoin::canUseSetMaps` for when it is picked.
enum class JoinMapsKind : uint8_t
{
    Default,
    All,
    Set,
};

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
class HashJoinMethods;

struct BuildResult
{
    bool is_inserted = false;
    bool all_values_unique = true;
    size_t new_keys = 0;
};

/// A join whose result never contains a value taken from a right row (see `MapGetter`)
/// does not need the mapped part of a cell. The table only has to answer whether a key is present.
/// Such a join instantiates the maps with `VoidMapped`. Every alias below then selects the set
/// counterpart of the same table, so a cell holds the key alone.
template <typename Mapped>
constexpr bool is_join_set_mapped = std::is_same_v<Mapped, VoidMapped>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using JoinHashMap = std::conditional_t<is_join_set_mapped<Mapped>, HashSet<Key, Hash>, HashMap<Key, Mapped, Hash>>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using JoinHashMapWithSavedHash
    = std::conditional_t<is_join_set_mapped<Mapped>, HashSetWithSavedHash<Key, Hash>, HashMapWithSavedHash<Key, Mapped, Hash>>;

template <typename Key, typename Mapped, size_t size_bits = sizeof(Key) * 8>
using JoinFixedHashMap = std::conditional_t<
    is_join_set_mapped<Mapped>,
    FixedHashSetWithSizeBits<Key, size_bits>,
    FixedHashMapWithSizeBits<Key, Mapped, size_bits>>;

/** Data structure for implementation of hash JOIN.
  * It is a hash table: keys -> rows of joined ("right") table.
  *
  * JOIN-s could be of these types:
  * - ALL × LEFT/INNER/RIGHT/FULL
  * - ANY × LEFT/INNER/RIGHT
  * - SEMI/ANTI x LEFT/RIGHT
  * - ASOF x LEFT/INNER
  *
  * ALL means usual JOIN, when rows are multiplied by number of matching rows from the "right" table.
  * ANY uses one line per unique key from right table. For LEFT JOIN it would be any row (with needed joined key) from the right table,
  * for RIGHT JOIN it would be any row from the left table and for INNER one it would be any row from right and any row from left.
  * SEMI JOIN filter left table by keys that are present in right table for LEFT JOIN, and filter right table by keys from left table
  * for RIGHT JOIN. In other words SEMI JOIN returns only rows which joining keys present in another table.
  * ANTI JOIN is the same as SEMI JOIN but returns rows with joining keys that are NOT present in another table.
  * SEMI/ANTI JOINs allow to get values from both tables. For filter table it gets any row with joining same key. For ANTI JOIN it returns
  * defaults other table columns.
  * ASOF JOIN is not-equi join. For one key column it finds nearest value to join according to join inequality.
  * It's expected that ANY|SEMI LEFT JOIN is more efficient that ALL one.
  *
  * If INNER is specified - leave only rows that have matching rows from "right" table.
  * If LEFT is specified - in case when there is no matching row in "right" table, fill it with default values instead.
  * If RIGHT is specified - first process as INNER, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  * If FULL is specified - first process as LEFT, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  *
  * Thus, LEFT and RIGHT JOINs are not symmetric in terms of implementation.
  *
  * All JOINs are done by equality condition on keys (equijoin).
  * Non-equality and other conditions are not supported.
  *
  * Implementation:
  *
  * 1. Build hash table in memory from "right" table.
  * This hash table is in form of keys -> row in case of ANY or keys -> [rows...] in case of ALL.
  * This is done in insertFromBlock method.
  *
  * 2. Process "left" table and join corresponding rows from "right" table by lookups in the map.
  * This is done in joinBlock methods.
  *
  * In case of ANY LEFT JOIN - form new columns with found values or default values.
  * This is the most simple. Number of rows in left table does not change.
  *
  * In case of ANY INNER JOIN - form new columns with found values,
  *  and also build a filter - in what rows nothing was found.
  * Then filter columns of "left" table.
  *
  * In case of ALL ... JOIN - form new columns with all found rows,
  *  and also fill 'offsets' array, describing how many times we need to replicate values of "left" table.
  * Then replicate columns of "left" table.
  *
  * How Nullable keys are processed:
  *
  * NULLs never join to anything, even to each other.
  * During building of map, we just skip keys with NULL value of any component.
  * During joining, we simply treat rows with any NULLs in key as non joined.
  *
  * Default values for outer joins (LEFT, RIGHT, FULL):
  *
  * Behaviour is controlled by 'join_use_nulls' settings.
  * If it is false, we substitute (global) default value for the data type, for non-joined rows
  *  (zero, empty string, etc. and NULL for Nullable data types).
  * If it is true, we always generate Nullable column and substitute NULLs for non-joined rows,
  *  as in standard SQL.
  */
class HashJoin : public IJoin
{
public:
    HashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block,
        bool any_take_last_row_ = false,
        /// `PartitionedHashJoin` passes false: its `HashJoinTable` has no key-only counterpart.
        bool allow_set_maps_ = true);

    ~HashJoin() override;

    std::string getName() const override { return "HashJoin"; }

    const TableJoin & getTableJoin() const override { return *table_join; }

    /// The left side is streamed through once, each row emitted in input order.
    bool preservesLeftBlockOrder() const override { return true; }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      * The build runs on one thread, so `worker_id` is not read.
      */
    bool addBlockToJoin(const Block & source_block_, size_t num_rows, size_t worker_id, bool check_limits) override;

    void checkTypesOfKeys(const Block & block) const override;

    using IJoin::joinBlock;

    /** Join data from the map (that was previously built by calls to addBlockToJoin) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    JoinResultPtr joinBlock(Block block) override;

    /// Check joinGet arguments and infer the return type.
    DataTypePtr joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const;

    /// Used by joinGet function that turns StorageJoin into a dictionary.
    ColumnWithTypeAndName joinGet(const Block & block, const Block & block_with_columns_to_add) const;

    bool isFilled() const override { return from_storage_join; }

    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    JoinPipelineType pipelineType() const override
    {
        /// No need to process anything in the right stream if hash table was already filled
        if (from_storage_join)
            return JoinPipelineType::FilledRight;

        /// Default pipeline processes right stream at first and then left.
        return JoinPipelineType::FillRightFirst;
    }

    /** For RIGHT and FULL JOINs.
      * A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
      * Use only after all calls to joinBlock was done.
      * left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
      */
    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    void onBuildPhaseFinish() override;

    bool hasPostBuildPhase() const override;
    void runPostBuildPhase() override;

    /// Number of unique keys in all built JOIN maps.
    size_t getTotalRowCount() const final;
    /// Sum size in bytes of all buffers, used for JOIN maps and for the memory pool.
    size_t getTotalByteCount() const final;
    /// Number of right-side rows ingested into the build.
    size_t getRightTableRowCount() const { return getJoinedData()->rows_to_join; }
    /// Peak bytes the build occupied
    size_t getPeakBuildBytes() const { return peak_build_bytes; }

    StepAnalysisReport getAnalysisReport() const override;
    const MatchedRowsStats * getMatchStats() const { return matched_rows_stats.get(); }

    bool alwaysReturnsEmptySet() const final;

    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOFJoinInequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const override { return any_take_last_row; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const;

/// Different types of keys for maps.
#define APPLY_FOR_JOIN_VARIANTS(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string) \
    M(keys32) \
    M(keys64) \
    M(keys128) \
    M(keys256) \
    M(hashed) \
    M(low_cardinality_key_string) \
    M(low_cardinality_key_fixed_string) \
    M(range8_key32) \
    M(range16_key32) \
    M(range17_key32) \
    M(range18_key32) \
    M(range8_key64) \
    M(range16_key64) \
    M(range17_key64) \
    M(range18_key64)

/// Used for reading from StorageJoin and applying joinGet function. The single-LowCardinality-key
/// maps store key values in maps physically identical to their non-LowCardinality counterparts, so
/// they are read back the same way (the output key column is the parent LowCardinality type).
/// The keysN maps hold the key columns packed into one fixed-width blob, so each key column is
/// recovered from its own byte range. `hashed` is absent: its map key is a hash of the values, and
/// the `range*` types are absent because a `StorageJoin`'s join runs no `range*` conversion.
#define APPLY_FOR_JOIN_VARIANTS_LIMITED(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string) \
    M(keys32) \
    M(keys64) \
    M(keys128) \
    M(keys256) \
    M(low_cardinality_key_string) \
    M(low_cardinality_key_fixed_string)

    enum class Type : uint8_t
    {
        #define M(NAME) NAME,
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
    };

    /// True for the single-LowCardinality-column maps, whose key getter consumes the live
    /// ColumnLowCardinality (so the key column must not be materialized for them).
    static bool isLowCardinalityType(Type type)
    {
        switch (type)
        {
            case Type::low_cardinality_key_string:
            case Type::low_cardinality_key_fixed_string:
                return true;
            default:
                return false;
        }
    }

    /** Different data structures, that are used to perform JOIN.
      */
    template <typename Mapped>
    struct MapsTemplate
    {
        /// NOLINTBEGIN(bugprone-macro-parentheses)
        using MappedType = Mapped;
        static constexpr bool has_mapped = !is_join_set_mapped<Mapped>;
        std::shared_ptr<JoinFixedHashMap<UInt8, Mapped>> key8;
        std::shared_ptr<JoinFixedHashMap<UInt16, Mapped>> key16;
        std::shared_ptr<JoinHashMap<UInt32, Mapped, HashCRC32<UInt32>>> key32;
        std::shared_ptr<JoinHashMap<UInt64, Mapped, HashCRC32<UInt64>>> key64;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>> key_string;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>> key_fixed_string;
        std::shared_ptr<JoinHashMap<UInt32, Mapped, HashCRC32<UInt32>>> keys32;
        std::shared_ptr<JoinHashMap<UInt64, Mapped, HashCRC32<UInt64>>> keys64;
        std::shared_ptr<JoinHashMap<UInt128, Mapped, UInt128HashCRC32>> keys128;
        std::shared_ptr<JoinHashMap<UInt256, Mapped, UInt256HashCRC32>> keys256;
        std::shared_ptr<JoinHashMap<UInt128, Mapped, UInt128TrivialHash>> hashed;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>> low_cardinality_key_string;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>> low_cardinality_key_fixed_string;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 8>> range8_key32;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 16>> range16_key32;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 17>> range17_key32;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 18>> range18_key32;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 8>> range8_key64;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 16>> range16_key64;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 17>> range17_key64;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 18>> range18_key64;

        void create(Type which)
        {
            switch (which)
            {
#define M(NAME) \
    case Type::NAME: { \
        using Table = typename decltype(NAME)::element_type; \
        NAME = std::make_shared<Table>(); \
        break; \
    }

                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        }

        size_t getTotalRowCount(Type which) const
        {
            switch (which)
            {
            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->size() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }
        }

        size_t getTotalByteCountImpl(Type which) const
        {
            switch (which)
            {
            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->getBufferSizeInBytes() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }
        }

        size_t getBufferSizeInCells(Type which) const
        {
            switch (which)
            {
            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->getBufferSizeInCells() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }
        }

        /// NOLINTEND(bugprone-macro-parentheses)
    };

    using MapsOne = MapsTemplate<RowRef>;
    using MapsAll = MapsTemplate<RowRefList>;
    using MapsAsof = MapsTemplate<AsofRowRefs>;
    using MapsSet = MapsTemplate<VoidMapped>;

    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof, MapsSet>;

    struct NullMapHolder
    {
        const StoredBlock * columns{};
        ColumnPtr column;
        size_t selector_rows = 0;

        NullMapHolder() = default;
        explicit NullMapHolder(const StoredBlock * columns_, ColumnPtr column_)
            : columns(columns_), column(column_)
        {
            // we can cache the selector size at construction to make the holder robust
            // even if columns are moved/cleared later
            selector_rows = columns ? columns->selector.size() : (this->column ? this->column->size() : 0);
        }

        size_t allocatedBytes() const;
    };

    using NullmapList = std::deque<NullMapHolder>;
    using StoredBlocksList = std::list<StoredBlock>;

    enum class RowStoreState : uint8_t
    {
        Disabled,
        Enabled,
        Initialized,
    };

    struct RightTableData
    {
        Type type = Type::hashed;

        /// tab1 join tab2 on t1.x = t2.x or t1.y = t2.y
        /// =>
        /// tab1 join tab2 on t1.x = t2.x
        /// join tab2 on [not_joined(t1.x = t2.x)] and t1.y = t2.y
        std::vector<MapsVariant> maps;
        Block sample_block; /// Block as it would appear in the BlockList
        /// Track index of "right" table columns in columns list or row store.
        ColumnAccessIndexes column_access_indexes;

        StoredBlocksList columns; /// Columns of "right" table.
        NullmapList nullmaps; /// Nullmaps for blocks of "right" table (if needed)

        StoredColumnsIndexPtr stored_columns_index = std::make_shared<StoredColumnsIndex>();

        /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
        Arena pool;

        /// Atomics because `PartitionedHashJoin` stores its blocks here from several threads.
        std::atomic<size_t> allocated_size = 0;
        std::atomic<size_t> nullmaps_allocated_size = 0;

        /// Number of rows of right table to join
        std::atomic<size_t> rows_to_join = 0;
        /// Number of keys of right table to join
        std::atomic<size_t> keys_to_join = 0;
        /// The maps and the arena; recomputed after every insert and after a post-build step swaps maps.
        std::atomic<size_t> maps_bytes = 0;

        /// Exact `allocated_size + nullmaps_allocated_size + maps_bytes`. The three parts are
        /// independent atomics. A concurrent sum can miss one update and under-count
        /// `max_bytes_in_join`. Size-limit checks and `peak_build_bytes` read only this.
        std::atomic<size_t> total_bytes = 0;

        /// Add `total_bytes` first so a concurrent size-limit check cannot under-count.
        void addBytes(std::atomic<size_t> & part, size_t n)
        {
            total_bytes.fetch_add(n, std::memory_order_relaxed);
            part.fetch_add(n, std::memory_order_relaxed);
        }

        void subBytes(std::atomic<size_t> & part, size_t n)
        {
            part.fetch_sub(n, std::memory_order_relaxed);
            total_bytes.fetch_sub(n, std::memory_order_relaxed);
        }

        void setBytes(std::atomic<size_t> & part, size_t n)
        {
            const size_t old = part.exchange(n, std::memory_order_relaxed);
            if (n >= old)
                total_bytes.fetch_add(n - old, std::memory_order_relaxed);
            else
                total_bytes.fetch_sub(old - n, std::memory_order_relaxed);
        }

        /// Whether the right table reranged by key
        bool sorted = false;
        /// Whether row-major storage is used or not and its layout if it is.
        RowStoreState row_store_state = RowStoreState::Enabled;
        RowDataStore::RowLayoutPtr row_store_layout;

        /// For range types: the minimum key value and the range size from min_key to max_key.
        struct KeyRange
        {
            UInt64 min_key = 0;
            UInt64 size = 0;
        };

        KeyRange key_range;

        size_t avgPerKeyRows() const
        {
            const size_t keys = keys_to_join.load(std::memory_order_relaxed);
            if (keys == 0)
                return 0;
            return rows_to_join.load(std::memory_order_relaxed) / keys;
        }

        bool hasStoredColumns() const { return !columns.empty(); }
    };

    /// For INNER/LEFT ALL JOINs, if the right side has no duplicates inside the join key columns,
    /// we can switch from ALL to RightAny strictness for better performance. Only ever goes from
    /// true to false, so a relaxed store needs no further ordering.
    std::atomic<bool> all_values_unique = true;
    bool all_join_was_promoted_to_right_any = false;

    using RightTableDataPtr = std::shared_ptr<RightTableData>;

    /// We keep correspondence between used_flags and hash table internal buffer.
    /// Hash table cannot be modified during HashJoin lifetime and must be protected with lock.
    void setLock(TableLockHolder rwlock_holder)
    {
        storage_join_lock = rwlock_holder;
    }

    void reuseJoinedData(const HashJoin & join);

    RightTableDataPtr getJoinedData() const { return data; }
    BlocksList releaseJoinedBlocks(bool restructure);
    /// One saved right block back in the structure of the right input, for an algorithm that takes
    /// the blocks over: the columns of `right_sample_block` by name, their nullability restored.
    static Block restoreRightBlock(const Block & saved_block, const Block & right_sample_block);

    /// Rebuilds one stored block's columns in saved-block order. The row store is scattered back into
    /// columns. The selector is applied to both parts. The access indexes put every column back at its
    /// saved position. Consumes the row store.
    static Columns materializeStoredBlock(StoredBlock & stored_block, const ColumnAccessIndexes & access_indexes);

    /// Modify right block (update structure according to sample block) to save it in block list
    static Block prepareRightBlock(const Block & block, const Block & saved_block_sample_);
    Block prepareRightBlock(const Block & block) const;

    const Block & savedBlockSample() const { return data->sample_block; }

    bool isUsed(size_t off) const;
    bool isUsed(UInt32 block_no, size_t row_idx) const;

    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize = false);

    void materializeColumnsFromLeftBlock(Block & block) const;
    Block materializeColumnsFromRightBlock(Block block) const;

    /// Creates a row store based on the already initialized layout and fills from block columns.
    RowDataStorePtr createRowStoreForBlock(const Block & block) const;
    /// Packs a prepared right block (`prepareRightBlock`) into its stored form. When the row store is
    /// initialized, the columns its layout admits go into a `RowDataStore` and the rest stay columnar.
    /// Otherwise every column stays columnar. A caller that already built this block's row store passes it in.
    StoredBlock createStoredBlock(
        const Block & block_to_save, ScatteredBlock::Selector selector, RowDataStorePtr row_store = nullptr) const;

    const std::vector<Sizes> & getKeySizes() const { return key_sizes; }

    bool enableSoftwarePrefetch() const { return enable_prefetch; }

    void setEnableLazyColumnsIndexing(bool value) override { enable_lazy_columns_indexing = value; }

    static bool isUsedByAnotherAlgorithm(const TableJoin & table_join);
    static bool canRemoveColumnsFromLeftBlock(const TableJoin & table_join);

private:
    friend class NotJoinedHash;
    friend class JoinSource;
    /// Uses a `HashJoin` as its schema delegate and row-store owner while building and probing its
    /// own partitioned maps. It needs the access the join methods have.
    friend class PartitionedHashJoin;
    friend class HashJoinClause;

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
    friend class HashJoinMethods;

    bool addBlockToJoin(const Block & block, ScatteredBlock::Selector selector, bool check_limits, RowDataStorePtr row_store = nullptr);

    std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;

    /// This join was created from StorageJoin and it is already filled.
    bool from_storage_join = false;

    const bool any_take_last_row; /// Overwrite existing values when encountering the same key again

    std::optional<TypeIndex> asof_type;
    const ASOFJoinInequality asof_inequality;

    mutable std::mutex totals_mutex;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// so we must guarantee constantness of hash table during HashJoin lifetime (using method setLock)
    mutable std::shared_ptr<JoinStuff::JoinUsedFlags> used_flags;

    std::unique_ptr<MatchedRowsStats> matched_rows_stats;
    RightTableDataPtr data;

    std::vector<Sizes> key_sizes;

    /// Block with columns from the right-side table.
    Block right_sample_block;
    /// Block with columns from the right-side table except key columns.
    Block sample_block_with_columns_to_add;
    /// Block with key columns in the same order they appear in the right-side table (duplicates appear once).
    Block right_table_keys;
    /// Block with key columns right-side table keys that are needed in result (would be attached after joined columns).
    Block required_right_keys;
    /// Left table column names that are sources for required_right_keys columns
    std::vector<String> required_right_keys_sources;

    std::vector<std::pair<size_t, size_t>> additional_filter_required_rhs_pos;

    /// Maximum number of rows in result block. If it is 0, then no limits.
    size_t max_joined_block_rows = 0;
    size_t max_joined_block_bytes = 0;
    bool joined_block_split_single_row = false;
    bool enable_lazy_columns_replication = false;
    bool enable_lazy_columns_indexing = false;
    bool enable_prefetch = true;

    /// When tracked memory consumption is more than a threshold, we will shrink to fit stored blocks.
    bool shrink_blocks = false;
    Int64 memory_usage_before_adding_blocks = 0;

    /// Peak of bytes observed during the build.
    size_t peak_build_bytes = 0;

    void updatePeakBuildBytes(size_t bytes) { peak_build_bytes = std::max(peak_build_bytes, bytes); }

    /// Whether the maps store keys alone, see `JoinMapsKind::Set`. Decided once, before they are created.
    bool use_set_maps = false;
    /// False when the owner cannot consume key-only maps, whatever `canUseSetMaps` would otherwise say.
    const bool allow_set_maps = true;

    LoggerPtr log;

    /// Should be set via setLock to protect hash table from modification from StorageJoin
    /// If set HashJoin instance is not available for modification (addBlockToJoin)
    TableLockHolder storage_join_lock = nullptr;

    /// Unchecked as in without `doDebugAsserts`. That walk cannot run while `PartitionedHashJoin`'s threads append.
    size_t getTotalByteCountUnchecked() const;

    void recomputeMapsBytes();

    void dataMapInit(MapsVariant & map);

    void initRightBlockStructure(Block & saved_block_sample);

    JoinResultPtr runJoinDispatch(ScatteredBlock block);

    bool preferUseMapsAll() const;

    bool canUseSetMaps() const;

    /// The maps flavour this join runs on. All the dispatch entry points take it.
    JoinMapsKind getMapsKind() const;

    bool isUsedByAnotherAlgorithm() const;
    bool canRemoveColumnsFromLeftBlock() const;

    void validateAdditionalFilterExpression(std::shared_ptr<ExpressionActions> additional_filter_expression);
    bool needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const;

    bool isRightTableRerangeEnabled() const;
    bool rightTableCanBeReranged() const;
    void tryRerangeRightTableData();

    template <JoinKind KIND, typename Map, JoinStrictness STRICTNESS> // NOLINT(readability-identifier-naming)
    void tryRerangeRightTableDataImpl(Map & map);

    bool isRowStoreSupported() const;

    /// Layout is from the sample block, before any fill thread. `may_rerange` is false for a caller
    /// that never reorders the stored rows. For such a caller the row store need not yield to the
    /// rerange optimization.
    void initRowStore(const Block & block, bool may_rerange = true);

    void reinitUsedFlags();

    bool hasNonJoinedRows() const;
    bool recordsRowRefsForStats() const;

    void doDebugAsserts() const;
};
}
