#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>
#include <vector>

#include <Interpreters/HashTablesStatistics.h>
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
#include <Common/HashTable/BucketPartitionedTable.h>
#include <Common/CacheLine.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/PartitionedFixedHashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>

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

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
class HashJoinMethods;

/// Zero bits is one bucket, where routing folds away and the map behaves as single-level.
constexpr Int32 BITS_FOR_BUCKET_SERIAL = 0;
constexpr Int32 BITS_FOR_BUCKET_TWO_LEVEL = DEFAULT_BITS_FOR_BUCKET;

constexpr size_t NUM_HASH_TABLE_BUCKETS = 1ull << BITS_FOR_BUCKET_TWO_LEVEL;

/// Power of two, so `slotForBucket` can mask; never more slots than there are buckets.
size_t slotCountForThreads(size_t max_threads);

inline size_t slotForBucket(size_t bucket, size_t num_slots)
{
    return bucket & (num_slots - 1);
}

/// Guards every clause's buckets that map to one slot, and that slot's arena.
struct alignas(DB::CH_CACHE_LINE_SIZE) BucketLock
{
    std::mutex mutex;
};

struct BuildResult
{
    bool is_inserted = false;
    bool all_values_unique = true;
    size_t new_keys = 0;
};

/// The two-level grower starts a one-bucket map too small: two extra rehashes on a full-size
/// map, 35-44% slower `FillingRightJoinSide` at 500k keys. It is the right choice at 256.
template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using JoinHashMap
    = TwoLevelHashMap<Key, Mapped, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator, HashMapTable, BITS_FOR_BUCKET_SERIAL>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using JoinHashMapWithSavedHash = TwoLevelHashMapWithSavedHash<
    Key,
    Mapped,
    Hash,
    HashTableGrowerWithPrecalculation<>,
    HashTableAllocator,
    HashMapTable,
    BITS_FOR_BUCKET_SERIAL>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using TwoLevelJoinHashMap
    = TwoLevelHashMap<Key, Mapped, Hash, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, BITS_FOR_BUCKET_TWO_LEVEL>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
using TwoLevelJoinHashMapWithSavedHash = TwoLevelHashMapWithSavedHash<
    Key,
    Mapped,
    Hash,
    TwoLevelHashTableGrower<>,
    HashTableAllocator,
    HashMapTable,
    BITS_FOR_BUCKET_TWO_LEVEL>;

template <typename Key, typename Mapped, size_t size_bits = sizeof(Key) * 8>
using JoinFixedHashMap = PartitionedFixedHashMap<Key, Mapped, size_bits, BITS_FOR_BUCKET_SERIAL>;

template <typename Key, typename Mapped, size_t size_bits = sizeof(Key) * 8>
using TwoLevelJoinFixedHashMap = PartitionedFixedHashMap<Key, Mapped, size_bits, BITS_FOR_BUCKET_TWO_LEVEL>;

static_assert(BucketPartitionedMap<JoinHashMap<UInt64, RowRefList>>);
static_assert(BucketPartitionedMap<JoinHashMapWithSavedHash<std::string_view, RowRefList>>);
static_assert(BucketPartitionedMap<TwoLevelJoinHashMap<UInt64, RowRefList>>);
static_assert(BucketPartitionedMap<TwoLevelJoinHashMapWithSavedHash<std::string_view, RowRefList>>);
static_assert(BucketPartitionedMap<JoinFixedHashMap<UInt8, RowRefList>>);
static_assert(BucketPartitionedMap<JoinFixedHashMap<UInt64, RowRefList, 18>>);
static_assert(BucketPartitionedMap<TwoLevelJoinFixedHashMap<UInt8, RowRefList>>);
static_assert(BucketPartitionedMap<TwoLevelJoinFixedHashMap<UInt64, RowRefList, 18>>);

static_assert(JoinHashMap<UInt64, RowRefList>::numBuckets() == 1);
static_assert(TwoLevelJoinHashMap<UInt64, RowRefList>::numBuckets() == NUM_HASH_TABLE_BUCKETS);
static_assert(JoinFixedHashMap<UInt8, RowRefList>::numBuckets() == 1);
static_assert(TwoLevelJoinFixedHashMap<UInt8, RowRefList>::numBuckets() == NUM_HASH_TABLE_BUCKETS);

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
        size_t reserve_num_ = 0,
        const String & instance_id_ = "",
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        size_t max_threads_ = 1,
        bool use_parallel_layout_ = true);

    ~HashJoin() override;

    std::string getName() const override { return "HashJoin"; }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return getTotals().empty() && getTotalRowCount() == 0;
    }

    std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        /// Pipeline copies keep this join's layout. A side-swap has to go through
        /// `cloneWithParallelLayout` with a layout recomputed from the new right-side estimate.
        return std::make_shared<HashJoin>(
            table_join_,
            right_sample_block_,
            any_take_last_row,
            reserve_num,
            instance_id,
            HashJoinStatsCollectingParams{},
            max_threads,
            use_parallel_layout);
    }

    /// Same as `clone`, but the caller has already recomputed the layout (side swap).
    std::shared_ptr<IJoin> cloneWithParallelLayout(
        const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_,
        bool use_parallel_layout_) const
    {
        return std::make_shared<HashJoin>(
            table_join_,
            right_sample_block_,
            any_take_last_row,
            reserve_num,
            instance_id,
            HashJoinStatsCollectingParams{},
            max_threads,
            use_parallel_layout_);
    }

    /// `joinPipelinesByShards` clones one join per PK layer and never installs
    /// `NonJoinedBlocksTransform`. A clone that still reports parallel non-joined
    /// processing would skip unmatched right rows of a RIGHT/FULL join.
    std::shared_ptr<IJoin> cloneNoParallel(
        const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        return std::make_shared<HashJoin>(
            table_join_,
            right_sample_block_,
            any_take_last_row,
            reserve_num,
            instance_id,
            HashJoinStatsCollectingParams{},
            /*max_threads=*/1,
            /*use_parallel_layout=*/false);
    }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool addBlockToJoin(const Block & source_block_, size_t num_rows, size_t worker_id, bool check_limits) override;

    void checkTypesOfKeys(const Block & block) const override;

    using IJoin::joinBlock;

    /** Join data from the map (that was previously built by calls to addBlockToJoin) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    JoinResultPtr joinBlock(Block block) override;
    JoinResultPtr joinScatteredBlock(ScatteredBlock block);

    /// Check joinGet arguments and infer the return type.
    DataTypePtr joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const;

    /// Used by joinGet function that turns StorageJoin into a dictionary.
    ColumnWithTypeAndName joinGet(const Block & block, const Block & block_with_columns_to_add) const;

    bool isFilled() const override { return from_storage_join; }

    /// Only the parallel layout has the slots that make concurrent fill safe, and keeping `hash`'s
    /// single lane is what keeps a serial-layout join's output row order reproducible.
    bool supportParallelJoin() const override { return use_parallel_layout && max_threads > 1; }
    size_t getMaxBuildThreads() const override { return max_threads; }

    /// False when `num_slots == 1` (including `use_parallel_layout && max_threads == 1`).
    bool supportParallelNonJoinedBlocksProcessing() const override;
    /// `FilledJoinStep`, which probes a StorageJoin, has no `NonJoinedBlocksTransform` to run.
    bool isParallelNonJoinedProcessingEnabled() const override
    {
        return !from_storage_join && supportParallelNonJoinedBlocksProcessing();
    }

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

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block,
        const Block & result_sample_block,
        UInt64 max_block_size,
        size_t stream_idx,
        size_t num_streams) const override;

    void onBuildPhaseFinish() override;
    void onProbePhaseFinish(size_t matched_right_rows) override
    {
        hash_table_matches = matched_right_rows;
        probe_phase_finished = true;
    }

    bool hasPostBuildPhase() const override;
    void runPostBuildPhase() override;

    /// Number of unique keys in all built JOIN maps.
    size_t getTotalRowCount() const final;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
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

#define APPLY_FOR_SINGLE_LEVEL_JOIN_VARIANTS(M) \
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
    M(low_cardinality_key_fixed_string)

#define APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M) \
    M(two_level_key8) \
    M(two_level_key16) \
    M(two_level_key32) \
    M(two_level_key64) \
    M(two_level_key_string) \
    M(two_level_key_fixed_string) \
    M(two_level_keys32) \
    M(two_level_keys64) \
    M(two_level_keys128) \
    M(two_level_keys256) \
    M(two_level_hashed) \
    M(two_level_low_cardinality_key_string) \
    M(two_level_low_cardinality_key_fixed_string)

#define APPLY_FOR_FIXED_JOIN_VARIANTS(M) \
    M(key8) \
    M(key16) \
    M(range8_key32) \
    M(range16_key32) \
    M(range17_key32) \
    M(range18_key32) \
    M(range8_key64) \
    M(range16_key64) \
    M(range17_key64) \
    M(range18_key64)

/// Different types of keys for maps.
#define APPLY_FOR_JOIN_VARIANTS(M) \
    APPLY_FOR_FIXED_JOIN_VARIANTS(M) \
    APPLY_FOR_SINGLE_LEVEL_JOIN_VARIANTS(M) \
    APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M)

/// Used for reading from StorageJoin and applying joinGet function. The single-LowCardinality-key
/// maps store key values in maps physically identical to their non-LowCardinality counterparts, so
/// they are read back the same way (the output key column is the parent LowCardinality type).
/// The keysN maps hold the key columns packed into one fixed-width blob, so each key column is
/// recovered from its own byte range. `hashed` is absent: its map key is a hash of the values.
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
    M(low_cardinality_key_fixed_string) \
    M(two_level_key32) \
    M(two_level_key64) \
    M(two_level_key_string) \
    M(two_level_key_fixed_string) \
    M(two_level_keys32) \
    M(two_level_keys64) \
    M(two_level_keys128) \
    M(two_level_keys256) \
    M(two_level_low_cardinality_key_string) \
    M(two_level_low_cardinality_key_fixed_string)

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
            case Type::two_level_low_cardinality_key_string:
            case Type::two_level_low_cardinality_key_fixed_string: return true;
            default:
                return false;
        }
    }

    static bool isTwoLevelType(Type type)
    {
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: return true;
            APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M)
#undef M
            default:
                return false;
        }
    }

    static Type toTwoLevelType(Type type)
    {
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: return Type::two_level_##NAME;
            APPLY_FOR_SINGLE_LEVEL_JOIN_VARIANTS(M)
#undef M
            case Type::key8: return Type::two_level_key8;
            case Type::key16: return Type::two_level_key16;
            default: return type;
        }
    }

    static const char * typeName(Type type)
    {
        switch (type)
        {
#define M(NAME) \
    case Type::NAME: return #NAME;
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
        }
        return "";
    }

    /** Different data structures, that are used to perform JOIN.
      */
    template <typename Mapped>
    struct MapsTemplate
    {
        /// NOLINTBEGIN(bugprone-macro-parentheses)
        using MappedType = Mapped;
        std::shared_ptr<JoinFixedHashMap<UInt8, Mapped>> key8;
        std::shared_ptr<JoinFixedHashMap<UInt16, Mapped>> key16;
        std::shared_ptr<TwoLevelJoinFixedHashMap<UInt8, Mapped>> two_level_key8;
        std::shared_ptr<TwoLevelJoinFixedHashMap<UInt16, Mapped>> two_level_key16;
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
        std::shared_ptr<TwoLevelJoinHashMap<UInt32, Mapped, HashCRC32<UInt32>>> two_level_key32;
        std::shared_ptr<TwoLevelJoinHashMap<UInt64, Mapped, HashCRC32<UInt64>>> two_level_key64;
        std::shared_ptr<TwoLevelJoinHashMapWithSavedHash<std::string_view, Mapped>> two_level_key_string;
        std::shared_ptr<TwoLevelJoinHashMapWithSavedHash<std::string_view, Mapped>> two_level_key_fixed_string;
        std::shared_ptr<TwoLevelJoinHashMap<UInt32, Mapped, HashCRC32<UInt32>>> two_level_keys32;
        std::shared_ptr<TwoLevelJoinHashMap<UInt64, Mapped, HashCRC32<UInt64>>> two_level_keys64;
        std::shared_ptr<TwoLevelJoinHashMap<UInt128, Mapped, UInt128HashCRC32>> two_level_keys128;
        std::shared_ptr<TwoLevelJoinHashMap<UInt256, Mapped, UInt256HashCRC32>> two_level_keys256;
        std::shared_ptr<TwoLevelJoinHashMap<UInt128, Mapped, UInt128TrivialHash>> two_level_hashed;
        std::shared_ptr<TwoLevelJoinHashMapWithSavedHash<std::string_view, Mapped>> two_level_low_cardinality_key_string;
        std::shared_ptr<TwoLevelJoinHashMapWithSavedHash<std::string_view, Mapped>> two_level_low_cardinality_key_fixed_string;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 8>> range8_key32;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 16>> range16_key32;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 17>> range17_key32;
        std::shared_ptr<JoinFixedHashMap<UInt32, Mapped, 18>> range18_key32;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 8>> range8_key64;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 16>> range16_key64;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 17>> range17_key64;
        std::shared_ptr<JoinFixedHashMap<UInt64, Mapped, 18>> range18_key64;

#define M(NAME) static_assert(BucketPartitionedMap<typename decltype(NAME)::element_type>);
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        /// A statistics-derived reserve is an estimate, so cap it at the spill budget.
        template <typename Table>
        static size_t clampReserve(size_t reserve, size_t max_reserve_bytes)
        {
            if (!max_reserve_bytes)
                return reserve;
            if constexpr (requires { sizeof(typename Table::cell_type); })
            {
                return std::min(reserve, max_reserve_bytes / (8 * sizeof(typename Table::cell_type)));
            }
            else
            {
                return reserve;
            }
        }

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

        size_t reserveSlot(Type which, size_t slot, size_t slots, size_t reserve, size_t max_reserve_bytes)
        {
            switch (which)
            {
#define M(NAME) \
    case Type::NAME: { \
        using Table = typename decltype(NAME)::element_type; \
        if constexpr (Table::isFixedRangeStorage()) \
        { \
            return 0; \
        } \
        else \
        { \
            const size_t clamped = clampReserve<Table>(reserve, max_reserve_bytes); \
            for (size_t bucket = slot; bucket < Table::numBuckets(); bucket += slots) \
                NAME->impls[bucket].reserve(clamped / Table::numBuckets()); \
            return clamped / slots; \
        } \
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

        size_t getBucketCount(Type which) const
        {
            switch (which)
            {
#define M(NAME) \
    case Type::NAME: return decltype(NAME)::element_type::numBuckets();
                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        }

        size_t getBucketBufferSizeInBytes(Type which, size_t bucket) const
        {
            switch (which)
            {
#define M(NAME) \
    case Type::NAME: { \
        using Table = typename decltype(NAME)::element_type; \
        if constexpr (Table::isFixedRangeStorage()) \
            return (NAME && bucket == 0) ? NAME->getBufferSizeInBytes() : 0; \
        else \
            return NAME ? NAME->impls[bucket].getBufferSizeInBytes() : 0; \
    }
                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        }

        void computeBucketPrefix(Type which) const
        {
            switch (which)
            {
#define M(NAME) \
    case Type::NAME: \
        if (NAME) \
            NAME->computeBucketPrefix(); \
        break;
                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        }

        void restoreMinMaxOptimization(Type which)
        {
            switch (which)
            {
#define M(NAME) \
    case Type::NAME: \
        if (NAME) \
            NAME->restoreMinMaxOptimization(); \
        break;
                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        }
        /// NOLINTEND(bugprone-macro-parentheses)
    };

    using MapsOne = MapsTemplate<RowRef>;
    using MapsAll = MapsTemplate<RowRefList>;
    using MapsAsof = MapsTemplate<AsofRowRefs>;

    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof>;

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

    /// Owned by exactly one build thread, which is why these lists need no mutex; the maps are
    /// shared and go through `bucket_locks`.
    struct WorkerStoredData
    {
        StoredBlocksList columns;
        NullmapList nullmaps;
        bool shrink_done = false;
    };

    struct RightTableData
    {
        explicit RightTableData(size_t slots, size_t num_workers)
            : num_slots(slots)
            , workers(std::max<size_t>(1, num_workers))
        {
            pools.reserve(slots);
            for (size_t i = 0; i < slots; ++i)
                pools.push_back(std::make_unique<Arena>());
        }

        /// Belongs to the maps, not the join: `StorageJoin` shares maps with joins built for a
        /// different thread count.
        const size_t num_slots;

        Type type = Type::hashed;

        /// tab1 join tab2 on t1.x = t2.x or t1.y = t2.y
        /// =>
        /// tab1 join tab2 on t1.x = t2.x
        /// join tab2 on [not_joined(t1.x = t2.x)] and t1.y = t2.y
        std::vector<MapsVariant> maps;
        Block sample_block; /// Block as it would appear in the BlockList
        /// Track index of "right" table columns in columns list or row store.
        ColumnAccessIndexes column_access_indexes;

        std::vector<WorkerStoredData> workers;

        StoredColumnsIndexPtr stored_columns_index = std::make_shared<StoredColumnsIndex>();

        /// Additional data - strings for string keys and continuation elements of single-linked
        /// lists of references to rows. One per slot, because `Arena` is unsynchronized; splitting
        /// is sound because neither allocation kind needs contiguity or rollback.
        std::vector<std::unique_ptr<Arena>> pools;

        Arena & poolForBucket(size_t bucket) { return *pools[slotForBucket(bucket, num_slots)]; }

        size_t poolsAllocatedBytes() const
        {
            size_t res = 0;
            for (const auto & pool : pools)
                res += pool->allocatedBytes();
            return res;
        }

        std::atomic<size_t> allocated_size = 0;
        std::atomic<size_t> nullmaps_allocated_size = 0;

        /// Number of rows of right table to join
        std::atomic<size_t> rows_to_join = 0;
        /// Running totals: summing the buckets would read what other build threads are mutating.
        /// `bucket_bytes` accumulates deltas, so a post-build step that swaps maps must recompute it.
        std::atomic<size_t> keys_to_join = 0;
        std::atomic<size_t> bucket_bytes = 0;

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

        bool hasStoredColumns() const
        {
            for (const auto & worker : workers)
            {
                if (!worker.columns.empty())
                    return true;
            }
            return false;
        }
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
    size_t getNumReleaseChunks() const;
    BlocksList releaseJoinedBlocksChunk(size_t chunk_idx);
    void releaseJoinMaps();

    /// Modify right block (update structure according to sample block) to save it in block list
    static Block prepareRightBlock(const Block & block, const Block & saved_block_sample_);
    Block prepareRightBlock(const Block & block) const;

    const Block & savedBlockSample() const { return data->sample_block; }

    bool isUsed(size_t off) const;
    bool isUsed(UInt32 block_no, size_t row_idx) const;

    void debugKeys() const;

    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join, size_t worker_id, bool force_optimize = false);

    void setMaxJoinedBlockRows(size_t value) { max_joined_block_rows = value; }
    void setMaxJoinedBlockBytes(size_t value) { max_joined_block_bytes = value; }

    void materializeColumnsFromLeftBlock(Block & block) const;
    Block materializeColumnsFromRightBlock(Block block) const;

    struct RowStoreLayoutWithAccessIndexes
    {
        RowDataStore::RowLayoutPtr layout;
        ColumnAccessIndexes access_indexes;
    };

    /// Derives the row store layout from the first right block.
    std::optional<RowStoreLayoutWithAccessIndexes> initRowStore(const Block & block);
    /// Takes a pre-computed row store layout.
    void initRowStore(const std::optional<RowStoreLayoutWithAccessIndexes> & layout_with_access_indexes);
    /// Creates a row store based on the already initialized layout and fills from block columns.
    RowDataStorePtr createRowStoreForBlock(const Block & block) const;

    size_t getAndSetRightTableKeys() const;

    const std::vector<Sizes> & getKeySizes() const { return key_sizes; }

    bool enableLazyColumnsReplication() const { return enable_lazy_columns_replication; }
    bool enableSoftwarePrefetch() const { return enable_prefetch; }

    void setEnableLazyColumnsIndexing(bool value) override { enable_lazy_columns_indexing = value; }

    static bool isUsedByAnotherAlgorithm(const TableJoin & table_join);
    static bool canRemoveColumnsFromLeftBlock(const TableJoin & table_join);

private:
    friend class NotJoinedHash;
    friend class JoinSource;

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
    friend class HashJoinMethods;

    bool addBlockToJoin(const Block & block, ScatteredBlock::Selector selector, size_t worker_id, bool check_limits, RowDataStorePtr row_store = nullptr);

    std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;

    /// This join was created from StorageJoin and it is already filled.
    bool from_storage_join = false;

    const bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    const size_t reserve_num;
    const String instance_id;

    const size_t max_threads;
    const bool use_parallel_layout;
    const size_t num_slots;

    std::optional<TypeIndex> asof_type;
    const ASOFJoinInequality asof_inequality;

    /// Taken only around map inserts; the stored-block lists need no lock (see `WorkerStoredData`).
    mutable std::vector<BucketLock> bucket_locks;

    /// Reserving up front would serialize every bucket before the build, so each slot reserves its
    /// own share on first use, under that slot's lock. Outer vector is per clause.
    size_t map_size_hint = 0;
    size_t map_reserve_bytes_cap = 0;
    std::vector<std::vector<char>> slot_space_reserved;

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

    /// Answering costs a scan of the used flags, and every parallel non-joined stream asks.
    mutable std::atomic<bool> has_non_joined_rows_checked = false;
    mutable std::atomic<bool> has_non_joined_rows = false;

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

    /// When tracked memory consumption is more than a threshold, we will shrink to fit stored
    /// blocks. Reading a stale `false` only delays the shrink by one block, so relaxed is enough.
    std::atomic<bool> shrink_blocks = false;
    std::atomic<Int64> memory_usage_before_adding_blocks = 0;

    /// Peak of bytes observed in the hash table during the build phase
    size_t peak_build_bytes = 0;

    /// Track if conversion to fixed hash map was already attempted to prevent repeated checks.
    bool conversion_to_fixed_hash_map_attempted = false;

    /// Track if shared runtime filters were already published to keep publication one-shot.
    bool shared_runtime_filters_publish_attempted = false;

    const HashJoinStatsCollectingParams stats_collecting_params;
    bool build_phase_finished = false;
    bool probe_phase_finished = false;

    /// Rows emitted from hash-table matches across all probe threads (excludes default/miss rows).
    size_t hash_table_matches = 0;

    /// Identifier to distinguish different HashJoin instances in logs
    /// Several instances can be created, for example, in GraceHashJoin to handle different buckets
    String instance_log_id;

    LoggerPtr log;

    /// Should be set via setLock to protect hash table from modification from StorageJoin
    /// If set HashJoin instance is not available for modification (addBlockToJoin)
    TableLockHolder storage_join_lock = nullptr;

    /// Unchecked as in without `doDebugAsserts`, which cannot run while build threads append.
    size_t getTotalByteCountUnchecked() const;

    void recomputeBucketBytes();

    void dataMapInit(MapsVariant & map);

    size_t sizeHintForMaps() const;

    void initRightBlockStructure(Block & saved_block_sample);

    JoinResultPtr runJoinDispatch(ScatteredBlock block);

    bool preferUseMapsAll() const;

    bool isUsedByAnotherAlgorithm() const;
    bool canRemoveColumnsFromLeftBlock() const;

    void shrinkWorkerStoredBlocks(WorkerStoredData & worker);

    void parallelDestroyRightTableData();

    void validateAdditionalFilterExpression(std::shared_ptr<ExpressionActions> additional_filter_expression);
    bool needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const;

    bool isRightTableRerangeEnabled() const;
    bool rightTableCanBeReranged() const;
    void tryRerangeRightTableData();

    template <JoinKind KIND, typename Map, JoinStrictness STRICTNESS> // NOLINT(readability-identifier-naming)
    void tryRerangeRightTableDataImpl(Map & map);

    bool canConvertToFixedHashMap() const;

    /// Publish a SharedFixedHashTableRuntimeFilter that replaces the Set/BloomFilter
    /// installed by BuildRuntimeFilterStep, when the build side is a FixedHashMap.
    void publishSharedRuntimeFilters();
    void tryConvertToFixedHashMap();

    template <bool is_signed, typename Key, typename SourcePtr, typename MapsTemplate>
    void tryConvertToFixedHashMapImpl(MapsTemplate & maps, SourcePtr & source_ptr);

    void freezeMapsForProbing();

    bool isRowStoreSupported() const;

    void reinitUsedFlags();

    bool hasNonJoinedRows() const;
    bool recordsRowRefsForStats() const;

    /// Walks every worker's stored blocks. Skip while a parallel fill is still appending.
    void doDebugAsserts() const;
};
}
