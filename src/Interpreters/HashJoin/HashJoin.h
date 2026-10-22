#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
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

#include <Columns/ColumnNullable.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashJoin/BuildSlots.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableTraits.h>
#include <Common/HashTable/PartitionedFixedHashTable.h>
#include <Common/HashTable/TwoLevelHashMap.h>

namespace DB
{

class TableJoin;
class ExpressionActions;
using Sizes = std::vector<size_t>;

class MatchedRowsStats;

namespace JoinStuff
{
/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags;
}

namespace JoinCommon
{
class JoinMask;
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

class DeferredBuildState;

/// Zero bits is one bucket, where routing folds away and the map behaves as single-level.
constexpr size_t BITS_FOR_BUCKET_SERIAL = 0;

constexpr size_t NUM_HASH_TABLE_BUCKETS = 1ull << DEFAULT_BITS_FOR_BUCKET;

/// How a `HashJoin` is built: with two-level maps (the parallel layout) or one-bucket maps (the serial layout),
/// how many build streams may fill it, and how many slots split the buckets of its maps.
struct HashJoinBuildLayout
{
    bool two_level;
    size_t num_build_workers;
    size_t num_slots;

    /// One-bucket maps behind one slot, whatever the number of workers.
    static HashJoinBuildLayout oneBucket(size_t num_build_workers) { return {false, std::max<size_t>(1, num_build_workers), 1}; }

    /// Two-level maps with one slot per worker, rounded up to a power of two and capped at the bucket count.
    static HashJoinBuildLayout twoLevel(size_t num_build_workers)
    {
        const size_t workers = std::max<size_t>(1, num_build_workers);
        return {true, workers, std::min<size_t>(std::bit_ceil(workers), NUM_HASH_TABLE_BUCKETS)};
    }
};

struct BuildResult
{
    bool is_inserted = false;
    bool all_values_unique = true;
};

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
  * With the parallel layout, several threads can build the hash table at once. A bucket is a part of the key space
  * that is always filled under the same lock. It is a sub-table of a `TwoLevelHashTable`, or a set of cache lines
  * of the flat `PartitionedFixedHashTable`. The parallel layout has `NUM_HASH_TABLE_BUCKETS` buckets, and the
  * serial layout has one. A slot is a group of buckets with one lock and one arena (see `BuildSlots`). The serial
  * layout has one slot, and the parallel layout one per build thread. A worker is one build stream (see `WorkerStoredData`).
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
        bool any_take_last_row_,
        size_t reserve_num_,
        const String & instance_id_,
        HashJoinBuildLayout build_layout_,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        bool allow_deferred_build_ = false);

    /// A join without a reserve or an instance id.
    static std::shared_ptr<HashJoin> create(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block,
        HashJoinBuildLayout build_layout_,
        bool any_take_last_row_ = false,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        bool allow_deferred_build_ = false)
    {
        return std::make_shared<HashJoin>(
            std::move(table_join_),
            std::move(right_sample_block),
            any_take_last_row_,
            /*reserve_num_=*/0,
            /*instance_id_=*/"",
            build_layout_,
            stats_collecting_params_,
            allow_deferred_build_);
    }

    ~HashJoin() override;

    std::string getName() const override { return "HashJoin"; }

    std::string getAlgorithm() const override { return toString(JoinAlgorithm::HASH); }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return getTotals().empty() && getTotalRowCount() == 0;
    }

    /// The left side is streamed through once, each row emitted in input order.
    bool preservesLeftBlockOrder() const override { return true; }

    std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        return cloneWith(table_join_, right_sample_block_, build_layout);
    }

    /// `joinPipelinesByShards` never installs `NonJoinedBlocksTransform`: a clone reporting parallel
    /// non-joined processing would skip the unmatched right rows of a RIGHT/FULL join.
    std::shared_ptr<IJoin> cloneNoParallel(
        const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        return cloneWith(table_join_, right_sample_block_, HashJoinBuildLayout::oneBucket(1));
    }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool addBlockToJoin(const Block & source_block_, size_t num_rows, JoinBuildContext context) override;

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

    /// A concurrent fill needs several slots. The planner fills a join with one slot, in either layout, from one
    /// stream, so its output row order stays reproducible.
    bool supportParallelJoin() const override { return build_layout.num_slots > 1; }
    size_t getMaxBuildThreads() const override { return build_layout.num_build_workers; }

    bool supportParallelNonJoinedBlocksProcessing() const override;

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
        const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size,
        size_t bucket_idx, size_t num_buckets) const override;

    void onBuildPhaseFinish() override;
    void onProbePhaseFinish(std::optional<size_t> matched_right_rows) override
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

    /// Whether the build stores the right blocks first and inserts their keys in the post-build phase,
    /// see `enable_hash_join_deferred_build`.
    bool defersBuild() const { return deferred_build != nullptr; }
    /// For a deferred build: the bytes it will hold once the stored blocks are inserted. With `all_blocks_stored = false`,
    /// a cheaper estimate from the counts published so far, which concurrent callers may share.
    size_t getProjectedBuildBytes(bool all_blocks_stored) const;

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
        M(key8)                        \
        M(key16)                       \
        M(key32)                       \
        M(key64)                       \
        M(key_string)                  \
        M(key_fixed_string)            \
        M(keys32)                      \
        M(keys64)                      \
        M(keys128)                     \
        M(keys256)                     \
        M(hashed)                      \
        M(low_cardinality_key_string)       \
        M(low_cardinality_key_fixed_string) \
        M(two_level_key8)              \
        M(two_level_key16)             \
        M(two_level_key32)             \
        M(two_level_key64)             \
        M(two_level_key_string)        \
        M(two_level_key_fixed_string)  \
        M(two_level_keys32)            \
        M(two_level_keys64)            \
        M(two_level_keys128)           \
        M(two_level_keys256)           \
        M(two_level_hashed)            \
        M(range8_key32)                \
        M(range16_key32)               \
        M(range17_key32)               \
        M(range18_key32)               \
        M(range8_key64)                \
        M(range16_key64)               \
        M(range17_key64)               \
        M(range18_key64)

    /// Used for reading from StorageJoin and applying joinGet function. The single-LowCardinality-key
    /// maps store key values in maps physically identical to their non-LowCardinality counterparts, so
    /// they are read back the same way (the output key column is the parent LowCardinality type).
    /// The keysN maps hold the key columns packed into one fixed-width blob, so each key column is
    /// recovered from its own byte range. `hashed` is absent: its map key is a hash of the values.
    /// Only the serial types are listed: a `JoinSource` reads a `StorageJoin` map, and `StorageJoin`
    /// always builds with the serial layout.
    #define APPLY_FOR_JOIN_VARIANTS_LIMITED(M) \
        M(key8)                                \
        M(key16)                               \
        M(key32)                               \
        M(key64)                               \
        M(key_string)                          \
        M(key_fixed_string)                    \
        M(keys32)                              \
        M(keys64)                              \
        M(keys128)                             \
        M(keys256)                             \
        M(low_cardinality_key_string)          \
        M(low_cardinality_key_fixed_string)

    #define APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M, ...)           \
        M(two_level_key8 __VA_OPT__(,) __VA_ARGS__)             \
        M(two_level_key16 __VA_OPT__(,) __VA_ARGS__)            \
        M(two_level_key32 __VA_OPT__(,) __VA_ARGS__)            \
        M(two_level_key64 __VA_OPT__(,) __VA_ARGS__)            \
        M(two_level_key_string __VA_OPT__(,) __VA_ARGS__)       \
        M(two_level_key_fixed_string __VA_OPT__(,) __VA_ARGS__) \
        M(two_level_keys32 __VA_OPT__(,) __VA_ARGS__)           \
        M(two_level_keys64 __VA_OPT__(,) __VA_ARGS__)           \
        M(two_level_keys128 __VA_OPT__(,) __VA_ARGS__)          \
        M(two_level_keys256 __VA_OPT__(,) __VA_ARGS__)          \
        M(two_level_hashed __VA_OPT__(,) __VA_ARGS__)

    /// The packed fixed-width maps, `keys32` to `keys256`, hold the keys of a clause if all of them are fixed-width
    /// and take at most this many bytes together.
    static constexpr size_t max_packed_keys_bytes = 32;

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
      *
      * A join whose result never contains a value taken from a right row - see `MapGetter` - does not
      * need the mapped part of a cell at all: the table only has to answer whether a key is present.
      * Such a join instantiates the maps with `VoidMapped`, which selects the set counterpart of every
      * table below, so a cell holds the key alone.
      */
    /// A one-bucket map keeps the single-level grower. At each rehash, `TwoLevelHashTableGrower` doubles a large bucket and the
    /// single-level grower quadruples it. Doubling suits the small buckets of a two-level map, but one bucket would rehash more often.
    template <typename Key, typename Mapped, typename Hash>
    using JoinHashMap = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        TwoLevelHashSet<Key, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator, BITS_FOR_BUCKET_SERIAL>,
        TwoLevelHashMap<Key, Mapped, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator, HashMapTable, BITS_FOR_BUCKET_SERIAL>>;

    template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
    using JoinHashMapWithSavedHash = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        TwoLevelHashSetWithSavedHash<Key, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator, BITS_FOR_BUCKET_SERIAL>,
        TwoLevelHashMapWithSavedHash<
            Key,
            Mapped,
            Hash,
            HashTableGrowerWithPrecalculation<>,
            HashTableAllocator,
            HashMapTable,
            BITS_FOR_BUCKET_SERIAL>>;

    template <typename Key, typename Mapped, typename Hash>
    using JoinTwoLevelHashMap
        = std::conditional_t<std::is_same_v<Mapped, VoidMapped>, TwoLevelHashSet<Key, Hash>, TwoLevelHashMap<Key, Mapped, Hash>>;

    template <typename Key, typename Mapped>
    using JoinTwoLevelHashMapWithSavedHash = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        TwoLevelHashSetWithSavedHash<Key>,
        TwoLevelHashMapWithSavedHash<Key, Mapped>>;

    template <typename Key, typename Mapped>
    using JoinFixedHashMap = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        PartitionedFixedHashSet<Key, sizeof(Key) * 8, BITS_FOR_BUCKET_SERIAL>,
        PartitionedFixedHashMap<Key, Mapped, sizeof(Key) * 8, BITS_FOR_BUCKET_SERIAL>>;

    template <typename Key, typename Mapped, size_t size_bits>
    using JoinFixedHashMapWithSizeBits = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        PartitionedFixedHashSet<Key, size_bits, BITS_FOR_BUCKET_SERIAL>,
        PartitionedFixedHashMap<Key, Mapped, size_bits, BITS_FOR_BUCKET_SERIAL>>;

    template <typename Key, typename Mapped>
    using JoinTwoLevelFixedHashMap = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        PartitionedFixedHashSet<Key, sizeof(Key) * 8, DEFAULT_BITS_FOR_BUCKET>,
        PartitionedFixedHashMap<Key, Mapped, sizeof(Key) * 8, DEFAULT_BITS_FOR_BUCKET>>;

    template <typename Mapped>
    struct MapsTemplate
    {
        /// NOLINTBEGIN(bugprone-macro-parentheses)
        using MappedType = Mapped;
        static constexpr bool has_mapped = !std::is_same_v<Mapped, VoidMapped>;
        std::shared_ptr<JoinFixedHashMap<UInt8, Mapped>>                          key8;
        std::shared_ptr<JoinFixedHashMap<UInt16, Mapped>>                         key16;
        std::shared_ptr<JoinTwoLevelFixedHashMap<UInt8, Mapped>>                  two_level_key8;
        std::shared_ptr<JoinTwoLevelFixedHashMap<UInt16, Mapped>>                 two_level_key16;
        std::shared_ptr<JoinHashMap<UInt32, Mapped, HashCRC32<UInt32>>>           key32;
        std::shared_ptr<JoinHashMap<UInt64, Mapped, HashCRC32<UInt64>>>           key64;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>>              key_string;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>>              key_fixed_string;
        std::shared_ptr<JoinHashMap<UInt32, Mapped, HashCRC32<UInt32>>>           keys32;
        std::shared_ptr<JoinHashMap<UInt64, Mapped, HashCRC32<UInt64>>>           keys64;
        std::shared_ptr<JoinHashMap<UInt128, Mapped, UInt128HashCRC32>>           keys128;
        std::shared_ptr<JoinHashMap<UInt256, Mapped, UInt256HashCRC32>>           keys256;
        std::shared_ptr<JoinHashMap<UInt128, Mapped, UInt128TrivialHash>>         hashed;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>>      low_cardinality_key_string;
        std::shared_ptr<JoinHashMapWithSavedHash<std::string_view, Mapped>>      low_cardinality_key_fixed_string;
        std::shared_ptr<JoinTwoLevelHashMap<UInt32, Mapped, HashCRC32<UInt32>>>   two_level_key32;
        std::shared_ptr<JoinTwoLevelHashMap<UInt64, Mapped, HashCRC32<UInt64>>>   two_level_key64;
        std::shared_ptr<JoinTwoLevelHashMapWithSavedHash<std::string_view, Mapped>>      two_level_key_string;
        std::shared_ptr<JoinTwoLevelHashMapWithSavedHash<std::string_view, Mapped>>      two_level_key_fixed_string;
        std::shared_ptr<JoinTwoLevelHashMap<UInt32, Mapped, HashCRC32<UInt32>>>   two_level_keys32;
        std::shared_ptr<JoinTwoLevelHashMap<UInt64, Mapped, HashCRC32<UInt64>>>   two_level_keys64;
        std::shared_ptr<JoinTwoLevelHashMap<UInt128, Mapped, UInt128HashCRC32>>   two_level_keys128;
        std::shared_ptr<JoinTwoLevelHashMap<UInt256, Mapped, UInt256HashCRC32>>   two_level_keys256;
        std::shared_ptr<JoinTwoLevelHashMap<UInt128, Mapped, UInt128TrivialHash>> two_level_hashed;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 8>>          range8_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 16>>         range16_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 17>>         range17_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 18>>         range18_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 8>>          range8_key64;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 16>>         range16_key64;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 17>>         range17_key64;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 18>>         range18_key64;

        void create(Type which)
        {
            visit(which, []<typename Table>(std::shared_ptr<Table> & map) { map = std::make_shared<Table>(); });
        }

        /// Calls `f` with the map of type `which`, a `std::shared_ptr` that is null until `create`.
        template <typename Self, typename F>
        decltype(auto) visit(this Self && self, Type which, F && f)
        {
            switch (which)
            {
#define M(NAME) \
    case Type::NAME: return std::forward<F>(f)(self.NAME);
                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
            UNREACHABLE();
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

    /// A stored block of a deferred build whose keys are not in the maps yet. `columns` has the right ON condition
    /// columns as they arrived: the insert reads them, and the stored block does not hold them.
    struct PendingInsert
    {
        StoredBlocksList::iterator stored;
        Block columns;
        size_t allocated_bytes = 0;
    };

    /// The lists of one build stream: only its inserts change them, and they are moved only after the inserts stop,
    /// so they need no mutex. `finishBuildPhase` splices them into the lists of the first worker.
    struct WorkerStoredData
    {
        StoredBlocksList columns;
        NullmapList nullmaps;
        std::vector<PendingInsert> pending_inserts;
        bool shrink_done = false;
    };

    struct RightTableData
    {
        explicit RightTableData(const HashJoinBuildLayout & layout)
            : workers(layout.num_build_workers)
            , slots(layout.num_slots)
        {
        }

        /// Frees a large table on a thread pool, see the definition.
        ~RightTableData();

        void spliceWorkerLists();

        Type type = Type::hashed;

        /// tab1 join tab2 on t1.x = t2.x or t1.y = t2.y
        /// =>
        /// tab1 join tab2 on t1.x = t2.x
        /// join tab2 on [not_joined(t1.x = t2.x)] and t1.y = t2.y
        std::vector<MapsVariant> maps;
        Block sample_block; /// Block as it would appear in the BlockList
        /// Track index of "right" table columns in columns list or row store.
        ColumnAccessIndexes column_access_indexes;

        /// `StorageJoin` has one worker.
        std::vector<WorkerStoredData> workers;

        /// Resolves RowRef::block_no to the stored block.
        StoredColumnsIndexPtr stored_columns_index = std::make_shared<StoredColumnsIndex>();

        BuildSlots slots;

        std::atomic<size_t> allocated_size = 0;
        std::atomic<size_t> nullmaps_allocated_size = 0;
        /// The columns of `PendingInsert`.
        std::atomic<size_t> pending_allocated_size = 0;
        /// Number of rows of right table to join
        std::atomic<size_t> rows_to_join = 0;
        /// Running totals: summing the buckets would read what other build threads are mutating.
        /// `bucket_bytes` accumulates deltas, so a post-build step that swaps maps must recompute it.
        std::atomic<size_t> keys_to_join = 0;
        std::atomic<size_t> bucket_bytes = 0;

        /// `allocated_size + nullmaps_allocated_size + pending_allocated_size + bucket_bytes` in one atomic: summing them
        /// while workers update them can under-count `max_bytes_in_join`. Size-limit checks and `peak_build_bytes` read only this.
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
            if (keys_to_join == 0)
                return 0;
            return rows_to_join / keys_to_join;
        }
    };

    /// For INNER/LEFT ALL JOINs, if the right side has no duplicates inside the join key columns,
    /// we can switch from ALL to RightAny strictness for better performance.
    /// Only ever goes from true to false, so a relaxed store needs no further ordering.
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
    BlocksList releaseJoinedBlocks(bool restructure = false);
    BlocksList releaseWorkerBlocks(size_t worker_id);
    void releaseJoinMaps();

    /// Modify right block (update structure according to sample block) to save it in block list
    static Block prepareRightBlock(const Block & block, const Block & saved_block_sample_);
    Block prepareRightBlock(const Block & block) const;

    const Block & savedBlockSample() const { return data->sample_block; }

    bool isUsed(size_t off) const;
    bool isUsed(UInt32 block_no, size_t row_idx) const;

    void debugKeys() const;

    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize = false, size_t worker_id = 0);

    void setMaxJoinedBlockRows(size_t value) { max_joined_block_rows = value; }
    void setMaxJoinedBlockBytes(size_t value) { max_joined_block_bytes = value; }

    void materializeColumnsFromLeftBlock(Block & block) const;
    Block materializeColumnsFromRightBlock(Block block) const;

    const std::vector<Sizes> & getKeySizes() const { return key_sizes; }

    bool enableLazyColumnsReplication() const { return enable_lazy_columns_replication; }
    bool enableSoftwarePrefetch() const { return enable_prefetch; }

    void setEnableLazyColumnsIndexing(bool value) override { enable_lazy_columns_indexing = value; }

    static bool isUsedByAnotherAlgorithm(const TableJoin & table_join);
    static bool canRemoveColumnsFromLeftBlock(const TableJoin & table_join);

private:
    friend class NotJoinedHash;
    friend class JoinSource;

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
    friend class HashJoinMethods;

    std::shared_ptr<IJoin> cloneWith(
        const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader right_sample_block_,
        HashJoinBuildLayout build_layout_) const
    {
        return std::make_shared<HashJoin>(
            table_join_,
            right_sample_block_,
            any_take_last_row,
            reserve_num,
            instance_id,
            build_layout_,
            HashJoinStatsCollectingParams{},
            allow_deferred_build);
    }

    std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;

    /// This join was created from StorageJoin and it is already filled.
    bool from_storage_join = false;

    const bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    const size_t reserve_num;
    const String instance_id;
    const HashJoinBuildLayout build_layout;
    /// Whether the planner lets this join defer its build, see `enable_hash_join_deferred_build`.
    const bool allow_deferred_build;
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
    /// Reading a stale `false` only delays the shrink by one block, so relaxed is enough.
    std::atomic<bool> shrink_blocks = false;
    std::atomic<Int64> memory_usage_before_adding_blocks = 0;

    /// Peak of bytes observed in the hash table during the build phase
    std::atomic<size_t> peak_build_bytes = 0;

    void updatePeakBuildBytes(size_t bytes)
    {
        size_t prev = peak_build_bytes.load(std::memory_order_relaxed);
        while (prev < bytes && !peak_build_bytes.compare_exchange_weak(prev, bytes, std::memory_order_relaxed))
        {
        }
    }

    /// Track if conversion to fixed hash map was already attempted to prevent repeated checks.
    bool conversion_to_fixed_hash_map_attempted = false;

    /// Track if shared runtime filters were already published to keep publication one-shot.
    bool shared_runtime_filters_publish_attempted = false;

    const HashJoinStatsCollectingParams stats_collecting_params;

    /// Set in the constructor when the build stores the right blocks first and inserts their keys in the post-build
    /// phase, once the number of distinct keys is known, instead of inserting each block as it arrives (a streaming
    /// build). See `replayDeferredBuild`.
    std::unique_ptr<DeferredBuildState> deferred_build;
    /// From `onBuildPhaseFinish` of a deferred build until its keys are in the maps.
    bool replay_pending = false;
    /// Whether a build stream asked this join to check the size limits, which a deferred build does once, in `replayDeferredBuild`.
    std::atomic<bool> deferred_build_checks_limits = false;

    bool build_phase_finished = false;
    bool probe_phase_finished = false;

    /// Rows emitted from hash-table matches across all probe threads (excludes default/miss rows).
    std::optional<size_t> hash_table_matches;

    /// Whether the maps store keys alone, see `JoinMapsKind::Set`. Decided once, before they are created.
    bool use_set_maps = false;

    /// Identifier to distinguish different HashJoin instances in logs
    /// Several instances can be created, for example, in GraceHashJoin to handle different buckets
    String instance_log_id;

    LoggerPtr log;

    /// Should be set via setLock to protect hash table from modification from StorageJoin
    /// If set HashJoin instance is not available for modification (addBlockToJoin)
    TableLockHolder storage_join_lock = nullptr;

    void recomputeBucketBytes();

    void dataMapInit(MapsVariant & map);

    void initRightBlockStructure(Block & saved_block_sample);

    class ContinuationResult;

    JoinResultPtr runJoinDispatch(ScatteredBlock block);

    bool preferUseMapsAll() const;

    bool canUseSetMaps() const;

public:
    bool mustKeepRightBlocks() const;

    /// Called by the algorithm that wraps this join, before it feeds it anything, when it may take
    /// the right blocks back out with `releaseJoinedBlocks`. Off by default: a join nobody wraps
    /// keeps no block a set map does not need, and whether some algorithm merely appears in
    /// `join_algorithm` says nothing about what was instantiated.
    void keepRightBlocksForAnotherAlgorithm() { right_blocks_may_be_taken = true; }

    /// Called once that algorithm can no longer take them - it has settled on this join. A join that
    /// stores only the keys reads nothing from them, so they can go.
    void dropRightBlocksKeptForAnotherAlgorithm();

private:

    static Type toTwoLevelType(Type type);

    /// The maps flavour this join runs on. All the dispatch entry points take it.
    JoinMapsKind getMapsKind() const;

    bool isUsedByAnotherAlgorithm() const;

    /// Whether a wrapping algorithm said it may take the right blocks. See
    /// `keepRightBlocksForAnotherAlgorithm`.
    bool right_blocks_may_be_taken = false;
    bool canRemoveColumnsFromLeftBlock() const;

    /// Appends the right columns of `block` to the list of `worker` and accounts for them.
    StoredBlocksList::iterator storeBlock(WorkerStoredData & worker, const Block & block, ScatteredBlock::Selector selector);

    /// Whether `storeBlock` stores copies of the columns of `block`.
    bool storesCopies(const Block & block) const;

    /// The key column `column` or the key columns of `block`, converted the way the maps read them.
    ColumnPtr prepareRightKeyColumn(const ColumnPtr & column) const;
    std::unordered_map<String, ColumnPtr> prepareRightKeyColumns(const Block & block) const;

    /// The index in `StoredBlock::columns` of the column `name`, or none when the stored blocks keep it in the row store or not at all.
    std::optional<size_t> storedColumnIndex(const String & name) const;

    /// Whether a deferred build holds no more than the streaming build: the stored blocks have the keys, and the map keeps every row.
    bool deferredBuildHoldsNoMore() const;

    /// For a deferred build: counts the keys of the stored block in the sketches of `stream`, and keeps what
    /// `insertStoredBlock` will read in a `PendingInsert` of `worker`.
    void addPendingInsert(WorkerStoredData & worker, StoredBlocksList::iterator stored, const Block & block, size_t stream);

    /// The block that `insertStoredBlock` reads for `pending`: the key columns of the stored block and the columns of `pending`.
    Block pendingInsertBlock(const PendingInsert & pending) const;

    /// Reserves the maps from the counts of a deferred build and inserts the stored blocks, with up to one thread per build stream.
    void replayDeferredBuild();

    /// What `onBuildPhaseFinish` does once the keys are in the maps.
    void finishBuildPhase();

    /// Inserts the keys of the stored block `stored` into the maps. `block` has the key and ON condition columns of the
    /// rows the block was stored from. Removes the stored block when nothing refers to it. When the join checks the
    /// limits, sets the totals for that check.
    void insertStoredBlock(
        WorkerStoredData & worker,
        StoredBlocksList::iterator stored,
        const Block & block,
        JoinBuildContext context,
        size_t & total_rows,
        size_t & total_bytes);

    void shrinkWorkerStoredBlocks(WorkerStoredData & worker);

    void validateAdditionalFilterExpression(std::shared_ptr<ExpressionActions> additional_filter_expression);
    bool needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const;

    bool isRightTableRerangeEnabled() const;
    bool rightTableCanBeReranged() const;
    void tryRerangeRightTableData();

    template <JoinKind KIND, typename Map, JoinStrictness STRICTNESS>
    void tryRerangeRightTableDataImpl(Map & map);

    bool canConvertToFixedHashMap() const;

    /// Publish a SharedFixedHashTableRuntimeFilter that replaces the Set/BloomFilter
    /// installed by BuildRuntimeFilterStep, when the build side is a FixedHashMap.
    void publishSharedRuntimeFilters();
    void tryConvertToFixedHashMap();

    template <bool is_signed, typename Key, typename MapsTemplate>
    void tryConvertToFixedHashMapImpl(MapsTemplate & maps);

    void freezeMapsForProbing();

    bool isRowStoreSupported() const;

    /// Layout is from the sample block, before any fill thread.
    void initRowStore(const Block & block);

    void reinitUsedFlags();

    bool recordsRowRefsForStats() const;

    void doDebugAsserts() const;
};
}
