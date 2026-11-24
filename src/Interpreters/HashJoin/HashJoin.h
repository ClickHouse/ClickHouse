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
#include <Interpreters/HashJoin/HashJoinBuild.h>
#include <Interpreters/HashJoin/JoinPartitionView.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableTraits.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <base/sanitizer_defs.h>

namespace DB
{

class TableJoin;
class ExpressionActions;
using Sizes = std::vector<size_t>;

class MatchedRowsStats;
class HashJoinTable;
struct HashJoinBuildState;

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

/// How a `HashJoin` is built.
struct HashJoinBuildLayout
{
    /// The planned layout: `false` is the serial layout (one partition), `true` the parallel layout (several partitions,
    /// chosen when the build runs).
    bool parallel = false;
    /// The build streams that may fill the join.
    size_t num_build_streams = 1;
    /// A `Join` engine table: every block goes into the map when it arrives, and the table is never frozen.
    bool insert_on_arrival = false;

    static HashJoinBuildLayout serialLayout(size_t num_build_streams)
    {
        return {.parallel = false, .num_build_streams = std::max<size_t>(1, num_build_streams), .insert_on_arrival = false};
    }
    static HashJoinBuildLayout parallelLayout(size_t num_build_streams)
    {
        return {.parallel = true, .num_build_streams = std::max<size_t>(1, num_build_streams), .insert_on_arrival = false};
    }
    static HashJoinBuildLayout joinEngine() { return {.parallel = false, .num_build_streams = 1, .insert_on_arrival = true}; }
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
  * With the parallel layout, the hash table of a disjunct is split into partitions by the hash of the key, and each
  * partition is an ordinary hash table that one thread fills (see `JoinPartitionedView`). The serial layout has one
  * partition.
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
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {});

    /// A join without a reserve or an instance id.
    static std::shared_ptr<HashJoin> create(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block,
        HashJoinBuildLayout build_layout_,
        bool any_take_last_row_ = false,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {})
    {
        return std::make_shared<HashJoin>(
            std::move(table_join_),
            std::move(right_sample_block),
            any_take_last_row_,
            /*reserve_num_=*/0,
            /*instance_id_=*/"",
            build_layout_,
            stats_collecting_params_);
    }

    ~HashJoin() override;

    std::string getName() const override { return "HashJoin"; }

    std::string getAlgorithm() const override { return toString(JoinAlgorithm::HASH); }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return getTotals().empty() && getTotalRowCount() == 0;
    }

    bool preservesLeftBlockOrder() const override { return !allow_reordering; }
    bool canPreserveLeftBlockOrder() const override { return true; }
    void keepLeftPipelineInOrder() override { allow_reordering = false; }
    void allowLeftRowsReordering() override { allow_reordering = true; }

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
        return cloneWith(table_join_, right_sample_block_, HashJoinBuildLayout::serialLayout(1));
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

    /// The parallel layout with more than one build stream. Otherwise the planner fills the join from one stream, so its
    /// output row order stays reproducible.
    bool supportParallelJoin() const override { return build_layout.parallel && build_layout.num_build_streams > 1; }
    size_t getMaxBuildThreads() const override { return build_layout.num_build_streams; }

    bool supportParallelNonJoinedBlocksProcessing() const override;

    void setTotals(const Block & block) override;

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

    /// True until `finishBuild` has run, for every join that is not a `Join` engine table.
    bool hasPostBuildPhase() const override;
    /// `finishBuild`, then `runPostBuildOptimizations`.
    void runPostBuildPhase() override;

    /// Builds what still waits, freezes the tables for probing, sizes the used flags, promotes `ALL` to `RightAny`, splices
    /// the lists of the build streams and runs a drop of the kept blocks that a wrapper asked for. Every driver calls it once;
    /// a second call throws `LOGICAL_ERROR`. A `Join` engine table never calls it.
    void finishBuild();
    /// Rerange, the conversion to a fixed table, the shared runtime filter. After `finishBuild`.
    void runPostBuildOptimizations();
    bool isBuildFinished() const { return build_phase_finished; }

    /// The allocated bytes, plus the tables and arenas that do not exist yet. The spill check while the join
    /// waits, grace's overflow check and the byte bound of the wait read it. Equal to `getTotalByteCount` once built.
    size_t getProjectedByteCount() const;
    /// The scatter copy that the build of the waiting rows takes at its peak: the copy of one round, at most `roundCopyCap`.
    /// 0 for a join without partitions, and once built. The memory scheduler adds it to `getProjectedByteCount`.
    size_t getProjectedCopyBytes() const;

    /// The limits that end the wait in time, from a wrapper that checks the limits itself (`JoinSwitcher`, grace). The join
    /// then runs no limit check of its own. Before the first block.
    void setWaitLimits(const SizeLimits & limits);
    /// From a wrapper that a memory spill scheduler can make spill: a join of one build stream ends its wait when an upper
    /// bound of its bytes reaches `bytes`, so that the scheduler sees its table grow block by block, as without the wait.
    /// After the early end, the table of one stream takes each block as it comes, so the early end costs it nothing. 0 = no
    /// bound. Before the first block.
    void setWaitMemoryBound(size_t bytes);

    /// At build finish, before any table exists: whether the build fits `budget` at once, in rounds, or must spill.
    enum class BuildFit : uint8_t
    {
        Fits,
        Grouped,
        MustSpill,
    };
    /// A Grouped verdict is remembered: `finishBuild` then builds in rounds within `budget`.
    BuildFit evaluateBuildFit(size_t budget);
    /// Whether the join still waits (no table built yet, or a batch waiting).
    bool isWaiting() const;
    /// Whether a table was built before build finish (several streams, an early end). Such a table grows as blocks arrive, so
    /// a wrapper checks the allocated bytes, not the projection.
    bool builtEarly() const;

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
        M(range8_key32)                \
        M(range16_key32)               \
        M(range17_key32)               \
        M(range18_key32)               \
        M(range8_key64)                \
        M(range16_key64)               \
        M(range17_key64)               \
        M(range18_key64)               \
        M(key_packed_string)

    /// Used for reading from StorageJoin and applying joinGet function. The single-LowCardinality-key
    /// maps store key values in maps physically identical to their non-LowCardinality counterparts, so
    /// they are read back the same way (the output key column is the parent LowCardinality type).
    /// The keysN maps hold the key columns packed into one fixed-width blob, so each key column is
    /// recovered from its own byte range. `hashed` is absent: its map key is a hash of the values.
    /// The packed `String` maps are absent too, because a key of 16 bytes or more is stored as its hash.
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
    /// The map of a table planned with one partition: the single-level grower, which quadruples a large table at each rehash.
    template <typename Key, typename Mapped, typename Hash>
    using JoinHashMap = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        HashSetTable<Key, HashTableCell<Key, Hash>, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>,
        HashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>>;

    template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
    using JoinHashMapWithSavedHash = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        HashSetTable<Key, HashSetCellWithSavedHash<Key, Hash>, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>,
        HashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, HashTableGrowerWithPrecalculation<>, HashTableAllocator>>;

    /// The build sizes partitions together. `JoinPartitionedView` grows them before a new key would take the last empty cell.
    struct JoinPartitionGrower : TwoLevelHashTableGrower<1>
    {
        using Base = TwoLevelHashTableGrower<1>;

        void setExactCapacity(size_t cells, bool robin_hood)
        {
            setRobinHoodAllowed(robin_hood);
            if (robin_hood)
                Base::set(cells / 2 + 1);
            else
                Base::setBufSize(cells);
            chassert(bufSize() == cells && isRobinHood() == robin_hood);
        }

        void setBufSize(size_t cells) { setExactCapacity(cells, isRobinHood()); }
        void increaseSize() { setBufSize(bufSize() * 2); }
        bool overflow(size_t keys) const { return keys > bufSize(); }
    };

    template <typename Key, typename Mapped, typename Hash>
    using JoinPartitionHashMap = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        HashSetTable<Key, HashTableCell<Key, Hash>, Hash, JoinPartitionGrower, HashTableAllocator>,
        HashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, JoinPartitionGrower, HashTableAllocator>>;

    template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
    using JoinPartitionHashMapWithSavedHash = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        HashSetTable<Key, HashSetCellWithSavedHash<Key, Hash>, Hash, JoinPartitionGrower, HashTableAllocator>,
        HashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, JoinPartitionGrower, HashTableAllocator>>;

    /// The two forms of a hash map: one map, or one map per partition.
    template <typename Key, typename Mapped, typename Hash>
    using JoinHashMapForms
        = JoinMapForms<JoinSingleView<JoinHashMap<Key, Mapped, Hash>>, JoinPartitionedView<JoinPartitionHashMap<Key, Mapped, Hash>>>;

    template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>>
    using JoinHashMapWithSavedHashForms = JoinMapForms<
        JoinSingleView<JoinHashMapWithSavedHash<Key, Mapped, Hash>>,
        JoinPartitionedView<JoinPartitionHashMapWithSavedHash<Key, Mapped, Hash>>>;

    template <typename Key, typename Mapped, size_t size_bits>
    using JoinFixedHashMapWithSizeBits = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        FixedHashSetWithSizeBitsAndCalculatedSize<Key, size_bits>,
        FixedHashMapWithSizeBitsAndCalculatedSize<Key, Mapped, size_bits>>;

    template <typename Key, typename Mapped>
    using JoinFixedHashMap = JoinFixedHashMapWithSizeBits<Key, Mapped, sizeof(Key) * 8>;

    /// The two forms of a `key8` or `key16` map: the direct-addressed map, or one hash map per partition, as for any key.
    template <typename Key, typename Mapped>
    using JoinFixedHashMapForms
        = JoinMapForms<JoinFixedHashMap<Key, Mapped>, JoinPartitionedView<JoinPartitionHashMap<Key, Mapped, HashCRC32<Key>>>>;

    template <typename Mapped>
    struct MapsTemplate
    {
        /// NOLINTBEGIN(bugprone-macro-parentheses)
        using MappedType = Mapped;
        static constexpr bool has_mapped = !std::is_same_v<Mapped, VoidMapped>;
        std::shared_ptr<JoinFixedHashMapForms<UInt8, Mapped>>                     key8;
        std::shared_ptr<JoinFixedHashMapForms<UInt16, Mapped>>                    key16;
        std::shared_ptr<JoinHashMapForms<UInt32, Mapped, HashCRC32<UInt32>>>      key32;
        std::shared_ptr<JoinHashMapForms<UInt64, Mapped, HashCRC32<UInt64>>>      key64;
        std::shared_ptr<JoinHashMapWithSavedHashForms<std::string_view, Mapped>>  key_string;
        std::shared_ptr<JoinHashMapWithSavedHashForms<std::string_view, Mapped>>  key_fixed_string;
        std::shared_ptr<JoinHashMapForms<UInt32, Mapped, HashCRC32<UInt32>>>      keys32;
        std::shared_ptr<JoinHashMapForms<UInt64, Mapped, HashCRC32<UInt64>>>      keys64;
        std::shared_ptr<JoinHashMapForms<UInt128, Mapped, UInt128HashCRC32>>      keys128;
        std::shared_ptr<JoinHashMapForms<UInt256, Mapped, UInt256HashCRC32>>      keys256;
        std::shared_ptr<JoinHashMapForms<UInt128, Mapped, UInt128TrivialHash>>    hashed;
        /// LowCardinality maps exist only in the serial layout, so they have one form.
        std::shared_ptr<JoinSingleView<JoinHashMapWithSavedHash<std::string_view, Mapped>>> low_cardinality_key_string;
        std::shared_ptr<JoinSingleView<JoinHashMapWithSavedHash<std::string_view, Mapped>>> low_cardinality_key_fixed_string;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 8>>          range8_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 16>>         range16_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 17>>         range17_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt32, Mapped, 18>>         range18_key32;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 8>>          range8_key64;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 16>>         range16_key64;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 17>>         range17_key64;
        std::shared_ptr<JoinFixedHashMapWithSizeBits<UInt64, Mapped, 18>>         range18_key64;
        /// A single `String` key as a `PackedStringJoinKey`, see `enable_packed_string_keys_in_hash_join`.
        std::shared_ptr<JoinHashMapForms<UInt128, Mapped, UInt128HashCRC32>>      key_packed_string;

        /// Creates the empty table of type `which` in the single form.
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

        /// The map of type `which`, a `std::shared_ptr` that is null until `create`.
        template <Type which, typename Self>
        auto & mapOf(this Self & self)
        {
#define M(NAME) \
    if constexpr (which == Type::NAME) \
        return self.NAME;
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
        }

        [[nodiscard]] size_t getTotalByteCountImpl(Type which) const
        {
            switch (which)
            {
            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->getBufferSizeInBytes() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }
        }

        [[nodiscard]] size_t getBufferSizeInCells(Type which) const
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

        [[nodiscard]] size_t allocatedBytes() const;
    };

    using NullmapList = std::deque<NullMapHolder>;
    using StoredBlocksList = std::list<StoredBlock>;

    enum class RowStoreState : uint8_t
    {
        Disabled,
        Enabled,
        Initialized,
    };

    /// The lists of one build stream: only its inserts change them, and they are moved only after the inserts stop,
    /// so they need no mutex. `onBuildPhaseFinish` splices them into the lists of the first build stream.
    struct BuildStreamData
    {
        StoredBlocksList columns;
        NullmapList nullmaps;
        bool shrink_done = false;
    };

    struct RightTableData
    {
        explicit RightTableData(const HashJoinBuildLayout & layout);
        /// The wait and the rounds of the build; null for a `Join` engine table.
        std::unique_ptr<HashJoinBuildState> build_state;

        /// Frees a large table on a thread pool, see the definition.
        ~RightTableData();

        void spliceBuildStreamLists();

        /// One table per `ON` disjunct:
        /// tab1 join tab2 on t1.x = t2.x or t1.y = t2.y
        /// =>
        /// tab1 join tab2 on t1.x = t2.x
        /// join tab2 on [not_joined(t1.x = t2.x)] and t1.y = t2.y
        std::vector<std::unique_ptr<HashJoinTable>> tables;
        Block sample_block; /// Block as it would appear in the BlockList
        /// Track index of "right" table columns in columns list or row store.
        ColumnAccessIndexes column_access_indexes;

        /// `StorageJoin` has one build stream.
        std::vector<BuildStreamData> build_streams;

        /// Resolves RowRef::block_no to the stored block.
        StoredColumnsIndexPtr stored_columns_index = std::make_shared<StoredColumnsIndex>();

        std::atomic<size_t> allocated_size = 0;
        std::atomic<size_t> nullmaps_allocated_size = 0;
        /// Number of rows of right table to join
        std::atomic<size_t> rows_to_join = 0;
        /// A running total: summing the buckets would read what other build threads are mutating.
        /// `bucket_bytes` accumulates deltas, so a post-build step that swaps maps must recompute it.
        std::atomic<size_t> bucket_bytes = 0;
        /// The records of the waiting blocks, see `WaitingBlockRecord`.
        std::atomic<size_t> record_bytes = 0;

        /// `allocated_size + nullmaps_allocated_size + bucket_bytes` in one atomic: summing the three while build threads
        /// update them can under-count `max_bytes_in_join`. Size-limit checks and `peak_build_bytes` read only this.
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

        size_t keys() const;

        [[nodiscard]] size_t avgPerKeyRows() const
        {
            const size_t num_keys = keys();
            if (num_keys == 0)
                return 0;
            return rows_to_join / num_keys;
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
    BlocksList releaseBuildStreamBlocks(size_t stream);
    void releaseJoinMaps();

    /// Modify right block (update structure according to sample block) to save it in block list
    static Block prepareRightBlock(const Block & block, const Block & saved_block_sample_);
    Block prepareRightBlock(const Block & block) const;

    const Block & savedBlockSample() const { return data->sample_block; }

    bool isUsed(UInt32 block_no, size_t row_idx) const;

    void debugKeys() const;

    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize = false, size_t stream = 0);

    void setMaxJoinedBlockRows(size_t value) { max_joined_block_rows = value; }
    void setMaxJoinedBlockBytes(size_t value) { max_joined_block_bytes = value; }

    void materializeColumnsFromLeftBlock(Block & block) const;
    Block materializeColumnsFromRightBlock(Block block) const;

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
        auto cloned = std::make_shared<HashJoin>(table_join_, right_sample_block_, any_take_last_row, reserve_num, instance_id, build_layout_);
        cloned->allow_reordering = allow_reordering;
        return cloned;
    }

    std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;

    /// This join was created from StorageJoin and it is already filled.
    bool from_storage_join = false;
    bool allow_reordering = false;

    const bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    const size_t reserve_num;
    const String instance_id;
    const HashJoinBuildLayout build_layout;
    std::optional<TypeIndex> asof_type;
    const ASOFJoinInequality asof_inequality;

    std::mutex totals_mutex;
    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// so we must guarantee constantness of hash table during HashJoin lifetime (using method setLock)
    mutable std::shared_ptr<JoinStuff::JoinUsedFlags> used_flags;

    std::unique_ptr<MatchedRowsStats> matched_rows_stats;
    RightTableDataPtr data;

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
    /// `SpillingHashJoin` reads it from the memory scheduler while `finishBuild` sets it.
    std::atomic<bool> build_phase_finished = false;
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

    void initRightBlockStructure(Block & saved_block_sample);

    class ContinuationResult;

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

    /// The key type of the maps of every table.
    Type keyType() const;

    /// The maps flavour this join runs on. All the dispatch entry points take it.
    JoinMapsKind getMapsKind() const;

    bool isUsedByAnotherAlgorithm() const;

    /// Whether a wrapping algorithm said it may take the right blocks. See
    /// `keepRightBlocksForAnotherAlgorithm`.
    bool right_blocks_may_be_taken = false;
    bool canRemoveColumnsFromLeftBlock() const;

    void shrinkBuildStreamBlocks(BuildStreamData & build_stream);

    void validateAdditionalFilterExpression(std::shared_ptr<ExpressionActions> additional_filter_expression);
    bool needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const;

    bool isRightTableRerangeEnabled() const;
    bool rightTableCanBeReranged() const;
    void tryRerangeRightTableData();

    template <JoinKind KIND, typename Maps, JoinStrictness STRICTNESS>
    void tryRerangeRightTableDataImpl();

    bool canConvertToFixedHashMap() const;

    /// Publish a SharedFixedHashTableRuntimeFilter that replaces the Set/BloomFilter
    /// installed by BuildRuntimeFilterStep, when the build side is a FixedHashMap.
    void publishSharedRuntimeFilters();
    void tryConvertToFixedHashMap();

    void freezeMapsForProbing();

    /// Layout is from the sample block, before any fill thread.
    void initRowStore(const Block & block);

    void reinitUsedFlags();

    bool recordsRowRefsForStats() const;

    void doDebugAsserts() const;

    /// The build of a join that waits, see `HashJoinBuild.cpp`.

    /// Called once, on the first block: one build stream or several, the bounds of the wait, the remembered verdict.
    void decideBuildKind(const JoinBuildContext & context);
    /// Rethrows the failure of a build that failed.
    void throwIfBuildFailed() const;
    /// Marks the build failed with the current exception. Call from a `catch` block.
    void markBuildFailed() noexcept;
    /// `KILL QUERY`, the time limit and the stop flags. Called between the blocks of a replay and inside rounds.
    void checkBuildCancelled() const;
    /// Inserts the rows of `record` into the table of every `ON` disjunct. Returns whether a table entry or a holder points
    /// into the stored block.
    bool insertRecord(WaitingBlockRecord & record, size_t stream);
    /// Drops the stored block of `record`, which nothing points into, from the list of `stream`.
    void dropStoredBlock(WaitingBlockRecord & record, size_t stream);
    /// Frees the record of a block that was inserted or scattered.
    void releaseRecord(WaitingBlockList & list, WaitingBlockList::iterator it);
    /// Feeds the sketch of `stream` and refreshes the live estimate; then checks the bounds. Returns why the wait ends now.
    JoinWaitEnd afterBlockWaits(size_t stream);
    void maybeRefreshEstimate();
    /// Merges the sketch of every stream into the estimate of each disjunct, whatever its change flag says. Only for a
    /// partitioned build, whose streams all have their sketches. No other thread may merge meanwhile.
    void mergeEverySketch();
    void liveTest(size_t rows);
    /// Starts the sketch of one stream of a table of one build stream: feeds the rows it already holds.
    void startSketch(size_t stream);
    /// The bounds of the wait that do not depend on the live test.
    JoinWaitEnd checkWaitBounds() const;
    /// The wait ended: builds every waiting block. One stream: the replay in arrival order. Several: a round of scatter and
    /// owners. Returns false when a limit with `break` stopped the right side.
    bool buildWaitingBlocks(JoinWaitEnd reason);
    bool replayOneStream();
    bool runPartitionedRound(JoinWaitEnd reason);
    /// The exact check of the size limits after a round or a replayed block. `bytes` is what the check reads.
    bool checkSizeLimitsAfterBuild(size_t bytes);
    /// The keys that the waiting rows of disjunct `clause_idx` add to its table, estimated: the merged sketch or the cached
    /// count, times `key_scale`, less the keys the table holds; or the rows themselves. `source` says which.
    size_t estimatedKeysOfClause(size_t clause_idx, const char ** source = nullptr, double key_scale = 1.0) const;
    /// The sum over the disjuncts.
    size_t estimatedKeys() const;
    /// The bytes that the tables and arenas will take for the waiting rows beyond what they hold now. `every_row_new` counts
    /// every waiting row as a new key, an upper bound; otherwise each disjunct takes its estimate times `key_scale`. A table
    /// that an early round built counts the growth that the new keys cause; with `include_built`, it counts as if built anew.
    /// Reads atomics and fields fixed at construction or by the first round only.
    size_t projectedTableBytes(bool every_row_new, bool include_built = false, double key_scale = 1.0, bool use_final_routes = false) const;
    /// The upper bound of the bytes of the join when every waiting row is a new key: the byte bound of the wait.
    size_t waitByteBound() const;
    /// The most bytes that the scatter copy of one round takes: the largest of the tables, a quarter of the bytes the join
    /// holds, and 256 MiB. A build of few keys and many rows then copies its rows in a few rounds, not all at once.
    size_t roundCopyCap() const;
    /// The bytes that the scatter of a round copies for `rows` rows whose `String` keys have `string_key_bytes` characters.
    size_t scatterCopyBytes(size_t rows, size_t string_key_bytes) const;
    /// Removes a waiting block that is never inserted (a limit with `break`), with its holders and its record.
    void discardWaitingBlock(WaitingBlockList::iterator it, size_t stream);
    void logBuild(JoinWaitEnd reason, const JoinPartitionPlan * plan, size_t rows) const;

    /// Decided on the first block, see `decideBuildKind`.
    std::once_flag build_kind_once;
    /// The join checks `max_rows_in_join` and `max_bytes_in_join` itself (`JoinBuildContext::joinChecksLimits`).
    bool join_checks_limits = true;
    /// A wrapper passed its limits through `setWaitLimits`: the join runs no limit check and no shrink of its own.
    bool wait_limits_from_wrapper = false;
    /// A grace bucket: the sketch starts at the first block. See `estimateKeysFromFirstBlock`.
    bool sketch_from_first_block = false;
    /// The budget of a Grouped verdict of `evaluateBuildFit`: `finishBuild` builds in rounds over ranges of blocks. 0 = none.
    size_t grouped_budget = 0;
    /// The 256-partition reserve hint of the parallel layout fed by one stream, capped at the spill budget.
    size_t one_stream_reserve_hint = 0;
    /// The buffer bytes of each table when the constructor made it (after its reserve), so the projection counts only growth.
    std::vector<size_t> initial_table_bytes;
    /// The inserts running in a join of one build stream: more than one is a pipeline that feeds it from several streams.
    std::atomic<size_t> one_stream_inserts = 0;
    /// The cancel flag of the first processor that added a block. The pipeline cancels every processor that fills the join,
    /// so this one flag stops the whole build, the one in `finishBuild` too. Null without a processor.
    std::atomic<const std::atomic<bool> *> build_cancellation_flag = nullptr;
    /// The right-side rows the build read, the dropped blocks included, for the statistics cache.
    std::atomic<size_t> rows_read_by_build = 0;

public:
    /// For `GraceHashJoin` buckets: the join estimates its keys from the first block, because grace reads the key count to
    /// decide whether a bucket can be split. Before the first block.
    void estimateKeysFromFirstBlock() { sketch_from_first_block = true; }
};
}
