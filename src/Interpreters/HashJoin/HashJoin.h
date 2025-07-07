#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/IJoin.h>
#include <Interpreters/RowRefs.h>

#include <Core/Block_fwd.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableTraits.h>
#include <Common/HashTable/TwoLevelHashMap.h>

namespace DB
{

class TableJoin;
class ExpressionActions;
using Sizes = std::vector<size_t>;

namespace JoinStuff
{
/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
class HashJoinMethods;

/** Data structure for implementation of JOIN.
  * It is just a hash table: keys -> rows of joined ("right") table.
  * Additionally, CROSS JOIN is supported: instead of hash table, it use just set of blocks without keys.
  *
  * JOIN-s could be of these types:
  * - ALL × LEFT/INNER/RIGHT/FULL
  * - ANY × LEFT/INNER/RIGHT
  * - SEMI/ANTI x LEFT/RIGHT
  * - ASOF x LEFT/INNER
  * - CROSS
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
  * All JOINs (except CROSS) are done by equality condition on keys (equijoin).
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
        const Block & right_sample_block,
        bool any_take_last_row_ = false,
        size_t reserve_num_ = 0,
        const String & instance_id_ = "",
        bool use_two_level_maps_ = false);

    ~HashJoin() override;

    std::string getName() const override { return "HashJoin"; }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return !getTotals() && getTotalRowCount() == 0;
    }

    std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_,
        const Block &,
        const Block & right_sample_block_) const override
    {
        return std::make_shared<HashJoin>(table_join_, right_sample_block_, any_take_last_row, reserve_num, instance_id);
    }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool addBlockToJoin(const Block & source_block_, bool check_limits) override;

    /// Called directly from ConcurrentJoin::addBlockToJoin
    bool addBlockToJoin(ScatteredBlock & source_block_, bool check_limits);

    void checkTypesOfKeys(const Block & block) const override;

    using IJoin::joinBlock;

    /** Join data from the map (that was previously built by calls to addBlockToJoin) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    void joinBlock(Block & block, ExtraBlockPtr & not_processed) override;

    /// Called directly from ConcurrentJoin::joinBlock
    void joinBlock(ScatteredBlock & block, ScatteredBlock & remaining_block);

    /// Check joinGet arguments and infer the return type.
    DataTypePtr joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const;

    /// Used by joinGet function that turns StorageJoin into a dictionary.
    ColumnWithTypeAndName joinGet(const Block & block, const Block & block_with_columns_to_add) const;

    bool isFilled() const override { return from_storage_join; }

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

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const final;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const final;

    bool alwaysReturnsEmptySet() const final;

    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOFJoinInequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const { return any_take_last_row; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const;

    /// Different types of keys for maps.
    #define APPLY_FOR_JOIN_VARIANTS(M) \
        M(key8)                        \
        M(key16)                       \
        M(key32)                       \
        M(key64)                       \
        M(key_string)                  \
        M(key_fixed_string)            \
        M(keys128)                     \
        M(keys256)                     \
        M(hashed)                      \
        M(two_level_key32)             \
        M(two_level_key64)             \
        M(two_level_key_string)        \
        M(two_level_key_fixed_string)  \
        M(two_level_keys128)           \
        M(two_level_keys256)           \
        M(two_level_hashed)

    /// Used for reading from StorageJoin and applying joinGet function
    #define APPLY_FOR_JOIN_VARIANTS_LIMITED(M) \
        M(key8)                                \
        M(key16)                               \
        M(key32)                               \
        M(key64)                               \
        M(key_string)                          \
        M(key_fixed_string)

    /// Used in ConcurrentHashJoin
    #define APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M, ...)           \
        M(two_level_key32 __VA_OPT__(,) __VA_ARGS__)            \
        M(two_level_key64 __VA_OPT__(,) __VA_ARGS__)            \
        M(two_level_key_string __VA_OPT__(,) __VA_ARGS__)       \
        M(two_level_key_fixed_string __VA_OPT__(,) __VA_ARGS__) \
        M(two_level_keys128 __VA_OPT__(,) __VA_ARGS__)          \
        M(two_level_keys256 __VA_OPT__(,) __VA_ARGS__)          \
        M(two_level_hashed __VA_OPT__(,) __VA_ARGS__)

    enum class Type : uint8_t
    {
        EMPTY,
        CROSS,
        #define M(NAME) NAME,
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
    };

    bool twoLevelMapIsUsed() const
    {
        switch (data->type)
        {
        #define M(NAME) \
            case Type::NAME: \
                return true;

            APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M)
        #undef M

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
        std::shared_ptr<FixedHashMap<UInt8, Mapped>>                          key8;
        std::shared_ptr<FixedHashMap<UInt16, Mapped>>                         key16;
        std::shared_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>>           key32;
        std::shared_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>>           key64;
        std::shared_ptr<HashMapWithSavedHash<StringRef, Mapped>>              key_string;
        std::shared_ptr<HashMapWithSavedHash<StringRef, Mapped>>              key_fixed_string;
        std::shared_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>>           keys128;
        std::shared_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>>           keys256;
        std::shared_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>>         hashed;
        std::shared_ptr<TwoLevelHashMap<UInt32, Mapped, HashCRC32<UInt32>>>   two_level_key32;
        std::shared_ptr<TwoLevelHashMap<UInt64, Mapped, HashCRC32<UInt64>>>   two_level_key64;
        std::shared_ptr<TwoLevelHashMapWithSavedHash<StringRef, Mapped>>      two_level_key_string;
        std::shared_ptr<TwoLevelHashMapWithSavedHash<StringRef, Mapped>>      two_level_key_fixed_string;
        std::shared_ptr<TwoLevelHashMap<UInt128, Mapped, UInt128HashCRC32>>   two_level_keys128;
        std::shared_ptr<TwoLevelHashMap<UInt256, Mapped, UInt256HashCRC32>>   two_level_keys256;
        std::shared_ptr<TwoLevelHashMap<UInt128, Mapped, UInt128TrivialHash>> two_level_hashed;

        void create(Type which, size_t reserve)
        {
            switch (which)
            {
            #define M(NAME)                                                                                       \
                case Type::NAME:                                                                                  \
                    if constexpr (HasConstructorOfNumberOfElements<typename decltype(NAME)::element_type>::value) \
                        NAME = reserve ? std::make_shared<typename decltype(NAME)::element_type>(reserve)         \
                                       : std::make_shared<typename decltype(NAME)::element_type>();               \
                    else                                                                                          \
                        NAME = std::make_shared<typename decltype(NAME)::element_type>();                         \
                    break;

                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M

                default:
                    break;
            }
        }

        size_t getTotalRowCount(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:            return 0;
                case Type::CROSS:            return 0;

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
                case Type::EMPTY:            return 0;
                case Type::CROSS:            return 0;

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
                case Type::EMPTY:            return 0;
                case Type::CROSS:            return 0;

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

    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof>;

    struct ScatteredColumns
    {
        Columns columns;
        ScatteredBlock::Selector selector;

        size_t allocatedBytes() const;
    };

    struct NullMapHolder
    {
        const ScatteredColumns * columns;
        ColumnPtr column;

        size_t allocatedBytes() const;
    };

    using NullmapList = std::deque<NullMapHolder>;
    using ScatteredColumnsList = std::list<ScatteredColumns>;

    struct RightTableData
    {
        Type type = Type::EMPTY;
        bool empty = true;

        /// tab1 join tab2 on t1.x = t2.x or t1.y = t2.y
        /// =>
        /// tab1 join tab2 on t1.x = t2.x
        /// join tab2 on [not_joined(t1.x = t2.x)] and t1.y = t2.y
        std::vector<MapsVariant> maps;
        Block sample_block; /// Block as it would appear in the BlockList
        ScatteredColumnsList columns; /// Columns of "right" table.
        NullmapList nullmaps; /// Nullmaps for blocks of "right" table (if needed)

        /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
        Arena pool;

        size_t allocated_size = 0;
        size_t nullmaps_allocated_size = 0;
        /// Number of rows of right table to join
        size_t rows_to_join = 0;
        /// Number of keys of right table to join
        size_t keys_to_join = 0;
        /// Whether the right table reranged by key
        bool sorted = false;

        size_t avgPerKeyRows() const
        {
            if (keys_to_join == 0)
                return 0;
            return rows_to_join / keys_to_join;
        }
    };

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

    /// Modify right block (update structure according to sample block) to save it in block list
    static Block prepareRightBlock(const Block & block, const Block & saved_block_sample_);
    Block prepareRightBlock(const Block & block) const;

    const Block & savedBlockSample() const { return data->sample_block; }

    bool isUsed(size_t off) const;
    bool isUsed(const Columns * columns_ptr, size_t row_idx) const;

    void debugKeys() const;

    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize = false);

    void setMaxJoinedBlockRows(size_t value) { max_joined_block_rows = value; }

    void materializeColumnsFromLeftBlock(Block & block) const;
    Block materializeColumnsFromRightBlock(Block block) const;

    bool rightTableCanBeReranged() const override;
    void tryRerangeRightTableData() override;
    size_t getAndSetRightTableKeys() const;

    const std::vector<Sizes> & getKeySizes() const { return key_sizes; }

    std::shared_ptr<JoinStuff::JoinUsedFlags> getUsedFlags() const { return used_flags; }

private:
    friend class NotJoinedHash;

    friend class JoinSource;

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
    friend class HashJoinMethods;

    std::shared_ptr<TableJoin> table_join;
    const JoinKind kind;
    const JoinStrictness strictness;

    /// This join was created from StorageJoin and it is already filled.
    bool from_storage_join = false;

    const bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    const size_t reserve_num;
    const String instance_id;
    std::optional<TypeIndex> asof_type;
    const ASOFJoinInequality asof_inequality;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// so we must guarantee constantness of hash table during HashJoin lifetime (using method setLock)
    mutable std::shared_ptr<JoinStuff::JoinUsedFlags> used_flags;
    RightTableDataPtr data;
    bool have_compressed = false;

    std::vector<Sizes> key_sizes;

    /// Needed to do external cross join
    TemporaryDataOnDiskScopePtr tmp_data;
    std::optional<TemporaryBlockStreamHolder> tmp_stream;
    mutable std::once_flag finish_writing;

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

    /// When tracked memory consumption is more than a threshold, we will shrink to fit stored blocks.
    bool shrink_blocks = false;
    Int64 memory_usage_before_adding_blocks = 0;

    /// Identifier to distinguish different HashJoin instances in logs
    /// Several instances can be created, for example, in GraceHashJoin to handle different buckets
    String instance_log_id;

    LoggerPtr log;

    /// Should be set via setLock to protect hash table from modification from StorageJoin
    /// If set HashJoin instance is not available for modification (addBlockToJoin)
    TableLockHolder storage_join_lock = nullptr;

    void dataMapInit(MapsVariant & map);

    void initRightBlockStructure(Block & saved_block_sample);

    void joinBlockImplCross(Block & block, ExtraBlockPtr & not_processed) const;

    bool empty() const;

    bool isUsedByAnotherAlgorithm() const;
    bool canRemoveColumnsFromLeftBlock() const;

    void validateAdditionalFilterExpression(std::shared_ptr<ExpressionActions> additional_filter_expression);
    bool needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const;

    template <JoinKind KIND, typename Map, JoinStrictness STRICTNESS>
    void tryRerangeRightTableDataImpl(Map & map);
    void doDebugAsserts() const;
};
}
