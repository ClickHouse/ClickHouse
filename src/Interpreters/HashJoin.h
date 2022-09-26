#pragma once

#include <variant>
#include <optional>
#include <shared_mutex>
#include <deque>
#include <vector>

#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/IJoin.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/RowRefs.h>

#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Storages/TableLockHolder.h>
#include <Common/logger_useful.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <QueryPipeline/SizeLimits.h>

#include <Core/Block.h>

#include <Storages/IStorage_fwd.h>
#include <Storages/IKVStorage.h>

namespace DB
{

class TableJoin;
class DictionaryReader;

namespace JoinStuff
{

/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags
{
    using RawBlockPtr = const Block *;
    using UsedFlagsForBlock = std::vector<std::atomic_bool>;

    /// For multiple dijuncts each empty in hashmap stores flags for particular block
    /// For single dicunct we store all flags in `nullptr` entry, index is the offset in FindResult
    std::unordered_map<RawBlockPtr, UsedFlagsForBlock> flags;

    bool need_flags;

public:
    /// Update size for vector with flags.
    /// Calling this method invalidates existing flags.
    /// It can be called several times, but all of them should happen before using this structure.
    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
    void reinit(size_t size_);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
    void reinit(const Block * block_ptr);

    bool getUsedSafe(size_t i) const;
    bool getUsedSafe(const Block * block_ptr, size_t row_idx) const;

    template <bool use_flags, bool multiple_disjuncts, typename T>
    void setUsed(const T & f);

    template <bool use_flags, bool multiple_disjunct>
    void setUsed(const Block * block, size_t row_num, size_t offset);

    template <bool use_flags, bool multiple_disjuncts, typename T>
    bool getUsed(const T & f);

    template <bool use_flags, bool multiple_disjuncts, typename T>
    bool setUsedOnce(const T & f);
};

}

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
    HashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block, bool any_take_last_row_ = false);

    const TableJoin & getTableJoin() const override { return *table_join; }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool addJoinedBlock(const Block & block, bool check_limits) override;

    void checkTypesOfKeys(const Block & block) const override;

    /** Join data from the map (that was previously built by calls to addJoinedBlock) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    void joinBlock(Block & block, ExtraBlockPtr & not_processed) override;

    /// Check joinGet arguments and infer the return type.
    DataTypePtr joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const;

    /// Used by joinGet function that turns StorageJoin into a dictionary.
    ColumnWithTypeAndName joinGet(const Block & block, const Block & block_with_columns_to_add) const;

    bool isFilled() const override { return from_storage_join || data->type == Type::DICT; }

    JoinPipelineType pipelineType() const override
    {
        /// No need to process anything in the right stream if it's a dictionary will just join the left stream with it.
        bool is_filled = from_storage_join || data->type == Type::DICT;
        if (is_filled)
            return JoinPipelineType::FilledRight;

        /// Default pipeline processes right stream at first and then left.
        return JoinPipelineType::FillRightFirst;
    }

    /** For RIGHT and FULL JOINs.
      * A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
      * Use only after all calls to joinBlock was done.
      * left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
      */
    std::shared_ptr<NotJoinedBlocks> getNonJoinedBlocks(
        const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const final;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const final;

    bool alwaysReturnsEmptySet() const final;

    ASTTableJoin::Kind getKind() const { return kind; }
    ASTTableJoin::Strictness getStrictness() const { return strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOF::Inequality getAsofInequality() const { return asof_inequality; }
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
        M(hashed)


    /// Used for reading from StorageJoin and applying joinGet function
    #define APPLY_FOR_JOIN_VARIANTS_LIMITED(M) \
        M(key8)                                \
        M(key16)                               \
        M(key32)                               \
        M(key64)                               \
        M(key_string)                          \
        M(key_fixed_string)

    enum class Type
    {
        EMPTY,
        CROSS,
        DICT,
        #define M(NAME) NAME,
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
    };

    /** Different data structures, that are used to perform JOIN.
      */
    template <typename Mapped>
    struct MapsTemplate
    {
        using MappedType = Mapped;
        std::unique_ptr<FixedHashMap<UInt8, Mapped>>                  key8;
        std::unique_ptr<FixedHashMap<UInt16, Mapped>>                 key16;
        std::unique_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>>   key32;
        std::unique_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>>   key64;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>>      key_string;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>>      key_fixed_string;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>>   keys128;
        std::unique_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>>   keys256;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>> hashed;

        void create(Type which)
        {
            switch (which)
            {
                case Type::EMPTY:            break;
                case Type::CROSS:            break;
                case Type::DICT:             break;

            #define M(NAME) \
                case Type::NAME: NAME = std::make_unique<typename decltype(NAME)::element_type>(); break;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }
        }

        size_t getTotalRowCount(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:            return 0;
                case Type::CROSS:            return 0;
                case Type::DICT:             return 0;

            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->size() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }

            __builtin_unreachable();
        }

        size_t getTotalByteCountImpl(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:            return 0;
                case Type::CROSS:            return 0;
                case Type::DICT:             return 0;

            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->getBufferSizeInBytes() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }

            __builtin_unreachable();
        }

        size_t getBufferSizeInCells(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:            return 0;
                case Type::CROSS:            return 0;
                case Type::DICT:             return 0;

            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->getBufferSizeInCells() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }

            __builtin_unreachable();
        }
    };

    using MapsOne = MapsTemplate<RowRef>;
    using MapsAll = MapsTemplate<RowRefList>;
    using MapsAsof = MapsTemplate<AsofRowRefs>;

    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof>;

    using RawBlockPtr = const Block *;
    using BlockNullmapList = std::deque<std::pair<RawBlockPtr, ColumnPtr>>;

    struct RightTableData
    {
        Type type = Type::EMPTY;
        bool empty = true;

        std::vector<MapsVariant> maps;
        Block sample_block; /// Block as it would appear in the BlockList
        BlocksList blocks; /// Blocks of "right" table.
        BlockNullmapList blocks_nullmaps; /// Nullmaps for blocks of "right" table (if needed)

        /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
        Arena pool;
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

    bool isUsed(size_t off) const { return used_flags.getUsedSafe(off); }
    bool isUsed(const Block * block_ptr, size_t row_idx) const { return used_flags.getUsedSafe(block_ptr, row_idx); }

private:
    template<bool> friend class NotJoinedHash;

    friend class JoinSource;

    std::shared_ptr<TableJoin> table_join;
    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    /// This join was created from StorageJoin and it is already filled.
    bool from_storage_join = false;

    bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    std::optional<TypeIndex> asof_type;
    ASOF::Inequality asof_inequality;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// so we must guarantee constantness of hash table during HashJoin lifetime (using method setLock)
    mutable JoinStuff::JoinUsedFlags used_flags;
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

    Poco::Logger * log;

    /// Should be set via setLock to protect hash table from modification from StorageJoin
    /// If set HashJoin instance is not available for modification (addJoinedBlock)
    TableLockHolder storage_join_lock = nullptr;

    void dataMapInit(MapsVariant &);

    const Block & savedBlockSample() const { return data->sample_block; }

    /// Modify (structure) right block to save it in block list
    Block structureRightBlock(const Block & stored_block) const;
    void initRightBlockStructure(Block & saved_block_sample);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(
        Block & block,
        const Block & block_with_columns_to_add,
        const std::vector<const Maps *> & maps_,
        bool is_join_get = false) const;

    void joinBlockImplCross(Block & block, ExtraBlockPtr & not_processed) const;

    static Type chooseMethod(ASTTableJoin::Kind kind, const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    bool empty() const;
    bool overDictionary() const;
};

}
