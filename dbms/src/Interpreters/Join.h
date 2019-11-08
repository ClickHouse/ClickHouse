#pragma once

#include <variant>
#include <optional>
#include <shared_mutex>
#include <deque>

#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/IJoin.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/RowRefs.h>
#include <Core/SettingsCommon.h>

#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/FixedHashMap.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <DataStreams/SizeLimits.h>
#include <DataStreams/IBlockStream_fwd.h>


namespace DB
{

class AnalyzedJoin;

namespace JoinStuff
{

/// Base class with optional flag attached that's needed to implement RIGHT and FULL JOINs.
template <typename T, bool with_used>
struct WithFlags;

template <typename T>
struct WithFlags<T, true> : T
{
    using Base = T;
    using T::T;

    mutable std::atomic<bool> used {};
    void setUsed() const { used.store(true, std::memory_order_relaxed); }    /// Could be set simultaneously from different threads.
    bool getUsed() const { return used; }
};

template <typename T>
struct WithFlags<T, false> : T
{
    using Base = T;
    using T::T;

    void setUsed() const {}
    bool getUsed() const { return true; }
};

using MappedAny =       WithFlags<RowRef, false>;
using MappedAll =       WithFlags<RowRefList, false>;
using MappedAnyFull =   WithFlags<RowRef, true>;
using MappedAllFull =   WithFlags<RowRefList, true>;
using MappedAsof =      WithFlags<AsofRowRefs, false>;

}

/** Data structure for implementation of JOIN.
  * It is just a hash table: keys -> rows of joined ("right") table.
  * Additionally, CROSS JOIN is supported: instead of hash table, it use just set of blocks without keys.
  *
  * JOIN-s could be of nine types: ANY/ALL × LEFT/INNER/RIGHT/FULL, and also CROSS.
  *
  * If ANY is specified - then select only one row from the "right" table, (first encountered row), even if there was more matching rows.
  * If ALL is specified - usual JOIN, when rows are multiplied by number of matching rows from the "right" table.
  * ANY is more efficient.
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
class Join : public IJoin
{
public:
    Join(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block, bool any_take_last_row_ = false);

    bool empty() { return type == Type::EMPTY; }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool addJoinedBlock(const Block & block) override;

    /** Join data from the map (that was previously built by calls to addJoinedBlock) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    void joinBlock(Block & block) override;

    /// Infer the return type for joinGet function
    DataTypePtr joinGetReturnType(const String & column_name) const;

    /// Used by joinGet function that turns StorageJoin into a dictionary
    void joinGet(Block & block, const String & column_name) const;

    /** Keep "totals" (separate part of dataset, see WITH TOTALS) to use later.
      */
    void setTotals(const Block & block) override { totals = block; }
    bool hasTotals() const override { return totals; }

    void joinTotals(Block & block) const override;

    /** For RIGHT and FULL JOINs.
      * A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
      * Use only after all calls to joinBlock was done.
      * left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
      */
    BlockInputStreamPtr createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size) const override;

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const final;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const;

    bool alwaysReturnsEmptySet() const final { return isInnerOrRight(getKind()) && has_no_rows_in_maps; }

    ASTTableJoin::Kind getKind() const { return kind; }
    ASTTableJoin::Strictness getStrictness() const { return strictness; }
    AsofRowRefs::Type getAsofType() const { return *asof_type; }
    ASOF::Inequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const { return any_take_last_row; }

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
        #define M(NAME) NAME,
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
    };


    /** Different data structures, that are used to perform JOIN.
      */
    template <typename Mapped>
    struct MapsTemplate
    {
        std::unique_ptr<FixedHashMap<UInt8, Mapped>>   key8;
        std::unique_ptr<FixedHashMap<UInt16, Mapped>> key16;
        std::unique_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>>                     key32;
        std::unique_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>>                     key64;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>>                        key_string;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>>                        key_fixed_string;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>>                     keys128;
        std::unique_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>>                     keys256;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>>                   hashed;

        void create(Type which)
        {
            switch (which)
            {
                case Type::EMPTY:            break;
                case Type::CROSS:            break;

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

            #define M(NAME) \
                case Type::NAME: return NAME ? NAME->getBufferSizeInBytes() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
            #undef M
            }

            __builtin_unreachable();
        }
    };

    using MapsAny =             MapsTemplate<JoinStuff::MappedAny>;
    using MapsAll =             MapsTemplate<JoinStuff::MappedAll>;
    using MapsAnyFull =         MapsTemplate<JoinStuff::MappedAnyFull>;
    using MapsAllFull =         MapsTemplate<JoinStuff::MappedAllFull>;
    using MapsAsof =            MapsTemplate<JoinStuff::MappedAsof>;

    using MapsVariant = std::variant<MapsAny, MapsAll, MapsAnyFull, MapsAllFull, MapsAsof>;

private:
    friend class NonJoinedBlockInputStream;
    friend class JoinBlockInputStream;

    std::shared_ptr<AnalyzedJoin> table_join;
    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    /// Names of key columns in right-side table (in the order they appear in ON/USING clause). @note It could contain duplicates.
    const Names & key_names_right;

    /// In case of LEFT and FULL joins, if use_nulls, convert right-side columns to Nullable.
    bool nullable_right_side;
    /// In case of RIGHT and FULL joins, if use_nulls, convert left-side columns to Nullable.
    bool nullable_left_side;

    /// Overwrite existing values when encountering the same key again
    bool any_take_last_row;

    /// Blocks of "right" table.
    BlocksList blocks;

    /// Nullmaps for blocks of "right" table (if needed)
    using BlockNullmapList = std::deque<std::pair<const Block *, ColumnPtr>>;
    BlockNullmapList blocks_nullmaps;

    MapsVariant maps;
    bool has_no_rows_in_maps = true;

    /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
    Arena pool;

    Type type = Type::EMPTY;
    std::optional<AsofRowRefs::Type> asof_type;
    ASOF::Inequality asof_inequality;

    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    Sizes key_sizes;

    /// Block with columns from the right-side table except key columns.
    Block sample_block_with_columns_to_add;
    /// Block with key columns in the same order they appear in the right-side table (duplicates appear once).
    Block right_table_keys;
    /// Block with key columns right-side table keys that are needed in result (would be attached after joined columns).
    Block required_right_keys;
    /// Left table column names that are sources for required_right_keys columns
    std::vector<String> required_right_keys_sources;

    /// Block as it would appear in the BlockList
    Block saved_block_sample;

    Poco::Logger * log;

    Block totals;

    /** Protect state for concurrent use in insertFromBlock and joinBlock.
      * Note that these methods could be called simultaneously only while use of StorageJoin,
      *  and StorageJoin only calls these two methods.
      * That's why another methods are not guarded.
      */
    mutable std::shared_mutex rwlock;

    void init(Type type_);

    /** Set information about structure of right hand of JOIN (joined data).
      */
    void setSampleBlock(const Block & block);

    /// Modify (structure) and save right block, @returns pointer to saved block
    Block * storeRightBlock(const Block & stored_block);
    void initRightBlockStructure();
    void initRequiredRightKeys();

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(
        Block & block,
        const Names & key_names_left,
        const Block & block_with_columns_to_add,
        const Maps & maps) const;

    void joinBlockImplCross(Block & block) const;

    template <typename Maps>
    void joinGetImpl(Block & block, const String & column_name, const Maps & maps) const;
};

}
