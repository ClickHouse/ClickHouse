#pragma once

#include <shared_mutex>

#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/AggregationCommon.h>
#include <Interpreters/SettingsCommon.h>

#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/// Helpers to obtain keys (to use in a hash table or similar data structure) for various equi-JOINs.

/// UInt8/16/32/64 or another types with same number of bits.
template <typename FieldType>
struct JoinKeyGetterOneNumber
{
    using Key = FieldType;

    const FieldType * vec;

    /** Created before processing of each block.
      * Initialize some members, used in another methods, called in inner loops.
      */
    JoinKeyGetterOneNumber(const ColumnRawPtrs & key_columns)
    {
        vec = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
    }

    Key getKey(
        const ColumnRawPtrs & /*key_columns*/,
        size_t /*keys_size*/,                 /// number of key columns.
        size_t i,                             /// row number to get key from.
        const Sizes & /*key_sizes*/) const    /// If keys are of fixed size - their sizes. Not used for methods with variable-length keys.
    {
        return unionCastToUInt64(vec[i]);
    }

    /// Place additional data into memory pool, if needed, when new key was inserted into hash table.
    static void onNewKey(Key & /*key*/, Arena & /*pool*/) {}
};

/// For single String key.
struct JoinKeyGetterString
{
    using Key = StringRef;

    const ColumnString::Offsets * offsets;
    const ColumnString::Chars_t * chars;

    JoinKeyGetterString(const ColumnRawPtrs & key_columns)
    {
        const IColumn & column = *key_columns[0];
        const ColumnString & column_string = static_cast<const ColumnString &>(column);
        offsets = &column_string.getOffsets();
        chars = &column_string.getChars();
    }

    Key getKey(
        const ColumnRawPtrs &,
        size_t,
        size_t i,
        const Sizes &) const
    {
        return StringRef(
            &(*chars)[i == 0 ? 0 : (*offsets)[i - 1]],
            (i == 0 ? (*offsets)[i] : ((*offsets)[i] - (*offsets)[i - 1])) - 1);
    }

    static void onNewKey(Key & key, Arena & pool)
    {
        key.data = pool.insert(key.data, key.size);
    }
};

/// For single FixedString key.
struct JoinKeyGetterFixedString
{
    using Key = StringRef;

    size_t n;
    const ColumnFixedString::Chars_t * chars;

    JoinKeyGetterFixedString(const ColumnRawPtrs & key_columns)
    {
        const IColumn & column = *key_columns[0];
        const ColumnFixedString & column_string = static_cast<const ColumnFixedString &>(column);
        n = column_string.getN();
        chars = &column_string.getChars();
    }

    Key getKey(
        const ColumnRawPtrs &,
        size_t,
        size_t i,
        const Sizes &) const
    {
        return StringRef(&(*chars)[i * n], n);
    }

    static void onNewKey(Key & key, Arena & pool)
    {
        key.data = pool.insert(key.data, key.size);
    }
};

/// For keys of fixed size, that could be packed in sizeof TKey width.
template <typename TKey>
struct JoinKeyGetterFixed
{
    using Key = TKey;

    JoinKeyGetterFixed(const ColumnRawPtrs &)
    {
    }

    Key getKey(
        const ColumnRawPtrs & key_columns,
        size_t keys_size,
        size_t i,
        const Sizes & key_sizes) const
    {
        return packFixed<Key>(i, keys_size, key_columns, key_sizes);
    }

    static void onNewKey(Key &, Arena &) {}
};

/// Generic method, use crypto hash function.
struct JoinKeyGetterHashed
{
    using Key = UInt128;

    JoinKeyGetterHashed(const ColumnRawPtrs &)
    {
    }

    Key getKey(
        const ColumnRawPtrs & key_columns,
        size_t keys_size,
        size_t i,
        const Sizes &) const
    {
        return hash128(i, keys_size, key_columns);
    }

    static void onNewKey(Key &, Arena &) {}
};



struct Limits;


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
class Join
{
public:
    Join(const Names & key_names_left_, const Names & key_names_right_, bool use_nulls_,
         const Limits & limits, ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_);

    bool empty() { return type == Type::EMPTY; }

    /** Set information about structure of right hand of JOIN (joined data).
      * You must call this method before subsequent calls to insertFromBlock.
      */
    void setSampleBlock(const Block & block);

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool insertFromBlock(const Block & block);

    /** Join data from the map (that was previously built by calls to insertFromBlock) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    void joinBlock(Block & block) const;

    /** Keep "totals" (separate part of dataset, see WITH TOTALS) to use later.
      */
    void setTotals(const Block & block) { totals = block; }
    bool hasTotals() const { return totals; };

    void joinTotals(Block & block) const;

    /** For RIGHT and FULL JOINs.
      * A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
      * Use only after all calls to joinBlock was done.
      * left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
      */
    BlockInputStreamPtr createStreamWithNonJoinedRows(const Block & left_sample_block, size_t max_block_size) const;

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const;

    ASTTableJoin::Kind getKind() const { return kind; }


    /// Reference to the row in block.
    struct RowRef
    {
        const Block * block;
        size_t row_num;

        RowRef() {}
        RowRef(const Block * block_, size_t row_num_) : block(block_), row_num(row_num_) {}
    };

    /// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
    struct RowRefList : RowRef
    {
        RowRefList * next = nullptr;

        RowRefList() {}
        RowRefList(const Block * block_, size_t row_num_) : RowRef(block_, row_num_) {}
    };


    /** Depending on template parameter, adds or doesn't add a flag, that element was used (row was joined).
      * For implementation of RIGHT and FULL JOINs.
      * NOTE: It is possible to store the flag in one bit of pointer to block or row_num. It seems not reasonable, because memory saving is minimal.
      */
    template <bool enable, typename Base>
    struct WithUsedFlag;

    template <typename Base>
    struct WithUsedFlag<true, Base> : Base
    {
        mutable std::atomic<bool> used {};
        using Base::Base;
        using Base_t = Base;
        void setUsed() const { used.store(true, std::memory_order_relaxed); }    /// Could be set simultaneously from different threads.
        bool getUsed() const { return used; }
    };

    template <typename Base>
    struct WithUsedFlag<false, Base> : Base
    {
        using Base::Base;
        using Base_t = Base;
        void setUsed() const {}
        bool getUsed() const { return true; }
    };


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
        std::unique_ptr<HashMap<UInt8, Mapped, TrivialHash, HashTableFixedGrower<8>>>   key8;
        std::unique_ptr<HashMap<UInt16, Mapped, TrivialHash, HashTableFixedGrower<16>>> key16;
        std::unique_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>>                     key32;
        std::unique_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>>                     key64;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>>                        key_string;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>>                        key_fixed_string;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>>                     keys128;
        std::unique_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>>                     keys256;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>>                   hashed;
    };

    using MapsAny = MapsTemplate<WithUsedFlag<false, RowRef>>;
    using MapsAll = MapsTemplate<WithUsedFlag<false, RowRefList>>;
    using MapsAnyFull = MapsTemplate<WithUsedFlag<true, RowRef>>;
    using MapsAllFull = MapsTemplate<WithUsedFlag<true, RowRefList>>;

private:
    friend class NonJoinedBlockInputStream;

    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    /// Names of key columns (columns for equi-JOIN) in "left" table (in the order they appear in USING clause).
    const Names key_names_left;
    /// Names of key columns (columns for equi-JOIN) in "right" table (in the order they appear in USING clause).
    const Names key_names_right;

    /// Substitute NULLs for non-JOINed rows.
    bool use_nulls;

    /** Blocks of "right" table.
      */
    BlocksList blocks;

    MapsAny maps_any;            /// For ANY LEFT|INNER JOIN
    MapsAll maps_all;            /// For ALL LEFT|INNER JOIN
    MapsAnyFull maps_any_full;    /// For ANY RIGHT|FULL JOIN
    MapsAllFull maps_all_full;    /// For ALL RIGHT|FULL JOIN

    /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
    Arena pool;

private:
    Type type = Type::EMPTY;

    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    Sizes key_sizes;

    /// Block with columns from the right-side table except key columns.
    Block sample_block_with_columns_to_add;
    /// Block with key columns in the same order they appear in the right-side table.
    Block sample_block_with_keys;

    Poco::Logger * log;

    /// Limits for maximum map size.
    size_t max_rows;
    size_t max_bytes;
    OverflowMode overflow_mode;

    Block totals;

    /** Protect state for concurrent use in insertFromBlock and joinBlock.
      * Note that these methods could be called simultaneously only while use of StorageJoin,
      *  and StorageJoin only calls these two methods.
      * That's why another methods are not guarded.
      */
    mutable std::shared_mutex rwlock;

    void init(Type type_);

    bool checkSizeLimits() const;

    /// Throw an exception if blocks have different types of key columns.
    void checkTypesOfKeys(const Block & block_left, const Block & block_right) const;

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(Block & block, const Maps & maps) const;

    void joinBlockImplCross(Block & block) const;
};

using JoinPtr = std::shared_ptr<Join>;
using Joins = std::vector<JoinPtr>;


}
