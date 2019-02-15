#pragma once

#include <shared_mutex>

#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/AggregationCommon.h>
#include <Interpreters/SettingsCommon.h>

#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/HashMap.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <DataStreams/SizeLimits.h>
#include <DataStreams/IBlockInputStream.h>
#include <variant>
#include <common/constexpr_helpers.h>


namespace DB
{
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
    Join(const Names & key_names_right_, bool use_nulls_, const SizeLimits & limits,
         ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_, bool any_take_last_row_ = false);

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
    void joinBlock(Block & block, const Names & key_names_left, const NamesAndTypesList & columns_added_by_join) const;

    /// Infer the return type for joinGet function
    DataTypePtr joinGetReturnType(const String & column_name) const;

    /// Used by joinGet function that turns StorageJoin into a dictionary
    void joinGet(Block & block, const String & column_name) const;

    /** Keep "totals" (separate part of dataset, see WITH TOTALS) to use later.
      */
    void setTotals(const Block & block) { totals = block; }
    bool hasTotals() const { return totals; }

    void joinTotals(Block & block) const;

    /** For RIGHT and FULL JOINs.
      * A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
      * Use only after all calls to joinBlock was done.
      * left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
      */
    BlockInputStreamPtr createStreamWithNonJoinedRows(const Block & left_sample_block, const Names & key_names_left,
                                                      const NamesAndTypesList & columns_added_by_join, UInt64 max_block_size) const;

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
      * Depending on template parameter, decide whether to overwrite existing values when encountering the same key again
      * with_used is for implementation of RIGHT and FULL JOINs.
      * overwrite is for implementation of StorageJoin with overwrite setting enabled
      * NOTE: It is possible to store the flag in one bit of pointer to block or row_num. It seems not reasonable, because memory saving is minimal.
      */
    template <bool with_used, bool overwrite_, typename Base>
    struct WithFlags;

    template <bool overwrite_, typename Base>
    struct WithFlags<true, overwrite_, Base> : Base
    {
        static constexpr bool overwrite = overwrite_;
        mutable std::atomic<bool> used {};
        using Base::Base;
        using Base_t = Base;
        void setUsed() const { used.store(true, std::memory_order_relaxed); }    /// Could be set simultaneously from different threads.
        bool getUsed() const { return used; }
    };

    template <bool overwrite_, typename Base>
    struct WithFlags<false, overwrite_, Base> : Base
    {
        static constexpr bool overwrite = overwrite_;
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

    using MapsAny = MapsTemplate<WithFlags<false, false, RowRef>>;
    using MapsAnyOverwrite = MapsTemplate<WithFlags<false, true, RowRef>>;
    using MapsAll = MapsTemplate<WithFlags<false, false, RowRefList>>;
    using MapsAnyFull = MapsTemplate<WithFlags<true, false, RowRef>>;
    using MapsAnyFullOverwrite = MapsTemplate<WithFlags<true, true, RowRef>>;
    using MapsAllFull = MapsTemplate<WithFlags<true, false, RowRefList>>;

    template <ASTTableJoin::Kind KIND>
    struct KindTrait
    {
        // Affects the Adder trait so that when the right part is empty, adding a default value on the left
        static constexpr bool fill_left = static_in_v<KIND, ASTTableJoin::Kind::Left, ASTTableJoin::Kind::Full>;

        // Affects the Map trait so that a `used` flag is attached to map slots in order to
        // generate default values on the right when the left part is empty
        static constexpr bool fill_right = static_in_v<KIND, ASTTableJoin::Kind::Right, ASTTableJoin::Kind::Full>;
    };

    template <bool fill_right, typename ASTTableJoin::Strictness, bool overwrite>
    struct MapGetterImpl;

    template <ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness, bool overwrite>
    using Map = typename MapGetterImpl<KindTrait<kind>::fill_right, strictness, overwrite>::Map;

    static constexpr std::array<ASTTableJoin::Strictness, 2> STRICTNESSES = {ASTTableJoin::Strictness::Any, ASTTableJoin::Strictness::All};
    static constexpr std::array<ASTTableJoin::Kind, 4> KINDS
        = {ASTTableJoin::Kind::Left, ASTTableJoin::Kind::Inner, ASTTableJoin::Kind::Full, ASTTableJoin::Kind::Right};

    struct MapInitTag {};

    template <typename Func>
    bool dispatch(Func && func)
    {
        if (any_take_last_row)
        {
            return static_for<0, KINDS.size()>([&](auto i)
            {
                if (kind == KINDS[i] && strictness == ASTTableJoin::Strictness::Any)
                {
                    if constexpr (std::is_same_v<Func, MapInitTag>)
                        maps = Map<KINDS[i], ASTTableJoin::Strictness::Any, true>();
                    else
                        func(
                            std::integral_constant<ASTTableJoin::Kind, KINDS[i]>(),
                            std::integral_constant<ASTTableJoin::Strictness, ASTTableJoin::Strictness::Any>(),
                            std::get<Map<KINDS[i], ASTTableJoin::Strictness::Any, true>>(maps));
                    return true;
                }
                return false;
            });
        }
        else
        {
            return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
            {
                // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
                // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
                constexpr auto i = ij / STRICTNESSES.size();
                constexpr auto j = ij % STRICTNESSES.size();
                if (kind == KINDS[i] && strictness == STRICTNESSES[j])
                {
                    if constexpr (std::is_same_v<Func, MapInitTag>)
                        maps = Map<KINDS[i], STRICTNESSES[j], false>();
                    else
                        func(
                            std::integral_constant<ASTTableJoin::Kind, KINDS[i]>(),
                            std::integral_constant<ASTTableJoin::Strictness, STRICTNESSES[j]>(),
                            std::get<Map<KINDS[i], STRICTNESSES[j], false>>(maps));
                    return true;
                }
                return false;
            });
        }
    }

    template <typename Func>
    bool dispatch(Func && func) const
    {
        return const_cast<Join &>(*this).dispatch(std::forward<Func>(func));
    }

private:
    friend class NonJoinedBlockInputStream;
    friend class JoinBlockInputStream;

    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    /// Names of key columns (columns for equi-JOIN) in "right" table (in the order they appear in USING clause).
    const Names key_names_right;

    /// Substitute NULLs for non-JOINed rows.
    bool use_nulls;

    /// Overwrite existing values when encountering the same key again
    bool any_take_last_row;

    /** Blocks of "right" table.
      */
    BlocksList blocks;

    std::variant<MapsAny, MapsAnyOverwrite, MapsAll, MapsAnyFull, MapsAnyFullOverwrite, MapsAllFull> maps;

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
    SizeLimits limits;

    Block totals;

    /** Protect state for concurrent use in insertFromBlock and joinBlock.
      * Note that these methods could be called simultaneously only while use of StorageJoin,
      *  and StorageJoin only calls these two methods.
      * That's why another methods are not guarded.
      */
    mutable std::shared_mutex rwlock;

    void init(Type type_);

    /// Throw an exception if blocks have different types of key columns.
    void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right) const;

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(
        Block & block,
        const Names & key_names_left,
        const NamesAndTypesList & columns_added_by_join,
        const Block & block_with_columns_to_add,
        const Maps & maps) const;

    void joinBlockImplCross(Block & block) const;

    template <typename Maps>
    void joinGetImpl(Block & block, const String & column_name, const Maps & maps) const;
};

using JoinPtr = std::shared_ptr<Join>;
using Joins = std::vector<JoinPtr>;

template <bool overwrite_>
struct Join::MapGetterImpl<false, ASTTableJoin::Strictness::Any, overwrite_>
{
    using Map = std::conditional_t<overwrite_, MapsAnyOverwrite, MapsAny>;
};

template <bool overwrite_>
struct Join::MapGetterImpl<true, ASTTableJoin::Strictness::Any, overwrite_>
{
    using Map = std::conditional_t<overwrite_, MapsAnyFullOverwrite, MapsAnyFull>;
};

template <>
struct Join::MapGetterImpl<false, ASTTableJoin::Strictness::All, false>
{
    using Map = MapsAll;
};

template <>
struct Join::MapGetterImpl<true, ASTTableJoin::Strictness::All, false>
{
    using Map = MapsAllFull;
};

}
