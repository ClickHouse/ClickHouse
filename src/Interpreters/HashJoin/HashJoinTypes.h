#pragma once

#include <atomic>
#include <deque>
#include <list>
#include <memory>
#include <type_traits>
#include <variant>
#include <vector>

#include <Core/Block.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/RowRefs.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>

namespace DB
{

using Sizes = std::vector<size_t>;

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

/// Holds the types and static helpers of `HashJoin`, so headers included before `HashJoin` can name them.
struct HashJoinTypes
{
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
        /// The maps and the arena.
        std::atomic<size_t> maps_bytes = 0;

        /// Exact `allocated_size + nullmaps_allocated_size + maps_bytes`. The three parts are
        /// independent atomics. A concurrent sum can miss one update and under-count
        /// `max_bytes_in_join`. Size-limit checks read only this.
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

    using RightTableDataPtr = std::shared_ptr<RightTableData>;

    /// One saved right block back in the structure of the right input, for an algorithm that takes
    /// the blocks over: the columns of `right_sample_block` by name, their nullability restored.
    static Block restoreRightBlock(const Block & saved_block, const Block & right_sample_block);

    /// Rebuilds one stored block's columns in saved-block order. The row store is scattered back into
    /// columns. The selector is applied to both parts. The access indexes put every column back at its
    /// saved position. Consumes the row store.
    static Columns materializeStoredBlock(StoredBlock & stored_block, const ColumnAccessIndexes & access_indexes);

    /// Modify right block (update structure according to sample block) to save it in block list
    static Block prepareRightBlock(const Block & block, const Block & saved_block_sample_);
};

}
