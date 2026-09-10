#pragma once

#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/PartitionedHashJoin/RangeCommittedBuffer.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>

#include <bit>
#include <optional>
#include <variant>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_JOIN_KEYS;
extern const int LOGICAL_ERROR;
}

/// What a partitioned build can produce: the single-level subset of `HashJoin::Type`. Two-level maps
/// are what partitioning replaces, and the `range*` conversions are post-build optimizations this
/// path does not run.
#define APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M) \
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
    M(low_cardinality_key_fixed_string)

/// Turns a map hash into the bits the shared table addresses by. The home cell is the top `size_degree`
/// bits of the product and a row's partition the top `partition_bits`, so every key's home cell lies
/// inside its partition's range by construction, whatever table size the barrier later chooses. The
/// multiplier is the 64-bit golden ratio; a multiply moves the entropy of a 32-bit CRC into the high
/// bits without another dependent operation on the probe path.
ALWAYS_INLINE inline UInt64 sharedJoinMix(size_t hash_value)
{
    return static_cast<UInt64>(hash_value) * 0x9E3779B97F4A7C15ULL;
}

/** One open-addressing table for the whole build. Its cell buffer is split into `2^partition_bits`
  * contiguous ranges of `2^range_bits` cells; a partition owns its range during the parallel build.
  * The table may grow in place during post-build (a new buffer, same object, same partition bits).
  * The probe is the standard linear walk over one `{buf, mask}` pair, wrapping at the end of the buffer.
  *
  * `Cell` and `Hash` are exactly the standard join map's, taken from `HashJoin::MapsTemplate`, so the
  * cells this table holds are bit-identical to `HashJoin`'s and every key getter works on it unchanged:
  * it provides `find`, `offsetInternal`, `prefetch` and the type aliases `ColumnsHashing` reads. There is
  * no `emplace`: the build claims cells through `claim` under its own ownership protocol, and the table's
  * size is published once at the end. The zero key lives in the standard zero-value cell.
  *
  * Memory: the buffer is one reservation whose ranges are committed and accounted when their owners
  * first touch them (`RangeCommittedBuffer`), so the post-build peak keeps trading the table off against
  * the scattered chunks range by range. `getBufferSizeInBytes` reports what has been committed.
  */
template <typename Key, typename Cell, typename Hash, typename Grower>
class SharedJoinTable : private Hash, public ZeroValueStorage<Cell::need_zero_value_storage, Cell>
{
public:
    using key_type = Key;
    using cell_type = Cell;
    using mapped_type = typename Cell::mapped_type;
    using value_type = typename Cell::value_type;
    using grower_type = Grower;
    using hash_type = Hash;
    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    static_assert(std::is_same_v<typename Cell::State, HashTableNoState>, "the walk reads nothing through the table");
    static_assert(Grower::performs_linear_probing_with_single_step, "ranges assume the standard linear probe");
    static_assert(Cell::need_zero_value_storage, "the zero key is stored in the zero-value cell");

    /// The standard grower's rounding for `reserve` keys, so the plan's predicted bytes equal the
    /// created ones: `cells / 2 >= reserve`, at least 2^8 cells.
    static size_t degreeFor(size_t reserve)
    {
        Grower grower;
        grower.set(reserve);
        return static_cast<size_t>(std::countr_zero(grower.bufSize()));
    }
    static size_t bufferBytesFor(size_t reserve) { return (1uz << degreeFor(reserve)) * sizeof(Cell); }
    static size_t maxFillFor(size_t size_degree_) { return 1uz << (size_degree_ - 1); }

    /// Validated before any member derives a shift or a buffer size from it.
    static size_t checkedSizeDegree(size_t size_degree_, size_t partition_bits_)
    {
        if (size_degree_ == 0 || size_degree_ > 32 || partition_bits_ > size_degree_)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SharedJoinTable: bad geometry, size degree {} with {} partition bits", size_degree_, partition_bits_);
        return size_degree_;
    }

    SharedJoinTable(size_t size_degree_, size_t partition_bits_)
        : size_degree(checkedSizeDegree(size_degree_, partition_bits_))
        , mask((1uz << size_degree) - 1)
        , partition_bits(partition_bits_)
        , range_bits(size_degree - partition_bits)
        , buffer((1uz << size_degree) * sizeof(Cell))
        , buf(reinterpret_cast<Cell *>(buffer.data()))
    {
    }

    ~SharedJoinTable()
    {
        /// Only the ASOF cells own anything (a sorted lookup behind a `unique_ptr`); the ref words are
        /// trivial. Every range is committed by the end of a build; a build that failed half-way leaves
        /// uncommitted (never zeroed) ranges, which must not be read, so the walk is skipped then.
        if constexpr (!std::is_trivially_destructible_v<Cell>)
        {
            if (buffer.committedBytes() == buffer.size())
                for (Cell * cell = buf, * end = buf + cellCount(); cell != end; ++cell)
                    if (!cell->isZero(state))
                        cell->~Cell();
        }
        if (this->hasZero())
            this->clearHasZero();
    }

    SharedJoinTable(const SharedJoinTable &) = delete;
    SharedJoinTable & operator=(const SharedJoinTable &) = delete;

    /// Geometry.
    size_t sizeDegree() const { return size_degree; }
    size_t cellCount() const { return mask + 1; }
    size_t cellMask() const { return mask; }
    size_t partitionBits() const { return partition_bits; }
    size_t partitions() const { return 1uz << partition_bits; }
    size_t rangeBits() const { return range_bits; }
    size_t rangeCells() const { return 1uz << range_bits; }
    size_t rangeBegin(size_t partition) const { return partition << range_bits; }
    size_t rangeEnd(size_t partition) const { return (partition + 1) << range_bits; }
    size_t maxFill() const { return maxFillFor(size_degree); }

    /// The standard map interface the key getters, the flags and the accounting read.
    size_t getBufferSizeInCells() const { return mask + 1; }
    size_t getBufferSizeInBytes() const { return buffer.committedBytes(); }
    size_t reservedBytes() const { return buffer.size(); }
    size_t size() const { return m_size; }
    /// The distinct-key count, summed by the build from its owners' claims and set once at publication.
    void setSize(size_t size_) { m_size = size_; }

    /// Hashing and placement.
    ALWAYS_INLINE size_t hash(const Key & key) const { return Hash::operator()(key); }
    ALWAYS_INLINE size_t place(size_t hash_value) const { return sharedJoinMix(hash_value) >> (64 - size_degree); }
    ALWAYS_INLINE size_t next(size_t pos) const { return (pos + 1) & mask; }
    ALWAYS_INLINE size_t partitionOf(size_t hash_value) const
    {
        return partition_bits ? static_cast<size_t>(sharedJoinMix(hash_value) >> (64 - partition_bits)) : 0;
    }
    static ALWAYS_INLINE bool isZeroKey(const Key & key) { return Cell::isZero(key, HashTableNoState{}); }

    Cell * cells() { return buf; }
    const Cell * cells() const { return buf; }
    Cell * cellAt(size_t pos) { return buf + pos; }
    const Cell * cellAt(size_t pos) const { return buf + pos; }
    ALWAYS_INLINE bool isEmptyCell(const Cell * cell) const { return cell->isZero(state); }
    ALWAYS_INLINE bool keyEquals(const Cell * cell, const Key & key, size_t hash_value) const { return cell->keyEquals(key, hash_value, state); }

    /// Accounts and pre-faults one partition's range; called by the owner before its first insert.
    void commitRange(size_t partition) { buffer.commit(rangeBegin(partition) * sizeof(Cell), rangeCells() * sizeof(Cell)); }
    /// For tests and the single-partition path: the whole buffer at once.
    void commitAll() { buffer.commit(0, buffer.size()); }
    bool fullyCommitted() const { return buffer.committedBytes() == buffer.size(); }

    /// Claims the empty cell at `pos` for `key_holder`, as `emplaceNonZeroImpl` does up to the mapped
    /// write, which the caller performs. Not counted here: owners count their claims and the build
    /// publishes the sum with `setSize`.
    template <typename KeyHolder>
    ALWAYS_INLINE Cell * claim(size_t pos, KeyHolder && key_holder, size_t hash_value)
    {
        keyHolderPersistKey(key_holder);
        const auto & key = keyHolderGetKey(key_holder);
        return claimPersisted(buf + pos, key, hash_value);
    }

    /// Places an already-persisted key into `cell` of either the live buffer or a rehash buffer.
    ALWAYS_INLINE Cell * claimPersisted(Cell * cell, const Key & key, size_t hash_value)
    {
        chassert(cell->isZero(state));
        new (cell) Cell(key, state);
        cell->setHash(hash_value);
        return cell;
    }

    size_t cellHash(const Cell * cell) const { return cell->getHash(static_cast<const Hash &>(*this)); }

    /// In-place rehash onto a larger buffer. `beginRehash` allocates the new geometry; the caller
    /// writes into it through `newCellAt` / `newPlace` / `commitNewRange`; `adoptRehash` swaps it in
    /// and releases the old buffer without running cell destructors.
    void beginRehash(size_t new_degree)
    {
        const size_t degree = checkedSizeDegree(new_degree, partition_bits);
        new_size_degree = degree;
        new_mask = (1uz << degree) - 1;
        new_range_bits = degree - partition_bits;
        new_buffer.emplace((1uz << degree) * sizeof(Cell));
        new_buf = reinterpret_cast<Cell *>(new_buffer->data());
        new_range_committed.assign(partitions(), 0);
    }

    ALWAYS_INLINE size_t newPlace(size_t hash_value) const { return sharedJoinMix(hash_value) >> (64 - new_size_degree); }
    ALWAYS_INLINE size_t newNext(size_t pos) const { return (pos + 1) & new_mask; }
    ALWAYS_INLINE size_t newRangeBegin(size_t partition) const { return partition << new_range_bits; }
    ALWAYS_INLINE size_t newRangeEnd(size_t partition) const { return (partition + 1) << new_range_bits; }
    ALWAYS_INLINE size_t newCellCount() const { return new_mask + 1; }
    ALWAYS_INLINE Cell * newCellAt(size_t pos) { return new_buf + pos; }
    ALWAYS_INLINE const Cell * newCellAt(size_t pos) const { return new_buf + pos; }

    void commitNewRange(size_t partition)
    {
        if (new_range_committed[partition])
            return;
        new_buffer->commit(newRangeBegin(partition) * sizeof(Cell), (1uz << new_range_bits) * sizeof(Cell));
        new_range_committed[partition] = 1;
    }

    bool newRangeIsCommitted(size_t partition) const { return new_range_committed[partition]; }

    void adoptRehash()
    {
        /// The old cells' mapped values were moved out. Skip their destructors; move-assigning
        /// `buffer` frees the old reservation and its tracker charge.
        Cell * next = reinterpret_cast<Cell *>(new_buffer->data());
        buffer = std::move(*new_buffer);
        new_buffer.reset();
        buf = next;
        size_degree = new_size_degree;
        mask = new_mask;
        range_bits = new_range_bits;
        new_buf = nullptr;
        new_size_degree = 0;
        new_mask = 0;
        new_range_bits = 0;
        new_range_committed.clear();
    }

    /// Claims the zero-value cell; its mapped value is default-constructed, as in `emplaceIfZero`.
    Cell * claimZero(size_t hash_value)
    {
        chassert(!this->hasZero());
        this->setHasZero();
        this->zeroValue()->setHash(hash_value);
        return this->zeroValue();
    }

    /// The probe lookup: `HashMapTable::find` over one buffer.
    ALWAYS_INLINE LookupResult find(const Key & key) { return find(key, hash(key)); }
    ALWAYS_INLINE ConstLookupResult find(const Key & key) const { return find(key, hash(key)); }

    ALWAYS_INLINE LookupResult find(const Key & key, size_t hash_value)
    {
        if (Cell::isZero(key, state))
            return this->hasZero() ? this->zeroValue() : nullptr;
        size_t pos = place(hash_value);
        while (!buf[pos].isZero(state) && !buf[pos].keyEquals(key, hash_value, state))
            pos = next(pos);
        return buf[pos].isZero(state) ? nullptr : buf + pos;
    }
    ALWAYS_INLINE ConstLookupResult find(const Key & key, size_t hash_value) const
    {
        return const_cast<SharedJoinTable *>(this)->find(key, hash_value);
    }

    /// The used-flags offset of a found cell: 0 for the zero-value cell, position + 1 otherwise, exactly
    /// as `HashTable::offsetInternal`. The flag space is therefore `cellCount() + 1` wide.
    size_t offsetInternal(ConstLookupResult ptr) const
    {
        if (ptr->isZero(state))
            return 0;
        return static_cast<size_t>(ptr - buf) + 1;
    }

    template <typename KeyHolder>
    ALWAYS_INLINE void prefetch(KeyHolder && key_holder) const
    {
        const auto & key = keyHolderGetKey(key_holder);
        __builtin_prefetch(buf + place(hash(key)));
    }

private:
    size_t size_degree;
    size_t mask;
    size_t partition_bits;
    size_t range_bits;
    RangeCommittedBuffer buffer;
    Cell * buf;
    size_t m_size = 0;
    HashTableNoState state;

    std::optional<RangeCommittedBuffer> new_buffer;
    Cell * new_buf = nullptr;
    size_t new_size_degree = 0;
    size_t new_mask = 0;
    size_t new_range_bits = 0;
    std::vector<UInt8> new_range_committed;
};

template <typename T>
inline constexpr bool is_shared_join_table = false;
template <typename Key, typename Cell, typename Hash, typename Grower>
inline constexpr bool is_shared_join_table<SharedJoinTable<Key, Cell, Hash, Grower>> = true;

namespace SharedJoinTableDetail
{

/// The table type for a standard join hash map type: the shared table over the same key, cell, hash and
/// grower, or the unchanged `FixedHashMap` for the direct-index key types.
template <typename Map>
struct TableFor;

template <typename Key, typename Cell, typename Hash, typename Grower, typename Alloc>
struct TableFor<HashMapTable<Key, Cell, Hash, Grower, Alloc>>
{
    using Type = SharedJoinTable<Key, Cell, Hash, Grower>;
};

template <typename Key, typename Mapped, typename Cell, typename Size, typename Alloc, size_t size_bits>
struct TableFor<FixedHashMap<Key, Mapped, Cell, Size, Alloc, size_bits>>
{
    using Type = FixedHashMap<Key, Mapped, Cell, Size, Alloc, size_bits>;
};

/// The maps member for a `HashJoin::Type`, so a template can pick a table type by the enum value.
template <HashJoin::Type type>
struct MemberOf;

#define M(NAME) \
    template <> \
    struct MemberOf<HashJoin::Type::NAME> \
    { \
        template <typename Maps> \
        static auto & get(Maps & maps) \
        { \
            return maps.NAME; \
        } \
    };
APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M

}

/** The build's tables for one mapped-value shape: one member per supported `HashJoin::Type`, exactly one
  * of them created. Deriving every member from `HashJoin::MapsTemplate` means a master-side change of a
  * cell type or hash propagates here, and an incompatible restructuring breaks the build instead of
  * silently diverging.
  */
template <typename Mapped>
struct SharedJoinMapsTemplate
{
    using MappedType = Mapped;
    using StandardMaps = HashJoin::MapsTemplate<Mapped>;

    /// NOLINTBEGIN(bugprone-macro-parentheses)
#define M(NAME) \
    std::shared_ptr<typename SharedJoinTableDetail::TableFor<typename decltype(StandardMaps::NAME)::element_type>::Type> NAME;
    APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M

    template <typename Table>
    static size_t predictedBufferBytesFor(size_t reserve)
    {
        if constexpr (is_shared_join_table<Table>)
            return Table::bufferBytesFor(reserve);
        else
        {
            /// A FixedHashTable spans the whole key domain whatever the reserve says.
            static_assert(sizeof(typename Table::key_type) <= 2);
            return (1uz << (sizeof(typename Table::key_type) * 8)) * sizeof(typename Table::cell_type);
        }
    }

    template <typename Table>
    static size_t sizeDegreeFor(size_t reserve)
    {
        if constexpr (is_shared_join_table<Table>)
            return Table::degreeFor(reserve);
        else
            return 0;
    }

    static size_t predictedBufferBytes(HashJoin::Type which, size_t reserve)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return predictedBufferBytesFor<typename decltype(SharedJoinMapsTemplate::NAME)::element_type>(reserve);
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", which);
        }
    }

    /// The buffer degree the table for `reserve` keys gets (0 for the fixed-size types).
    static size_t sizeDegree(HashJoin::Type which, size_t reserve)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return sizeDegreeFor<typename decltype(SharedJoinMapsTemplate::NAME)::element_type>(reserve);
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", which);
        }
    }

    static size_t cellBytes(HashJoin::Type which)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return sizeof(typename decltype(SharedJoinMapsTemplate::NAME)::element_type::cell_type);
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", which);
        }
    }

    /// Creates the one table: a shared table of `2^size_degree` cells in `2^partition_bits` ranges, or the
    /// fixed map, whose partition count is always one.
    void create(HashJoin::Type which, size_t size_degree, size_t partition_bits)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: { \
        using Table = typename decltype(NAME)::element_type; \
        if constexpr (is_shared_join_table<Table>) \
            NAME = std::make_shared<Table>(size_degree, partition_bits); \
        else \
            NAME = std::make_shared<Table>(); \
        break; \
    }
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", which);
        }
    }

    size_t getTotalRowCount(HashJoin::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return NAME ? NAME->size() : 0;
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: return 0;
        }
    }

    size_t getBufferSizeInBytes(HashJoin::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return NAME ? NAME->getBufferSizeInBytes() : 0;
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: return 0;
        }
    }

    size_t getBufferSizeInCells(HashJoin::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return NAME ? NAME->getBufferSizeInCells() : 0;
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: return 0;
        }
    }

    /// The whole buffer, committed or not: what the plan predicted.
    template <typename Table>
    static size_t reservedBufferBytesOf(const Table & table)
    {
        if constexpr (is_shared_join_table<Table>)
            return table.reservedBytes();
        else
            return table.getBufferSizeInBytes();
    }

    size_t getReservedBufferBytes(HashJoin::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return NAME ? reservedBufferBytesOf(*NAME) : 0;
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: return 0;
        }
    }
    /// NOLINTEND(bugprone-macro-parentheses)
};

using SharedMapsOne = SharedJoinMapsTemplate<RowRef>;
using SharedMapsAll = SharedJoinMapsTemplate<RowRefList>;
using SharedMapsAsof = SharedJoinMapsTemplate<AsofRowRefs>;

/// `processMatch`, the used-flags offsets and the lazy emit all read cells through the standard
/// machinery, so the layouts have to match. The traits above make that true by construction; these break
/// the build if the member declarations ever stop being derived from the standard ones.
#define M(NAME) \
    static_assert( \
        std::is_same_v< \
            typename decltype(SharedMapsOne::NAME)::element_type::cell_type, \
            typename decltype(HashJoin::MapsOne::NAME)::element_type::cell_type> \
            && std::is_same_v< \
                typename decltype(SharedMapsAll::NAME)::element_type::cell_type, \
                typename decltype(HashJoin::MapsAll::NAME)::element_type::cell_type> \
            && std::is_same_v< \
                typename decltype(SharedMapsAsof::NAME)::element_type::cell_type, \
                typename decltype(HashJoin::MapsAsof::NAME)::element_type::cell_type>, \
        "shared join table cells must be identical to the standard join map cells");
APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M

/// From the maps shape `MapGetter` and `JoinFeatures` speak to the shared counterpart.
template <typename StandardMaps>
struct SharedMapsFor;

template <>
struct SharedMapsFor<HashJoin::MapsOne>
{
    using Type = SharedMapsOne;
};
template <>
struct SharedMapsFor<HashJoin::MapsAll>
{
    using Type = SharedMapsAll;
};
template <>
struct SharedMapsFor<HashJoin::MapsAsof>
{
    using Type = SharedMapsAsof;
};

/** A variant over the three mapped-value shapes whose active alternative mirrors the leaf `HashJoin`'s
  * own `MapsVariant`, so build and probe agree with the standard machinery about which shape a given
  * (kind, strictness) uses.
  */
struct SharedJoinMaps
{
    using Variant = std::variant<SharedMapsOne, SharedMapsAll, SharedMapsAsof>;

    /// Index-compatible with `HashJoin::MapsVariant` - the active alternative is selected by that
    /// variant's index. `HashJoin::MapsSet` (index 3) is the one alternative without a counterpart here:
    /// its key-only tables are hash sets, not the hash maps the traits rebind. The leaf `HashJoin` is
    /// therefore built with `allow_set_maps_ = false`, so it never selects one.
    static_assert(
        std::is_same_v<std::variant_alternative_t<0, HashJoin::MapsVariant>, HashJoin::MapsOne>
        && std::is_same_v<std::variant_alternative_t<1, HashJoin::MapsVariant>, HashJoin::MapsAll>
        && std::is_same_v<std::variant_alternative_t<2, HashJoin::MapsVariant>, HashJoin::MapsAsof>
        && std::is_same_v<std::variant_alternative_t<3, HashJoin::MapsVariant>, HashJoin::MapsSet>
        && std::variant_size_v<HashJoin::MapsVariant> == 4);

    Variant maps;

    explicit SharedJoinMaps(size_t standard_variant_index = 1)
    {
        switch (standard_variant_index)
        {
            case 0: maps.emplace<SharedMapsOne>(); break;
            case 1: maps.emplace<SharedMapsAll>(); break;
            case 2: maps.emplace<SharedMapsAsof>(); break;
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    static bool isSupportedType(HashJoin::Type which)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoin::Type::NAME: return true;
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: return false;
        }
    }

    /// A `FixedHashMap` buffer does not depend on the build size, so partitioning cannot shrink it
    /// and such plans always run as a single partition.
    static bool isFixedSizeType(HashJoin::Type which) { return which == HashJoin::Type::key8 || which == HashJoin::Type::key16; }

    static size_t predictedBufferBytes(size_t standard_variant_index, HashJoin::Type which, size_t reserve)
    {
        switch (standard_variant_index)
        {
            case 0: return SharedMapsOne::predictedBufferBytes(which, reserve);
            case 1: return SharedMapsAll::predictedBufferBytes(which, reserve);
            case 2: return SharedMapsAsof::predictedBufferBytes(which, reserve);
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    static size_t sizeDegree(size_t standard_variant_index, HashJoin::Type which, size_t reserve)
    {
        switch (standard_variant_index)
        {
            case 0: return SharedMapsOne::sizeDegree(which, reserve);
            case 1: return SharedMapsAll::sizeDegree(which, reserve);
            case 2: return SharedMapsAsof::sizeDegree(which, reserve);
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    static size_t cellBytes(size_t standard_variant_index, HashJoin::Type which)
    {
        switch (standard_variant_index)
        {
            case 0: return SharedMapsOne::cellBytes(which);
            case 1: return SharedMapsAll::cellBytes(which);
            case 2: return SharedMapsAsof::cellBytes(which);
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    void create(HashJoin::Type which, size_t size_degree, size_t partition_bits)
    {
        std::visit([&](auto & shape) { shape.create(which, size_degree, partition_bits); }, maps);
    }

    size_t getTotalRowCount(HashJoin::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getTotalRowCount(which); }, maps);
    }

    size_t getBufferSizeInBytes(HashJoin::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getBufferSizeInBytes(which); }, maps);
    }

    size_t getBufferSizeInCells(HashJoin::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getBufferSizeInCells(which); }, maps);
    }

    size_t getReservedBufferBytes(HashJoin::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getReservedBufferBytes(which); }, maps);
    }
};

}
