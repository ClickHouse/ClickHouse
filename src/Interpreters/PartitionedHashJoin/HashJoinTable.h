#pragma once

#include <Interpreters/HashJoin/HashJoinTypes.h>
#include <Interpreters/PartitionedHashJoin/RangeCommittedBuffer.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>

#include <bit>
#include <limits>
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

/// What a partitioned build can produce: `HashJoinTypes::Type` without the `range*` types, which no build
/// creates directly.
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

/// The fixed maps `tryConvertToFixedHashMap` turns a built `key32` / `key64` table into when its keys
/// span a dense range.
#define APPLY_FOR_PARTITIONED_JOIN_RANGE_VARIANTS(M) \
    M(range8_key32) \
    M(range16_key32) \
    M(range17_key32) \
    M(range18_key32) \
    M(range8_key64) \
    M(range16_key64) \
    M(range17_key64) \
    M(range18_key64)

/// Every table a built join may hold - every `HashJoinTypes::Type` - which is what the probe, the non-joined
/// scan and the accounting dispatch over.
#define APPLY_FOR_PARTITIONED_JOIN_TABLES(M) \
    APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M) \
    APPLY_FOR_PARTITIONED_JOIN_RANGE_VARIANTS(M)

/// Turns a map hash into the bits `HashJoinTable` addresses by. The home cell is the top `size_degree`
/// bits of this word and a row's partition the top `partition_bits`. Every key's home cell therefore
/// lies inside its partition's range by construction, whatever table size the barrier later chooses.
///
/// The low 32 hash bits are shifted into the high half, nothing more. A 32-bit CRC lands whole. A
/// 64-bit hash contributes 32 good bits, and a table is at most 2^32 cells. Mixing further would hurt.
/// `HashCRC32` is linear over GF(2), so probe keys arriving in sequence visit cells in a pattern the
/// branch predictor learns, exactly as with `HashMap`'s `hash & mask`. With a multiplicative mix every
/// empty-cell branch became a coin flip on such streams: about one mispredict per probed row against a
/// 2^11-cell table, where the plain placement has 0.08 (`perf stat`, sequential keys). On random keys
/// the two placements behave the same.
ALWAYS_INLINE inline UInt64 hashJoinTablePlacement(size_t hash_value)
{
    return static_cast<UInt64>(hash_value) << 32;
}

/// The distinct-key sketch wants uniform bits, which a raw CRC of structured keys does not give.
/// It still sees the multiplicative mix; the multiplier is the 64-bit golden ratio.
ALWAYS_INLINE inline UInt64 hashJoinTableMix(size_t hash_value)
{
    return static_cast<UInt64>(hash_value) * 0x9E3779B97F4A7C15ULL;
}

/** One open-addressing table for the whole build. Its cell buffer is split into `2^partition_bits`
  * contiguous ranges of `2^range_bits` cells; a partition owns its range during the parallel build.
  * The table may grow in place during post-build (a new buffer, same object, same partition bits).
  * The probe is the standard linear walk over one `{buf, mask}` pair, wrapping at the end of the buffer.
  *
  * `Cell` and `Hash` are the standard join map's, taken from `HashJoinTypes::MapsTemplate`. The cells
  * are bit-identical to `HashJoin`'s. Every key getter works on this table unchanged: it provides
  * `find`, `offsetInternal`, `prefetch` and the type aliases `ColumnsHashing` reads. The partitioned
  * build does not `emplace`: it claims cells through `claim` under its own ownership protocol. The
  * table's size is published once at the end. `emplace` exists for the Join table engine alone, whose
  * table has one range and grows as rows arrive. The zero key lives in the standard zero-value cell.
  *
  * Memory: the buffer is one reservation (`RangeCommittedBuffer`). A range is committed and charged
  * when its owner first touches it. During post-build the table's charge rises as the scattered
  * chunks' charge falls. `getBufferSizeInBytes` reports what has been committed; `reservedBytes` the
  * whole buffer.
  */
template <typename Key, typename Cell, typename Hash, typename Grower>
class HashJoinTable : private Hash, public ZeroValueStorage<Cell::need_zero_value_storage, Cell>
{
public:
    using key_type = Key;
    using cell_type = Cell;
    using mapped_type = typename Cell::mapped_type;
    using value_type = typename Cell::value_type;
    using hash_type = Hash;
    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;

    /// One buffer, as the single-level maps have; the standard probe's prefetch heuristic reads it.
    static constexpr size_t NUM_BUCKETS = 1;

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
    static size_t maxFillFor(size_t size_degree_) { return 1uz << (size_degree_ - 1); }

    HashJoinTable(size_t size_degree_, size_t partition_bits_)
        : size_degree(checkedSizeDegree(size_degree_, partition_bits_))
        , mask((1uz << size_degree) - 1)
        , partition_bits(partition_bits_)
        , range_bits(size_degree - partition_bits)
        , buffer((1uz << size_degree) * sizeof(Cell))
        , buf(reinterpret_cast<Cell *>(buffer.data()))
    {
    }

    ~HashJoinTable()
    {
        /// Only the ASOF cells own anything (a sorted lookup behind a `unique_ptr`); the ref words are
        /// trivial. The old buffer is walked only when fully committed. Uncommitted ranges must not be read.
        if constexpr (!std::is_trivially_destructible_v<Cell>)
        {
            if (buffer.committedBytes() == buffer.size())
                for (Cell * cell = buf, * end = buf + cellCount(); cell != end; ++cell)
                    if (!cell->isZero(state))
                        cell->~Cell();

            /// A failed rehash can leave owners in both buffers. `adoptRehash` clears the pending ranges.
            for (size_t partition = 0; partition < new_range_committed.size(); ++partition)
                if (new_range_committed[partition])
                    for (size_t pos = partition << new_range_bits; pos < newRangeEnd(partition); ++pos)
                        if (!new_buf[pos].isZero(state))
                            new_buf[pos].~Cell();
        }
        if (this->hasZero())
            this->clearHasZero();
    }

    HashJoinTable(const HashJoinTable &) = delete;
    HashJoinTable & operator=(const HashJoinTable &) = delete;

    /// Geometry, fixed at construction except through `adoptRehash`.
    size_t sizeDegree() const { return size_degree; }
    size_t cellCount() const { return mask + 1; }
    size_t cellMask() const { return mask; }
    size_t partitions() const { return 1uz << partition_bits; }
    size_t rangeBegin(size_t partition) const { return partition << range_bits; }
    size_t rangeEnd(size_t partition) const { return (partition + 1) << range_bits; }
    size_t maxFill() const { return maxFillFor(size_degree); }

    /// The standard map interface the key getters, the flags and the accounting read.
    size_t getBufferSizeInCells() const { return cellCount(); }
    size_t getBufferSizeInBytes() const { return buffer.committedBytes(); }
    size_t reservedBytes() const { return buffer.size(); }
    size_t size() const { return m_size; }
    /// The distinct-key count, summed by the build from its owners' claims and set once at publication.
    void setSize(size_t size_) { m_size = size_; }

    ALWAYS_INLINE size_t hash(const Key & key) const { return Hash::operator()(key); }
    ALWAYS_INLINE size_t place(size_t hash_value) const { return hashJoinTablePlacement(hash_value) >> (64 - size_degree); }
    ALWAYS_INLINE size_t next(size_t pos) const { return (pos + 1) & mask; }
    ALWAYS_INLINE size_t partitionOf(size_t hash_value) const
    {
        return partition_bits ? static_cast<size_t>(hashJoinTablePlacement(hash_value) >> (64 - partition_bits)) : 0;
    }
    static ALWAYS_INLINE bool isZeroKey(const Key & key) { return Cell::isZero(key, HashTableNoState{}); }

    Cell * cells() { return buf; }
    const Cell * cells() const { return buf; }
    Cell * cellAt(size_t pos) { return buf + pos; }
    const Cell * cellAt(size_t pos) const { return buf + pos; }
    ALWAYS_INLINE bool isEmptyCell(const Cell * cell) const { return cell->isZero(state); }
    ALWAYS_INLINE bool keyEquals(const Cell * cell, const Key & key, size_t hash_value) const { return cell->keyEquals(key, hash_value, state); }

    /// Accounts and pre-faults one partition's range; called by the owner before its first insert.
    void commitRange(size_t partition) { buffer.commit(rangeBegin(partition) * sizeof(Cell), (1uz << range_bits) * sizeof(Cell)); }
    /// The single-partition path commits the whole buffer at once.
    void commitAll() { buffer.commit(0, buffer.size()); }
    bool fullyCommitted() const { return buffer.committedBytes() == buffer.size(); }

    /// Claims the empty cell at `pos` for `key_holder`, exactly as `emplaceNonZeroImpl` does up to, not
    /// including, the mapped write. The caller writes the mapped value. Not counted here: owners count
    /// their claims and the build publishes the sum with `setSize`.
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

    ALWAYS_INLINE size_t newPlace(size_t hash_value) const { return hashJoinTablePlacement(hash_value) >> (64 - new_size_degree); }
    ALWAYS_INLINE size_t newNext(size_t pos) const { return (pos + 1) & new_mask; }
    ALWAYS_INLINE size_t newRangeEnd(size_t partition) const { return (partition + 1) << new_range_bits; }
    ALWAYS_INLINE size_t newCellCount() const { return new_mask + 1; }
    ALWAYS_INLINE Cell * newCellAt(size_t pos) { return new_buf + pos; }
    ALWAYS_INLINE const Cell * newCellAt(size_t pos) const { return new_buf + pos; }

    void commitNewRange(size_t partition)
    {
        if (new_range_committed[partition])
            return;
        new_buffer->commit((partition << new_range_bits) * sizeof(Cell), (1uz << new_range_bits) * sizeof(Cell));
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

    /// The Join table engine's insert: the classic insert-or-find of `HashTable::emplace`, over the one
    /// range of a single-partition table, doubling at the standard load factor (`maxFill`). Both
    /// signatures `ColumnsHashing` calls; the mapped value of a new cell is left for the caller to
    /// construct, as the standard table does. `it` is refreshed after a grow, so it always points at
    /// the live buffer.
    template <typename KeyHolder>
    ALWAYS_INLINE void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        emplace(key_holder, it, inserted, hash(keyHolderGetKey(key_holder)));
    }

    template <typename KeyHolder>
    ALWAYS_INLINE void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash_value)
    {
        const auto & key = keyHolderGetKey(key_holder);
        if (Cell::isZero(key, state))
        {
            inserted = !this->hasZero();
            if (inserted)
            {
                claimZero(hash_value);
                ++m_size;
            }
            it = this->zeroValue();
            return;
        }

        size_t pos = place(hash_value);
        while (!buf[pos].isZero(state))
        {
            if (buf[pos].keyEquals(key, hash_value, state))
            {
                keyHolderDiscardKey(key_holder);
                it = buf + pos;
                inserted = false;
                return;
            }
            pos = next(pos);
        }

        it = claim(pos, key_holder, hash_value);
        inserted = true;
        ++m_size;
        if (m_size > maxFill()) [[unlikely]]
        {
            growSingleRange();
            it = find(keyHolderGetKey(key_holder), hash_value);
        }
    }

    /// Walks the zero-value cell first, then the occupied buffer cells in position order; `SELECT`
    /// from a Join table reads the rows through it. The position is the cell's used-flags offset
    /// (`offsetInternal`): 0 is the zero-value cell, `p + 1` buffer cell `p`. `const_iterator` is the
    /// name the map-generic readers use.
    class ConstIterator
    {
    public:
        ConstIterator() = default;
        ConstIterator(const HashJoinTable * table_, size_t offset_) : table(table_), offset(offset_) { }

        const Cell & operator*() const { return *getPtr(); }
        const Cell * operator->() const { return getPtr(); }
        const Cell * getPtr() const { return offset == 0 ? table->zeroValue() : table->buf + (offset - 1); }

        ConstIterator & operator++()
        {
            advance();
            return *this;
        }

        bool operator==(const ConstIterator & rhs) const { return offset == rhs.offset; }
        bool operator!=(const ConstIterator & rhs) const { return offset != rhs.offset; }

        /// To the next occupied buffer cell, or to the end.
        void advance()
        {
            const size_t cells = table->cellCount();
            do
                ++offset;
            while (offset <= cells && table->buf[offset - 1].isZero(table->state));
        }

    private:
        const HashJoinTable * table = nullptr;
        size_t offset = 0;
    };
    using const_iterator = ConstIterator;

    const_iterator begin() const
    {
        const_iterator it(this, 0);
        if (!this->hasZero())
            it.advance();
        return it;
    }

    const_iterator end() const { return const_iterator(this, cellCount() + 1); }

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
        return const_cast<HashJoinTable *>(this)->find(key, hash_value);
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
    /// The doubling behind `emplace`: every cell is re-placed into a buffer of twice the cells with the
    /// global mask, its mapped value moved over, and the old buffer released without destructors. Only
    /// a single-partition table may do this; a partitioned one is grown by its build.
    void growSingleRange()
    {
        if (partition_bits != 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "HashJoinTable: emplace can only grow a single-partition table, this one has {} partition bits",
                partition_bits);

        beginRehash(size_degree + 1);
        commitNewRange(0);
        for (Cell * cell = buf, * end = buf + cellCount(); cell != end; ++cell)
        {
            if (cell->isZero(state))
                continue;
            const size_t hash_value = cellHash(cell);
            size_t pos = newPlace(hash_value);
            while (!new_buf[pos].isZero(state))
                pos = newNext(pos);
            Cell * moved = claimPersisted(new_buf + pos, Cell::getKey(cell->getValue()), hash_value);
            new (&moved->getMapped()) mapped_type(std::move(cell->getMapped()));
        }
        adoptRehash();
    }

    /// Validated before any member derives a shift or a buffer size from it.
    static size_t checkedSizeDegree(size_t size_degree_, size_t partition_bits_)
    {
        if (size_degree_ == 0 || size_degree_ > 32 || partition_bits_ > size_degree_)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoinTable: bad geometry, size degree {} with {} partition bits", size_degree_, partition_bits_);
        return size_degree_;
    }

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
inline constexpr bool is_hash_join_table = false;
template <typename Key, typename Cell, typename Hash, typename Grower>
inline constexpr bool is_hash_join_table<HashJoinTable<Key, Cell, Hash, Grower>> = true;

namespace HashJoinTableDetail
{

/// The table type for a standard join hash map type: the `HashJoinTable` over the same key, cell, hash and
/// grower. Direct-index key types keep the unchanged `FixedHashMap`.
template <typename Map>
struct TableFor;

template <typename Key, typename Cell, typename Hash, typename Grower, typename Alloc>
struct TableFor<HashMapTable<Key, Cell, Hash, Grower, Alloc>>
{
    using Type = HashJoinTable<Key, Cell, Hash, Grower>;
};

template <typename Key, typename Mapped, typename Cell, typename Size, typename Alloc, size_t size_bits>
struct TableFor<FixedHashMap<Key, Mapped, Cell, Size, Alloc, size_bits>>
{
    using Type = FixedHashMap<Key, Mapped, Cell, Size, Alloc, size_bits>;
};

}

/** The build's tables for one mapped-value type: one member per supported `HashJoinTypes::Type`, exactly one
  * of them created. Every member type is derived from `HashJoinTypes::MapsTemplate`. A master-side change of
  * a cell type or hash therefore propagates here, and an incompatible restructuring breaks the build
  * instead of silently diverging.
  */
template <typename Mapped>
struct HashJoinTableMapsTemplate
{
private:
    using StandardMaps = HashJoinTypes::MapsTemplate<Mapped>;

public:
    /// NOLINTBEGIN(bugprone-macro-parentheses)
#define M(NAME) \
    std::shared_ptr<typename HashJoinTableDetail::TableFor<typename decltype(StandardMaps::NAME)::element_type>::Type> NAME;
    APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M

private:
    /// A `FixedHashMap` spans the whole key domain whatever the plan asks for.
    template <typename Table>
    static size_t fixedDomainBytes()
    {
        static_assert(!is_hash_join_table<Table> && sizeof(typename Table::key_type) <= 2);
        return (1uz << (sizeof(typename Table::key_type) * 8)) * sizeof(typename Table::cell_type);
    }

    template <typename Table>
    static size_t bufferBytesForDegreeFor(size_t size_degree)
    {
        if constexpr (is_hash_join_table<Table>)
            return (1uz << size_degree) * sizeof(typename Table::cell_type);
        else
            return fixedDomainBytes<Table>();
    }

    /// The load at which the table doubles; a fixed-size map never grows.
    template <typename Table>
    static size_t maxFillOf(const Table & table)
    {
        if constexpr (is_hash_join_table<Table>)
            return table.maxFill();
        else
            return std::numeric_limits<size_t>::max();
    }

    template <typename Table>
    static size_t sizeDegreeFor(size_t reserve)
    {
        if constexpr (is_hash_join_table<Table>)
            return Table::degreeFor(reserve);
        else
            return 0;
    }

    /// The whole buffer, committed or not: what the plan predicted.
    template <typename Table>
    static size_t reservedBufferBytesOf(const Table & table)
    {
        if constexpr (is_hash_join_table<Table>)
            return table.reservedBytes();
        else
            return table.getBufferSizeInBytes();
    }

public:
    /// The bytes of a table with `2^size_degree` cells. For plans that widened the degree beyond what
    /// `reserve` asks for: the plan keeps at least 2^10 cells per partition range.
    static size_t bufferBytesForDegree(HashJoinTypes::Type which, size_t size_degree)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return bufferBytesForDegreeFor<typename decltype(HashJoinTableMapsTemplate::NAME)::element_type>(size_degree);
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", which);
        }
    }

    size_t maxFill(HashJoinTypes::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return NAME ? maxFillOf(*NAME) : 0;
            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
        }
    }

    /// The buffer degree the table for `reserve` keys gets (0 for the fixed-size types).
    static size_t sizeDegree(HashJoinTypes::Type which, size_t reserve)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return sizeDegreeFor<typename decltype(HashJoinTableMapsTemplate::NAME)::element_type>(reserve);
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", which);
        }
    }

    static size_t cellBytes(HashJoinTypes::Type which)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return sizeof(typename decltype(HashJoinTableMapsTemplate::NAME)::element_type::cell_type);
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", which);
        }
    }

    /// Creates the one table: a `HashJoinTable` of `2^size_degree` cells in `2^partition_bits` ranges, or the
    /// fixed map, whose partition count is always one.
    void create(HashJoinTypes::Type which, size_t size_degree, size_t partition_bits)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: { \
        using Table = typename decltype(NAME)::element_type; \
        if constexpr (is_hash_join_table<Table>) \
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

    size_t getTotalRowCount(HashJoinTypes::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return NAME ? NAME->size() : 0;
            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
        }
    }

    size_t getBufferSizeInBytes(HashJoinTypes::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return NAME ? NAME->getBufferSizeInBytes() : 0;
            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
        }
    }

    size_t getBufferSizeInCells(HashJoinTypes::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return NAME ? NAME->getBufferSizeInCells() : 0;
            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
        }
    }

    size_t getReservedBufferBytes(HashJoinTypes::Type which) const
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return NAME ? reservedBufferBytesOf(*NAME) : 0;
            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
        }
    }
    /// NOLINTEND(bugprone-macro-parentheses)
};

using HashJoinTableMapsOne = HashJoinTableMapsTemplate<RowRef>;
using HashJoinTableMapsAll = HashJoinTableMapsTemplate<RowRefList>;
using HashJoinTableMapsAsof = HashJoinTableMapsTemplate<AsofRowRefs>;

/// `processMatch`, the used-flags offsets and the lazy emit all read cells through the standard
/// machinery, so the layouts have to match. The traits above make that true by construction; these break
/// the build if the member declarations ever stop being derived from the standard ones.
#define M(NAME) \
    static_assert( \
        std::is_same_v< \
            typename decltype(HashJoinTableMapsOne::NAME)::element_type::cell_type, \
            typename decltype(HashJoinTypes::MapsOne::NAME)::element_type::cell_type> \
            && std::is_same_v< \
                typename decltype(HashJoinTableMapsAll::NAME)::element_type::cell_type, \
                typename decltype(HashJoinTypes::MapsAll::NAME)::element_type::cell_type> \
            && std::is_same_v< \
                typename decltype(HashJoinTableMapsAsof::NAME)::element_type::cell_type, \
                typename decltype(HashJoinTypes::MapsAsof::NAME)::element_type::cell_type>, \
        "HashJoinTable cells must be identical to the standard join map cells");
APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M

/// The `HashJoinTable` counterpart of a standard maps type, for the `MapGetter` and `JoinFeatures` templates
/// that are written in terms of the standard one.
template <typename StandardMaps>
struct HashJoinTableMapsFor;

template <>
struct HashJoinTableMapsFor<HashJoinTypes::MapsOne>
{
    using Type = HashJoinTableMapsOne;
};
template <>
struct HashJoinTableMapsFor<HashJoinTypes::MapsAll>
{
    using Type = HashJoinTableMapsAll;
};
template <>
struct HashJoinTableMapsFor<HashJoinTypes::MapsAsof>
{
    using Type = HashJoinTableMapsAsof;
};

/** A variant over the three mapped-value types whose active alternative mirrors the join's
  * own `MapsVariant`. Build and probe agree with the standard machinery about which maps type a
  * given (kind, strictness) uses.
  */
struct HashJoinTableMaps
{
    using Variant = std::variant<HashJoinTableMapsOne, HashJoinTableMapsAll, HashJoinTableMapsAsof>;

    /// Index-compatible with `HashJoinTypes::MapsVariant` - the active alternative is selected by that
    /// variant's index. `HashJoinTypes::MapsSet` (index 3) is the one alternative without a counterpart here:
    /// its key-only tables are hash sets, not the hash maps the traits rebind. The join therefore
    /// sets `allow_set_maps` to false. It never selects one.
    static_assert(
        std::is_same_v<std::variant_alternative_t<0, HashJoinTypes::MapsVariant>, HashJoinTypes::MapsOne>
        && std::is_same_v<std::variant_alternative_t<1, HashJoinTypes::MapsVariant>, HashJoinTypes::MapsAll>
        && std::is_same_v<std::variant_alternative_t<2, HashJoinTypes::MapsVariant>, HashJoinTypes::MapsAsof>
        && std::is_same_v<std::variant_alternative_t<3, HashJoinTypes::MapsVariant>, HashJoinTypes::MapsSet>
        && std::variant_size_v<HashJoinTypes::MapsVariant> == 4);

    Variant maps;

    explicit HashJoinTableMaps(size_t standard_variant_index)
    {
        switch (standard_variant_index)
        {
            case 0: maps.emplace<HashJoinTableMapsOne>(); break;
            case 1: maps.emplace<HashJoinTableMapsAll>(); break;
            case 2: maps.emplace<HashJoinTableMapsAsof>(); break;
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    static bool isSupportedType(HashJoinTypes::Type which)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return true;
            APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
            default: return false;
        }
    }

    /// A `FixedHashMap` buffer does not depend on the build size. Partitioning cannot shrink it,
    /// and such plans always run as a single partition.
    static bool isFixedSizeType(HashJoinTypes::Type which)
    {
        switch (which)
        {
#define M(NAME) \
    case HashJoinTypes::Type::NAME: return !is_hash_join_table<typename decltype(HashJoinTableMapsAll::NAME)::element_type>;
            APPLY_FOR_PARTITIONED_JOIN_TABLES(M)
#undef M
        }
    }

    /// The bytes the table for `reserve` keys will take: the standard grower's rounding of `reserve`
    /// (`sizeDegree`), then the buffer of that degree.
    static size_t predictedBufferBytes(size_t standard_variant_index, HashJoinTypes::Type which, size_t reserve)
    {
        return bufferBytesForDegree(standard_variant_index, which, sizeDegree(standard_variant_index, which, reserve));
    }

    static size_t sizeDegree(size_t standard_variant_index, HashJoinTypes::Type which, size_t reserve)
    {
        switch (standard_variant_index)
        {
            case 0: return HashJoinTableMapsOne::sizeDegree(which, reserve);
            case 1: return HashJoinTableMapsAll::sizeDegree(which, reserve);
            case 2: return HashJoinTableMapsAsof::sizeDegree(which, reserve);
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    static size_t cellBytes(size_t standard_variant_index, HashJoinTypes::Type which)
    {
        switch (standard_variant_index)
        {
            case 0: return HashJoinTableMapsOne::cellBytes(which);
            case 1: return HashJoinTableMapsAll::cellBytes(which);
            case 2: return HashJoinTableMapsAsof::cellBytes(which);
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    static size_t bufferBytesForDegree(size_t standard_variant_index, HashJoinTypes::Type which, size_t size_degree)
    {
        switch (standard_variant_index)
        {
            case 0: return HashJoinTableMapsOne::bufferBytesForDegree(which, size_degree);
            case 1: return HashJoinTableMapsAll::bufferBytesForDegree(which, size_degree);
            case 2: return HashJoinTableMapsAsof::bufferBytesForDegree(which, size_degree);
            default: throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unexpected join maps variant index {}", standard_variant_index);
        }
    }

    void create(HashJoinTypes::Type which, size_t size_degree, size_t partition_bits)
    {
        std::visit([&](auto & shape) { shape.create(which, size_degree, partition_bits); }, maps);
    }

    size_t getTotalRowCount(HashJoinTypes::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getTotalRowCount(which); }, maps);
    }

    size_t getBufferSizeInBytes(HashJoinTypes::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getBufferSizeInBytes(which); }, maps);
    }

    size_t getBufferSizeInCells(HashJoinTypes::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getBufferSizeInCells(which); }, maps);
    }

    size_t getReservedBufferBytes(HashJoinTypes::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.getReservedBufferBytes(which); }, maps);
    }

    size_t maxFill(HashJoinTypes::Type which) const
    {
        return std::visit([&](const auto & shape) { return shape.maxFill(which); }, maps);
    }
};

}
