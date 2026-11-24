#pragma once

#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/arithmeticOverflow.h>
#include <base/defines.h>
#include <base/types.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace DB
{

/// The bounds of one hash table.
/// The end of the cells is `cells + mask + 1`.
template <typename Cell>
struct JoinPartitionDesc
{
    const Cell * cells = nullptr;
    size_t mask = 0;
};

/** The hash table of one `ON` disjunct, planned with one partition. It is the map itself, so a probe has the loads of the
  * plain map, and every method of the map is available. `freeze` adds the descriptor that the AMAC ring reads.
  */
template <typename Map>
class JoinSingleView : public Map
{
public:
    using cell_type = typename Map::cell_type;
    using Desc = JoinPartitionDesc<cell_type>;

    static constexpr bool partitioned = false;

    /// The cells and the mask of the map, set by `freeze`. Valid until the next insert.
    [[nodiscard]] const Desc & desc(size_t) const { return frozen_desc; }
    void freeze()
    {
        frozen_desc = {this->buf, this->getBufferSizeInCells() - 1};
        frozen = true;
    }
    [[nodiscard]] bool isFrozen() const { return frozen; }

private:
    Desc frozen_desc;
    bool frozen = false;
};

namespace detail
{
template <typename Key, typename Cell, typename Hash, typename Grower, typename Allocator>
Hash joinMapHashOf(const HashTable<Key, Cell, Hash, Grower, Allocator> &);
}

/// The hash function of a `HashTable` map.
template <typename Map>
using JoinMapHashOf = decltype(detail::joinMapHashOf(std::declval<const Map &>()));

/** The hash table of one `ON` disjunct as P ordinary hash tables, one per partition, P = 2^k with 1 <= k <= 15. A key goes to
  * partition `(h >> (32 - k)) & (P - 1)` of the 64-bit hash `h` that the map computes, and to a cell by the low bits of `h`.
  *
  * Each partition has the same power-of-two cell count. The probe reads one cell pointer per partition; the high bits
  * of a global cell offset give its partition. The map objects retain ownership and store the zero key separately.
  */
template <typename Map>
class JoinPartitionedView
{
public:
    using map_type = Map;
    using key_type = typename Map::key_type;
    using mapped_type = typename Map::mapped_type;
    using value_type = typename Map::value_type;
    using cell_type = typename Map::cell_type;
    using LookupResult = typename Map::LookupResult;
    using ConstLookupResult = typename Map::ConstLookupResult;
    using Desc = JoinPartitionDesc<cell_type>;

    static constexpr bool partitioned = true;
    static constexpr size_t max_partition_bits = 15;

    /// `robin_hood_allowed_` is the setting `enable_robin_hood_hash_for_joins`, for the maps that the view creates itself.
    JoinPartitionedView(size_t partition_bits, bool robin_hood_allowed_)
        : shift(32 - partition_bits)
        , pmask((1uz << partition_bits) - 1)
        , robin_hood_allowed(robin_hood_allowed_)
        , maps(1uz << partition_bits)
    {
        chassert(partition_bits >= 1 && partition_bits <= max_partition_bits);
    }

    [[nodiscard]] size_t numPartitions() const { return maps.size(); }
    [[nodiscard]] ALWAYS_INLINE size_t partitionOfHash(size_t hash_value) const { return (hash_value >> shift) & pmask; }

    /// Null until `createPartition`.
    Map * partitionOrNull(size_t p) { return maps[p].get(); }
    [[nodiscard]] const Map * partitionOrNull(size_t p) const { return maps[p].get(); }
    Map & partition(size_t p) { return *maps[p]; }
    [[nodiscard]] const Map & partition(size_t p) const { return *maps[p]; }

    void setReserveOnCreate(size_t keys)
    {
        if (!keys)
            return;
        typename Map::grower_type grower;
        grower.setRobinHoodAllowed(robin_hood_allowed);
        grower.set(keys);
        configureCommonCapacity(std::min(grower.bufSize(), 1uz << shift), grower.isRobinHood());
    }

    /// Called before any owner can create a map.
    void configureCommonCapacity(size_t cells, bool robin_hood)
    {
        chassert(!frozen && std::ranges::none_of(maps, [](const auto & map) { return map != nullptr; }));
        checkCapacity(cells);
        setCommonCapacity(cells);
        partition_robin_hood = robin_hood;
    }

    [[nodiscard]] size_t commonCapacity() const { return cell_mask + 1; }

    /// Only the owner of partition `p` creates it.
    bool createPartition(size_t p)
    {
        chassert(!frozen);
        if (maps[p])
            return false;
        typename Map::grower_type grower;
        grower.setExactCapacity(commonCapacity(), partition_robin_hood);
        maps[p] = std::make_unique<Map>(grower);
        return true;
    }

    /// The owner that exhausted its partition has already grown it. Bring every other allocated partition up to the same
    /// capacity after the owners finish; a later owner will create a missing partition at the new size.
    size_t ensureCommonCapacity(size_t cells)
    {
        if (cells <= commonCapacity())
            return 0;
        checkCapacity(cells);
        size_t added_bytes = 0;
        for (auto & map : maps)
        {
            if (!map || map->getBufferSizeInCells() == cells)
                continue;
            const size_t old_bytes = map->getBufferSizeInBytes();
            map->resize(0, cells);
            added_bytes += map->getBufferSizeInBytes() - old_bytes;
        }
        setCommonCapacity(cells);
        return added_bytes;
    }

    template <typename KeyHolder>
    ALWAYS_INLINE void emplaceInPartition(Map & map, KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash_value) const
    {
        if (size_t cells = growthForInsert(map, key_holder, hash_value); unlikely(cells))
            map.resize(0, cells);
        map.emplace(std::forward<KeyHolder>(key_holder), it, inserted, hash_value);
    }

    [[nodiscard]] ALWAYS_INLINE size_t hash(const key_type & key) const { return hasher(key); }

    /// After `freeze` only the zero key needs the map object.
    [[nodiscard]] ALWAYS_INLINE ConstLookupResult find(const key_type & key, size_t hash_value) const
    {
        chassert(frozen);
        const size_t p = partitionOfHash(hash_value);
        /// The zero key is never in the cells: the map keeps it aside.
        if (cell_type::isZero(key, no_state))
            return std::as_const(*maps[p]).find(key, hash_value);
        const Desc d = desc(p);
        if (!partition_robin_hood)
        {
            const cell_type * cell = d.cells + (hash_value & d.mask);
            while (!cell->isZero(no_state))
            {
                if (cell->keyEquals(key, hash_value, no_state))
                    return cell;
                cell = d.cells + ((cell - d.cells + 1) & d.mask);
            }
            return nullptr;
        }
        /// The miss rule of `HashTable::findCellRobinHood`, as the AMAC ring applies it. The keys of a chain are in the order
        /// of their home cells, so the key is not in the table if a cell's key is closer to its home than the key would be
        /// there. The rule does not apply in the key's home cell.
        size_t place = hash_value & d.mask;
        for (size_t distance = 0;; ++distance)
        {
            const cell_type * cell = d.cells + place;
            if (cell->isZero(no_state))
                return nullptr;
            if (cell->keyEquals(key, hash_value, no_state))
                return cell;
            /// The subtraction wraps when the home cell is after `place` in the buffer; the mask takes it modulo the table size.
            if (distance && (common::subIgnoreOverflow(place, cell->getHash(hasher)) & d.mask) < distance)
                return nullptr;
            place = (place + 1) & d.mask;
        }
    }
    [[nodiscard]] ALWAYS_INLINE ConstLookupResult find(const key_type & key) const { return find(key, hash(key)); }
    ALWAYS_INLINE LookupResult find(const key_type & key, size_t hash_value)
    {
        return const_cast<LookupResult>(std::as_const(*this).find(key, hash_value));
    }
    ALWAYS_INLINE LookupResult find(const key_type & key) { return find(key, hash(key)); }

    /// The prefetch of the probe, from the partition cell pointers. After `freeze`.
    template <typename KeyHolder>
    ALWAYS_INLINE void prefetch(KeyHolder && key_holder) const
    {
        const auto & key = keyHolderGetKey(key_holder);
        const size_t hash_value = hash(key);
        const Desc d = desc(partitionOfHash(hash_value));
        __builtin_prefetch(d.cells + (hash_value & d.mask));
        keyHolderDiscardKey(key_holder);
    }

    /// The prefetch of the insert of one build stream, before `freeze`: through the map of the partition, if it exists.
    template <typename KeyHolder>
    ALWAYS_INLINE void prefetchForInsert(KeyHolder && key_holder) const
    {
        const auto & key = keyHolderGetKey(key_holder);
        const size_t hash_value = hash(key);
        if (const Map * map = maps[partitionOfHash(hash_value)].get())
            map->prefetchByHash(hash_value);
        keyHolderDiscardKey(key_holder);
    }

    /// 0 for the zero key; otherwise the cell's position over all partitions plus one. After `freeze`.
    size_t offsetInternal(ConstLookupResult cell) const
    {
        if (cell->isZero(no_state))
            return 0;
        return offsetOf(cell, partitionOfHash(cell->getHash(hasher)));
    }
    ALWAYS_INLINE size_t offsetOf(const cell_type * cell, size_t p) const { return prefixOf(p) + (cell - hot[p]) + 1; }

    /// The insert of one build stream: puts the key into the partition of its hash, and creates the map of the partition at its
    /// first key.
    template <typename KeyHolder>
    ALWAYS_INLINE void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash_value)
    {
        chassert(!frozen, "An insert after `freeze` would leave the published pointers and byte count stale");
        const size_t p = partitionOfHash(hash_value);
        if (unlikely(!maps[p]))
            createPartition(p);
        Map & map = *maps[p];
        if (size_t cells = growthForInsert(map, key_holder, hash_value); unlikely(cells))
            ensureCommonCapacity(cells);
        map.emplace(std::forward<KeyHolder>(key_holder), it, inserted, hash_value);
    }
    template <typename KeyHolder>
    ALWAYS_INLINE void emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        const size_t hash_value = hash(keyHolderGetKey(key_holder));
        emplace(std::forward<KeyHolder>(key_holder), it, inserted, hash_value);
    }

    /// Publish the partition pointers only after the last insert.
    void freeze()
    {
        chassert(!frozen);
        hot.resize(numPartitions());
        frozen_bytes = 0;
        for (size_t p = 0; p < numPartitions(); ++p)
        {
            createPartition(p);
            const Map & map = *maps[p];
            if (map.getBufferSizeInCells() != commonCapacity())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash join partition {} has {} cells, expected {}",
                    p, map.getBufferSizeInCells(), commonCapacity());
            hot[p] = map.buf;
            frozen_bytes += map.getBufferSizeInBytes();
        }
        frozen = true;
    }
    [[nodiscard]] bool isFrozen() const { return frozen; }

    [[nodiscard]] Desc desc(size_t p) const { return {hot[p], cell_mask}; }
    [[nodiscard]] const cell_type * const * cellBases() const { return hot.data(); }
    [[nodiscard]] size_t cellBits() const { return cell_bits; }
    [[nodiscard]] size_t prefixOf(size_t p) const { return p << cell_bits; }
    [[nodiscard]] size_t partitionShift() const { return shift; }

    [[nodiscard]] bool isRobinHood() const { return partition_robin_hood; }

    /// Before `freeze`, these sum the maps that exist. After it, they read what `freeze` recorded, so that a gate of the probe
    /// does not walk every map on every call.
    [[nodiscard]] size_t getBufferSizeInBytes() const
    {
        if (frozen)
            return frozen_bytes;
        size_t res = 0;
        for (const auto & map : maps)
            if (map)
                res += map->getBufferSizeInBytes();
        return res;
    }
    [[nodiscard]] size_t getBufferSizeInCells() const
    {
        if (frozen)
            return numPartitions() << cell_bits;
        size_t res = 0;
        for (const auto & map : maps)
            if (map)
                res += map->getBufferSizeInCells();
        return res;
    }
    [[nodiscard]] size_t size() const
    {
        size_t res = 0;
        for (const auto & map : maps)
            if (map)
                res += map->size();
        return res;
    }
    [[nodiscard]] bool empty() const { return size() == 0; }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & map : maps)
            if (map)
                map->forEachMapped(func);
    }

private:
    static constexpr HashTableNoState no_state{};

    /// An absent key must always encounter an empty cell. A duplicate or the separately stored zero key can still
    /// be inserted when only one empty cell remains.
    template <typename KeyHolder>
    ALWAYS_INLINE size_t growthForInsert(const Map & map, KeyHolder & key_holder, size_t hash_value) const
    {
        const size_t cells = map.getBufferSizeInCells();
        if (likely(map.size() < cells - 1) || map.size() - map.hasZero() < cells - 1)
            return 0;
        const auto & key = keyHolderGetKey(key_holder);
        if (cell_type::isZero(key, no_state) || map.find(key, hash_value))
            return 0;
        checkCapacity(2 * cells);
        return 2 * cells;
    }

    void checkCapacity(size_t cells) const
    {
        if (!std::has_single_bit(cells) || cells < Map::grower_type::initial_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid hash join partition size of {} cells", cells);
        if (cells > (1uz << shift) || cells > std::numeric_limits<size_t>::max() / numPartitions() / sizeof(cell_type))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Hash join with {} partitions of {} cells is too large", numPartitions(), cells);
    }

    void setCommonCapacity(size_t cells)
    {
        cell_mask = cells - 1;
        cell_bits = std::countr_zero(cells);
    }

    size_t shift;
    size_t pmask;
    bool robin_hood_allowed;
    bool partition_robin_hood = false;
    size_t cell_mask = Map::grower_type::initial_count - 1;
    size_t cell_bits = std::countr_zero(Map::grower_type::initial_count);
    /// The hash of the maps. It is stateless, as every hash of the join's maps.
    [[no_unique_address]] JoinMapHashOf<Map> hasher;
    std::vector<std::unique_ptr<Map>> maps;
    VectorWithMemoryTracking<const cell_type *> hot;
    bool frozen = false;
    size_t frozen_bytes = 0;
};

/** The two forms of the table of one key type: the single form, the map planned with one partition, and the partitioned form.
  * Exactly one of them exists; a new table has the single form.
  */
template <typename Single, typename Partitioned>
struct JoinMapForms
{
    using single_type = Single;
    using partitioned_type = Partitioned;
    using key_type = typename Single::key_type;
    using mapped_type = typename Single::mapped_type;
    using value_type = typename Single::value_type;
    using cell_type = typename Single::cell_type;

    std::unique_ptr<Single> single = std::make_unique<Single>();
    std::unique_ptr<Partitioned> partitioned;

    [[nodiscard]] bool isPartitioned() const { return partitioned != nullptr; }

    /// Calls `f` with the form that exists.
    template <typename Self, typename F>
    ALWAYS_INLINE decltype(auto) visit(this Self && self, F && f)
    {
        if (self.partitioned)
            return std::forward<F>(f)(*self.partitioned);
        return std::forward<F>(f)(*self.single);
    }

    [[nodiscard]] size_t getBufferSizeInBytes() const
    {
        return visit([](const auto & form) { return form.getBufferSizeInBytes(); });
    }
    [[nodiscard]] size_t getBufferSizeInCells() const
    {
        return visit([](const auto & form) { return form.getBufferSizeInCells(); });
    }
    [[nodiscard]] size_t size() const
    {
        return visit([](const auto & form) { return form.size(); });
    }
    [[nodiscard]] bool empty() const { return size() == 0; }
    template <typename Func>
    void forEachMapped(Func && func)
    {
        visit([&](auto & form) { form.forEachMapped(func); });
    }
};

template <typename T>
constexpr bool is_join_partitioned_view = false;
template <typename Map>
constexpr bool is_join_partitioned_view<JoinPartitionedView<Map>> = true;

template <typename T>
constexpr bool is_join_map_forms = false;
template <typename Single, typename Partitioned>
constexpr bool is_join_map_forms<JoinMapForms<Single, Partitioned>> = true;

}
