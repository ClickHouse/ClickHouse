#pragma once

#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Common/Arena.h>
#include <Common/CacheLine.h>
#include <Common/HashTable/BucketPartitionedTable.h>

#include <absl/functional/function_ref.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <span>
#include <vector>

namespace DB
{

/// The slots that the build threads of a hash join insert through. Slot `s` holds bucket `b` of every
/// map of the join when `b % size() == s`. Its lock guards those buckets and the slot's arena, so
/// threads that insert into different slots do not wait for each other. `StorageJoin` clones share
/// the slots of the join they read, but never insert.
class BuildSlots
{
public:
    explicit BuildSlots(size_t num_slots)
        : slots(num_slots)
    {
    }

    size_t size() const { return slots.size(); }

    /// The slot count is a power of two and never exceeds the bucket count, so masking is enough.
    static size_t slotForBucket(size_t bucket, size_t num_slots) { return bucket & (num_slots - 1); }

    Arena & arena(size_t slot) { return *slots[slot].arena; }

    /// The bytes of the arena of `slot` and of its buckets of `map`. Only this slot's buffers: the others are
    /// growing under their own locks.
    template <typename Map>
    size_t slotBytes(const Map & map, size_t slot) const
    {
        size_t res = slots[slot].arena->allocatedBytes();
        forEachBucketOfSlot(slot, Map::NUM_BUCKETS, [&](size_t bucket) { res += getBucketBufferSizeInBytes(map, bucket); });
        return res;
    }

    size_t arenaBytes() const
    {
        size_t res = 0;
        for (const auto & slot : slots)
            if (slot.arena)
                res += slot.arena->allocatedBytes();
        return res;
    }

    size_t numArenasWithMemory() const { return std::ranges::count_if(slots, arenaHoldsMemory); }

    /// Moves out the arenas that hold memory. They are freed when the caller drops the result.
    std::vector<std::unique_ptr<Arena>> releaseArenas()
    {
        std::vector<std::unique_ptr<Arena>> arenas;
        for (auto & slot : slots)
            if (arenaHoldsMemory(slot))
                arenas.push_back(std::move(slot.arena));
        return arenas;
    }

    /// Reserving every bucket up front would be serial work before the build. So each slot reserves its
    /// share of `size_hint_` keys in a map at its first insert into that map, under its own lock.
    void setReserve(size_t num_maps, size_t size_hint_, size_t max_reserve_bytes_)
    {
        size_hint = size_hint_;
        max_reserve_bytes = max_reserve_bytes_;
        reserved.assign(num_maps, std::vector<char>(slots.size(), 0));
    }

    /// Call under the lock of `slot`; `map` is map `map_idx`. On the first insert of `slot` into it, reserves the
    /// slot's share of the size hint over the slot's buckets. Returns that share, or 0 when nothing was reserved.
    template <typename Map>
    size_t reserveOnFirstInsert(Map & map, size_t map_idx, size_t slot)
    {
        if (!size_hint || reserved[map_idx][slot])
            return 0;
        const size_t keys = clampReserve<Map>(size_hint, max_reserve_bytes);
        bool any_reserved = false;
        forEachBucketOfSlot(
            slot, Map::NUM_BUCKETS, [&](size_t bucket) { any_reserved |= reserveBucket(map, bucket, keys / Map::NUM_BUCKETS); });
        reserved[map_idx][slot] = 1;
        return any_reserved ? keys / slots.size() : 0;
    }

    /// Calls `insert_slot(slot)` under the lock of every slot that `per_slot` gives rows to. Slots are taken in
    /// whatever order they come free, from slot `start % size()` on, so that concurrent build threads do not all
    /// queue behind slot 0.
    void insert(std::span<const ScatteredBlock::Selector> per_slot, size_t start, absl::FunctionRef<void(size_t)> insert_slot);

private:
    template <typename F>
    void forEachBucketOfSlot(size_t slot, size_t num_buckets, F && f) const
    {
        for (size_t bucket = slot; bucket < num_buckets; bucket += slots.size())
            f(bucket);
    }

    /// A statistics-derived reserve is an estimate, so cap it at the spill budget.
    template <typename Table>
    static size_t clampReserve(size_t reserve, size_t max_reserve_bytes)
    {
        /// `HashTableGrowerWithPrecalculation::set` keeps a buffer at most half full and rounds its size up to a power of two.
        static constexpr size_t max_cells_per_key = 4;
        /// `SpillingHashJoin` switches to `GraceHashJoin` at half of the budget, so the reserve stays under that half.
        static constexpr size_t budget_divisor = 2;

        if (!max_reserve_bytes)
            return reserve;
        return std::min(reserve, max_reserve_bytes / (budget_divisor * max_cells_per_key * sizeof(typename Table::cell_type)));
    }

    struct alignas(CH_CACHE_LINE_SIZE) Slot
    {
        std::mutex mutex;
        /// Strings for string keys, and continuation nodes of single-linked lists of row refs. One arena
        /// per slot, because `Arena` is unsynchronized. Splitting is sound because neither allocation
        /// kind needs contiguity or rollback. Behind a pointer, so it can be moved out and freed alone.
        std::unique_ptr<Arena> arena = std::make_unique<Arena>();
    };

    /// An arena allocates its first chunk on its first use, so the arena of a slot that never inserted holds nothing.
    static bool arenaHoldsMemory(const Slot & slot) { return slot.arena && slot.arena->allocatedBytes() != 0; }

    std::vector<Slot> slots;

    size_t size_hint = 0;
    size_t max_reserve_bytes = 0;
    /// Per map, per slot. A slot's flags are used only under its lock.
    std::vector<std::vector<char>> reserved;
};

}
