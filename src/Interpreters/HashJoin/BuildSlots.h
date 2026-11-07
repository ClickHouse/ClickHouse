#pragma once

#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Common/CacheLine.h>
#include <Common/HashTable/BucketPartitionedTable.h>

#include <absl/functional/function_ref.h>

#include <algorithm>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

namespace DB
{

/// The slots through which the build threads of a hash join insert into one map. Slot `s` holds bucket `b`
/// of the map when `b % size() == s`. Its lock guards those buckets, so threads that insert into different
/// slots do not wait for each other. `StorageJoin` clones share the slots of the join they read, but never
/// insert.
class BuildSlots
{
public:
    explicit BuildSlots(size_t num_slots)
        : slots(num_slots)
        , reserved(num_slots, 0)
    {
    }

    size_t size() const { return slots.size(); }

    /// The bytes of the buckets of `map` in `slot`. Only this slot's buffers: the others are growing under their
    /// own locks.
    template <typename Map>
    size_t slotBytes(const Map & map, size_t slot) const
    {
        /// A `PartitionedFixedHashTable` has one buffer, allocated when the table is created. Slot 0 counts it.
        if constexpr (is_partitioned_fixed_table<Map>)
            return slot == 0 ? map.getBufferSizeInBytes() : 0;
        else
        {
            size_t res = 0;
            for (size_t bucket = slot; bucket < Map::NUM_BUCKETS; bucket += slots.size())
                res += map.impls[bucket].getBufferSizeInBytes();
            return res;
        }
    }

    /// Reserving every bucket up front would be serial work before the build. So each slot reserves its
    /// share of `reserve_keys_` keys at its first insert, under its own lock.
    void setReserve(size_t reserve_keys_) { reserve_keys = reserve_keys_; }

    /// Call under the lock of `slot`. On the first insert of `slot`, reserves the slot's share of `reserve_keys`
    /// over the slot's buckets of `map`. Returns that share, or 0 when nothing was reserved.
    /// The caller uses the returned share only for `HashJoinPreallocatedElementsInHashTables`.
    template <typename Map>
    size_t reserveOnFirstInsert(Map & map, size_t slot)
    {
        /// A `PartitionedFixedHashTable` has all its cells from the start.
        if constexpr (is_partitioned_fixed_table<Map>)
            return 0;
        else
        {
            if (!reserve_keys || reserved[slot])
                return 0;
            for (size_t bucket = slot; bucket < Map::NUM_BUCKETS; bucket += slots.size())
                map.impls[bucket].reserve(reserve_keys / Map::NUM_BUCKETS);
            reserved[slot] = 1;
            return reserve_keys / slots.size();
        }
    }

    /// Calls `insert_slot(slot)` under the lock of every slot that `per_slot` gives rows to. Slots are taken in
    /// whatever order they come free.
    void insert(std::span<const ScatteredBlock::Selector> per_slot, absl::FunctionRef<void(size_t)> insert_slot)
    {
        const size_t num_slots = slots.size();
        std::vector<char> pending(num_slots);
        for (size_t slot = 0; slot < num_slots; ++slot)
            pending[slot] = per_slot[slot].size() != 0;
        size_t slots_left = std::ranges::count(pending, 1);

        while (slots_left > 0)
        {
            bool made_progress = false;

            for (size_t slot = 0; slot < num_slots; ++slot)
            {
                if (!pending[slot])
                    continue;

                std::unique_lock lock(slots[slot].mutex, std::try_to_lock);
                if (!lock.owns_lock())
                    continue;

                made_progress = true;
                insert_slot(slot);
                pending[slot] = 0;
                --slots_left;
            }

            /// Yielding beats blocking: another slot of this block is probably free.
            if (!made_progress)
                std::this_thread::yield();
        }
    }

private:
    struct alignas(CH_CACHE_LINE_SIZE) Slot
    {
        std::mutex mutex;
    };

    std::vector<Slot> slots;

    size_t reserve_keys = 0;
    /// Per slot. A slot's flag is used only under its lock.
    std::vector<char> reserved;
};

}
