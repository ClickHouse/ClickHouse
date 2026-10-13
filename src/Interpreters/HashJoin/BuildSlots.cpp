#include <Interpreters/HashJoin/BuildSlots.h>

#include <thread>

namespace DB
{

void BuildSlots::insert(std::span<const ScatteredBlock::Selector> per_slot, absl::FunctionRef<void(size_t)> insert_slot)
{
    const size_t num_slots = slots.size();
    std::vector<char> pending(num_slots, 0);
    size_t slots_left = 0;
    for (size_t slot = 0; slot < num_slots; ++slot)
    {
        if (per_slot[slot].size() != 0)
        {
            pending[slot] = 1;
            ++slots_left;
        }
    }

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

        if (made_progress)
            continue;

        /// Yielding beats blocking: another slot of this block is probably free.
        std::this_thread::yield();
    }
}

}
