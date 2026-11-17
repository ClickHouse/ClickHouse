#include <Interpreters/FileCache/PendingPriorityIncreases.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>
#include <base/defines.h>
#include <Common/CacheLine.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Interpreters/FileCache/FileCache_fwd.h>

namespace DB
{

/// As many stripes as CPU cores, so that hits of different threads rarely share a ring or a cache line.
struct PendingPriorityIncreases::Stripes
{
    struct Slot
    {
        /// Equals the write position of the slot while it is free, and that position + 1 once its record is written.
        std::atomic<size_t> sequence = 0;
        PendingPriorityIncrease record;
    };

    struct alignas(CH_CACHE_LINE_SIZE) Stripe
    {
        void createSlots(size_t slots_count)
        {
            slots = std::vector<Slot>(slots_count);
            for (size_t i = 0; i < slots.size(); ++i)
                slots[i].sequence.store(i, std::memory_order_relaxed);
        }

        [[nodiscard]] size_t getSizeApprox() const
        {
            const size_t read = read_position.load(std::memory_order_relaxed);
            const size_t write = write_position.load(std::memory_order_relaxed);
            return write > read ? write - read : 0;
        }

        /// Pops at most one ring of records, so that a call ends while hits keep adding.
        size_t popRecords(std::vector<PendingPriorityIncrease> & records, size_t max_records)
        {
            static_assert(std::is_nothrow_move_constructible_v<PendingPriorityIncrease>);

            const size_t max_count = std::min(max_records, slots.size());
            /// Reserve first, so that nothing below throws once a record has left its slot.
            records.reserve(records.size() + max_count);

            size_t position = read_position.load(std::memory_order_relaxed);
            size_t count = 0;
            for (; count < max_count; ++count, ++position)
            {
                auto & slot = slots[position % slots.size()];
                /// Stop at a free slot, and at a slot which a writer has claimed but not written yet.
                if (slot.sequence.load(std::memory_order_acquire) != position + 1)
                    break;

                records.push_back(std::move(slot.record));
                slot.sequence.store(position + slots.size(), std::memory_order_release);
            }
            read_position.store(position, std::memory_order_relaxed);
            return count;
        }

        std::atomic<size_t> write_position = 0;
        /// Changed only by the reader. Atomic because writers read it to tell how full the ring is.
        alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> read_position = 0;
        std::vector<Slot> slots;
    };

    Stripes(size_t stripes_count, size_t records_per_stripe_)
        : records_per_stripe(records_per_stripe_)
        , stripes(stripes_count)
    {
        for (auto & stripe : stripes)
            stripe.createSlots(records_per_stripe);
    }

    Stripe & getStripeOfCurrentThread()
    {
        static std::atomic<size_t> threads_count = 0;
        static thread_local const size_t thread_index = threads_count.fetch_add(1, std::memory_order_relaxed);
        return stripes[thread_index % stripes.size()];
    }

    const size_t records_per_stripe;
    std::vector<Stripe> stripes;
};

PendingPriorityIncreases::~PendingPriorityIncreases()
{
    delete stripes.load(std::memory_order_relaxed);
}

void PendingPriorityIncreases::allocateIfNeeded(size_t buffer_size)
{
    if (stripes.load(std::memory_order_acquire))
        return;

    chassert(buffer_size >= FILECACHE_MIN_BATCH_PRIORITY_INCREASE_BUFFER_SIZE);
    auto created = std::make_unique<Stripes>(
        std::max(1u, getNumberOfCPUCoresToUse()), std::max(FILECACHE_MIN_BATCH_PRIORITY_INCREASE_BUFFER_SIZE, buffer_size));
    Stripes * expected = nullptr;
    if (stripes.compare_exchange_strong(expected, created.get(), std::memory_order_acq_rel))
        created.release(); /// NOLINT(bugprone-unused-return-value)
}

PendingPriorityIncreases::AddResult PendingPriorityIncreases::add(PendingPriorityIncrease & record)
{
    auto & current_stripes = *stripes.load(std::memory_order_acquire);
    auto & stripe = current_stripes.getStripeOfCurrentThread();
    const size_t records_per_stripe = current_stripes.records_per_stripe;

    size_t position = stripe.write_position.load(std::memory_order_relaxed);
    while (true)
    {
        auto & slot = stripe.slots[position % records_per_stripe];
        const auto distance = static_cast<std::ptrdiff_t>(slot.sequence.load(std::memory_order_acquire) - position);
        if (distance < 0)
            return AddResult::Full;

        if (distance > 0)
        {
            /// Another thread of this stripe has claimed the slot.
            position = stripe.write_position.load(std::memory_order_relaxed);
            continue;
        }

        if (!stripe.write_position.compare_exchange_weak(position, position + 1, std::memory_order_relaxed))
            continue;

        slot.record = std::move(record);
        slot.sequence.store(position + 1, std::memory_order_release);
        return stripe.getSizeApprox() >= (records_per_stripe + 1) / 2 ? AddResult::AddedAndHalfFull : AddResult::Added;
    }
}

void PendingPriorityIncreases::popRecords(std::vector<PendingPriorityIncrease> & records, size_t max_records)
{
    auto * current_stripes = stripes.load(std::memory_order_acquire);
    if (!current_stripes)
        return;

    const size_t stripes_count = current_stripes->stripes.size();
    for (size_t i = 0; i < stripes_count && max_records; ++i)
        max_records -= current_stripes->stripes[(first_stripe_to_pop + i) % stripes_count].popRecords(records, max_records);
    first_stripe_to_pop = (first_stripe_to_pop + 1) % stripes_count;
}

void PendingPriorityIncreases::popAllRecords(std::vector<PendingPriorityIncrease> & records)
{
    popRecords(records, std::numeric_limits<size_t>::max());
}

void PendingPriorityIncreases::popRecordsOfCurrentThread(std::vector<PendingPriorityIncrease> & records)
{
    if (auto * current_stripes = stripes.load(std::memory_order_acquire))
        current_stripes->getStripeOfCurrentThread().popRecords(records, std::numeric_limits<size_t>::max());
}

size_t PendingPriorityIncreases::getSizeApprox() const
{
    const auto * current_stripes = stripes.load(std::memory_order_acquire);
    if (!current_stripes)
        return 0;

    size_t size = 0;
    for (const auto & stripe : current_stripes->stripes)
        size += stripe.getSizeApprox();
    return size;
}

}
