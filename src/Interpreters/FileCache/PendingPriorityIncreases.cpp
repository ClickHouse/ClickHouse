#include <Interpreters/FileCache/PendingPriorityIncreases.h>

#include <algorithm>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <vector>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Interpreters/FileCache/FileCache_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Bounded ring buffers with a sequence number per slot. A writer claims a position by advancing `write_position`,
/// writes its increase, then publishes it by setting the slot's sequence. The reader takes slots in order while
/// their sequence shows they are written, and frees each slot for the next pass around the ring.
struct PendingPriorityIncreases::Buffers
{
    struct Slot
    {
        /// Equals the write position of the slot while it is free, and that position + 1 once its increase is written.
        std::atomic<size_t> sequence = 0;
        PendingPriorityIncrease increase;
    };

    class Buffer
    {
    public:
        void createSlots(size_t slots_count)
        {
            slots = std::vector<Slot>(slots_count);
            for (size_t i = 0; i < slots.size(); ++i)
                slots[i].sequence.store(i, std::memory_order_relaxed);
        }

        bool tryAdd(PendingPriorityIncrease & increase, const std::function<void()> & on_half_full)
        {
            /// A throw between claiming a slot and publishing it would block the reader of this buffer forever.
            static_assert(std::is_nothrow_move_assignable_v<PendingPriorityIncrease>);

            size_t position = write_position.load(std::memory_order_relaxed);
            while (true)
            {
                auto & slot = slots[position % slots.size()];
                const auto distance = static_cast<Int64>(slot.sequence.load(std::memory_order_acquire) - position);
                if (distance < 0)
                    return false;

                if (distance > 0)
                {
                    /// Another thread adding to this buffer has claimed the slot.
                    position = write_position.load(std::memory_order_relaxed);
                    continue;
                }

                if (!write_position.compare_exchange_weak(position, position + 1, std::memory_order_relaxed))
                    continue;

                slot.increase = std::move(increase);
                slot.sequence.store(position + 1, std::memory_order_release);
                /// Counted from the claimed position, so that usually one add per filling of the buffer reaches half.
                /// A filling which starts above half, or which overlaps a pop, does not wake the task.
                const size_t size_with_this_increase = position + 1 - read_position.load(std::memory_order_relaxed);
                if (size_with_this_increase == (slots.size() + 1) / 2 && on_half_full)
                    on_half_full();
                return true;
            }
        }

        /// Pops at most one buffer of increases, so that a call ends while hits keep adding.
        size_t pop(PriorityIncreases & increases, size_t max_increases)
        {
            static_assert(std::is_nothrow_move_constructible_v<PendingPriorityIncrease>);

            const size_t max_count = std::min({max_increases, slots.size(), increases.capacity() - increases.size()});
            size_t position = read_position.load(std::memory_order_relaxed);
            size_t count = 0;
            for (; count < max_count; ++count, ++position)
            {
                auto & slot = slots[position % slots.size()];
                /// Stop at a free slot, and at a slot which a writer has claimed but not written yet.
                if (slot.sequence.load(std::memory_order_acquire) != position + 1)
                    break;

                increases.push_back(std::move(slot.increase));
                slot.sequence.store(position + slots.size(), std::memory_order_release);
            }
            read_position.store(position, std::memory_order_relaxed);
            return count;
        }

        size_t popAll(PriorityIncreases & increases) { return pop(increases, slots.size()); }

        [[nodiscard]] size_t getSizeApprox() const
        {
            const size_t read = read_position.load(std::memory_order_relaxed);
            const size_t write = write_position.load(std::memory_order_relaxed);
            return write > read ? write - read : 0;
        }

    private:
        std::atomic<size_t> write_position = 0;
        /// Changed only by the reader. Atomic because writers read it to tell how full the buffer is.
        std::atomic<size_t> read_position = 0;
        std::vector<Slot> slots;
    };

    Buffers(size_t buffers_count, size_t buffer_size)
        : buffers(buffers_count)
    {
        for (auto & buffer : buffers)
            buffer.createSlots(buffer_size);
    }

    bool tryAdd(PendingPriorityIncrease & increase, const std::function<void()> & on_half_full)
    {
        return getBufferOfCurrentThread().tryAdd(increase, on_half_full);
    }

    void pop(PriorityIncreases & increases, size_t max_increases)
    {
        size_t i = 0;
        for (; i < buffers.size(); ++i)
        {
            max_increases -= buffers[(first_buffer_to_pop + i) % buffers.size()].pop(increases, max_increases);
            if (!max_increases)
                break;
        }
        /// Start the next pop after the buffer which this one stopped at, so that every buffer is reached in one pass.
        first_buffer_to_pop = (first_buffer_to_pop + i + 1) % buffers.size();
    }

    void popBufferOfCurrentThread(PriorityIncreases & increases) { getBufferOfCurrentThread().popAll(increases); }

    [[nodiscard]] size_t getSizeApprox() const
    {
        size_t size = 0;
        for (const auto & buffer : buffers)
            size += buffer.getSizeApprox();
        return size;
    }

private:
    Buffer & getBufferOfCurrentThread()
    {
        static std::atomic<size_t> threads_count = 0;
        static thread_local const size_t thread_index = threads_count.fetch_add(1, std::memory_order_relaxed);
        return buffers[thread_index % buffers.size()];
    }

    std::vector<Buffer> buffers;
    /// Changed on every pop.
    size_t first_buffer_to_pop = 0;
};

PendingPriorityIncreases::PendingPriorityIncreases() = default;
PendingPriorityIncreases::~PendingPriorityIncreases() = default;

PendingPriorityIncreases::Buffers & PendingPriorityIncreases::getOrAllocateBuffers(size_t buffer_size)
{
    if (auto * current_buffers = buffers.load(std::memory_order_acquire))
        return *current_buffers;

    /// `FileCacheSettings::validate` rejects such a size, so invalid buffer size should not be here at this point.
    if (buffer_size < FILECACHE_MIN_BATCH_PRIORITY_INCREASE_BUFFER_SIZE || buffer_size > FILECACHE_MAX_BATCH_PRIORITY_INCREASE_BUFFER_SIZE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Buffer size of priority increases is out of range: {}", buffer_size);

    auto created = std::make_unique<Buffers>(std::max(1u, getNumberOfCPUCoresToUse()), buffer_size);
    Buffers * expected = nullptr;
    if (buffers.compare_exchange_strong(expected, created.get(), std::memory_order_acq_rel))
    {
        allocated_buffers = std::move(created);
        return *allocated_buffers;
    }
    /// Another hit allocated them first.
    return *expected;
}

bool PendingPriorityIncreases::tryAdd(PendingPriorityIncrease & increase, size_t buffer_size, const std::function<void()> & on_half_full)
{
    return getOrAllocateBuffers(buffer_size).tryAdd(increase, on_half_full);
}

void PendingPriorityIncreases::pop(PriorityIncreases & increases, size_t max_increases)
{
    if (auto * current_buffers = buffers.load(std::memory_order_acquire))
        current_buffers->pop(increases, max_increases);
}

void PendingPriorityIncreases::popBufferOfCurrentThread(PriorityIncreases & increases)
{
    if (auto * current_buffers = buffers.load(std::memory_order_acquire))
        current_buffers->popBufferOfCurrentThread(increases);
}

size_t PendingPriorityIncreases::getSizeApprox() const
{
    const auto * current_buffers = buffers.load(std::memory_order_acquire);
    return current_buffers ? current_buffers->getSizeApprox() : 0;
}

}
