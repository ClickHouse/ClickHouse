#include <Interpreters/FileCache/PendingPriorityIncreases.h>

#include <algorithm>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <vector>
#include <Common/CacheLine.h>
#include <Common/Exception.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Interpreters/FileCache/FileCache_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Ring buffers which many threads add to and one thread pops from. A writer takes the next position by incrementing
/// `write_position`, writes its increase into the slot, then sets the slot's `sequence` to mark it written. The reader
/// pops slots in order while they are marked written, and sets `sequence` so that each slot is free for the next pass.
class PendingPriorityIncreases::Buffers
{
public:
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
        /// The next pop starts after the buffer where this one stopped, so that all buffers are popped in turn, not always the first ones.
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
    /// Each slot on its own cache line, so that threads writing or popping neighbouring slots do not slow each other down.
    struct alignas(CH_CACHE_LINE_SIZE) Slot
    {
        /// For a write position `position` which maps to this slot, `sequence` equals `position` while the slot is free,
        /// `position + 1` once its increase is written, and `position + slots.size()` once popped (free for the next pass).
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
            /// If the move threw after the slot is taken, the slot would never be marked written, and the reader would stop at it forever.
            static_assert(std::is_nothrow_move_assignable_v<PendingPriorityIncrease>);

            size_t position = write_position.load(std::memory_order_relaxed);
            while (true)
            {
                auto & slot = slots[position % slots.size()];
                const size_t sequence = slot.sequence.load(std::memory_order_acquire);
                /// The slot still holds an increase from the previous pass, not popped yet: the buffer is full.
                if (sequence < position)
                    return false;

                if (sequence > position)
                {
                    /// Another writer has already taken this position, so retry with the current `write_position`.
                    position = write_position.load(std::memory_order_relaxed);
                    continue;
                }

                if (!write_position.compare_exchange_weak(position, position + 1, std::memory_order_relaxed))
                    continue;

                slot.increase = std::move(increase);
                slot.sequence.store(position + 1, std::memory_order_release);
                if (on_half_full && isHalfFullWith(position))
                    on_half_full();
                return true;
            }
        }

        /// Whether adding the increase at `position` made the buffer exactly half full. Checking for exactly half calls
        /// `on_half_full` once per filling. A filling skips the call if a pop left the buffer above half, or runs at the same time.
        bool isHalfFullWith(size_t position) const
        {
            return position + 1 - read_position.load(std::memory_order_relaxed) == (slots.size() + 1) / 2;
        }

        size_t pop(PriorityIncreases & increases, size_t max_increases)
        {
            static_assert(std::is_nothrow_move_constructible_v<PendingPriorityIncrease>);

            const size_t max_count = std::min({max_increases, slots.size(), PriorityIncreases::capacity() - increases.size()});
            size_t position = read_position.load(std::memory_order_relaxed);
            size_t count = 0;
            for (; count < max_count; ++count, ++position)
            {
                auto & slot = slots[position % slots.size()];
                /// Stop at the first slot which is not marked written: it is free, or a writer has taken it but not written it yet.
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
        std::vector<Slot> slots;
        alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> write_position = 0;
        /// Changed only by the reader. Atomic because writers read it to tell how full the buffer is.
        alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> read_position = 0;
    };

    Buffer & getBufferOfCurrentThread()
    {
        static std::atomic<size_t> threads_count = 0;
        static thread_local const size_t thread_index = threads_count.fetch_add(1, std::memory_order_relaxed);
        return buffers[thread_index % buffers.size()];
    }

    std::vector<Buffer> buffers;
    /// Used only by the reader, so not atomic.
    size_t first_buffer_to_pop = 0;
};

PendingPriorityIncreases::PendingPriorityIncreases() = default;
PendingPriorityIncreases::~PendingPriorityIncreases() = default;

PendingPriorityIncreases::Buffers & PendingPriorityIncreases::getOrAllocateBuffers(size_t buffer_size)
{
    if (auto * current_buffers = buffers.load(std::memory_order_acquire))
        return *current_buffers;

    /// `FileCacheSettings::validate` rejects such a size.
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
