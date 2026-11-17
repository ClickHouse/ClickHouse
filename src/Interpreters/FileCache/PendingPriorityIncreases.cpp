#include <Interpreters/FileCache/PendingPriorityIncreases.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/CacheLine.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Interpreters/FileCache/FileCache_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct PendingPriorityIncreases::Buffers
{
    struct Slot
    {
        /// Equals the write position of the slot while it is free, and that position + 1 once its increase is written.
        std::atomic<size_t> sequence = 0;
        PendingPriorityIncrease increase;
    };

    struct alignas(CH_CACHE_LINE_SIZE) Buffer
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

        /// Pops at most one buffer of increases, so that a call ends while hits keep adding.
        size_t pop(std::vector<PendingPriorityIncrease> & increases, size_t max_increases)
        {
            static_assert(std::is_nothrow_move_constructible_v<PendingPriorityIncrease>);

            const size_t max_count = std::min(max_increases, slots.size());
            /// The caller reserves, so that nothing here throws once an increase has left its slot.
            chassert(increases.capacity() - increases.size() >= max_count);

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

        std::atomic<size_t> write_position = 0;
        /// Changed only by the reader. Atomic because writers read it to tell how full the buffer is.
        alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> read_position = 0;
        std::vector<Slot> slots;
    };

    Buffers(size_t buffers_count, size_t buffer_size_)
        : buffer_size(buffer_size_)
        , buffers(buffers_count)
    {
        for (auto & buffer : buffers)
            buffer.createSlots(buffer_size);
    }

    Buffer & getBufferOfCurrentThread()
    {
        static std::atomic<size_t> threads_count = 0;
        static thread_local const size_t thread_index = threads_count.fetch_add(1, std::memory_order_relaxed);
        return buffers[thread_index % buffers.size()];
    }

    const size_t buffer_size;
    std::vector<Buffer> buffers;
};

PendingPriorityIncreases::~PendingPriorityIncreases()
{
    delete buffers.load(std::memory_order_relaxed);
}

void PendingPriorityIncreases::allocateIfNeeded(size_t buffer_size)
{
    if (buffers.load(std::memory_order_acquire))
        return;

    /// `FileCacheSettings::validate` rejects such a size, but programmatically built settings skip it.
    if (buffer_size < FILECACHE_MIN_BATCH_PRIORITY_INCREASE_BUFFER_SIZE || buffer_size > FILECACHE_MAX_BATCH_PRIORITY_INCREASE_BUFFER_SIZE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Buffer size of priority increases is out of range: {}", buffer_size);

    auto created = std::make_unique<Buffers>(std::max(1u, getNumberOfCPUCoresToUse()), buffer_size);
    Buffers * expected = nullptr;
    if (buffers.compare_exchange_strong(expected, created.get(), std::memory_order_acq_rel))
        created.release(); /// NOLINT(bugprone-unused-return-value)
}

PendingPriorityIncreases::AddResult PendingPriorityIncreases::add(PendingPriorityIncrease & increase)
{
    auto & current_buffers = *buffers.load(std::memory_order_acquire);
    auto & buffer = current_buffers.getBufferOfCurrentThread();
    const size_t buffer_size = current_buffers.buffer_size;

    size_t position = buffer.write_position.load(std::memory_order_relaxed);
    while (true)
    {
        auto & slot = buffer.slots[position % buffer_size];
        const auto distance = static_cast<std::ptrdiff_t>(slot.sequence.load(std::memory_order_acquire) - position);
        if (distance < 0)
            return AddResult::Full;

        if (distance > 0)
        {
            /// Another thread adding to this buffer has claimed the slot.
            position = buffer.write_position.load(std::memory_order_relaxed);
            continue;
        }

        if (!buffer.write_position.compare_exchange_weak(position, position + 1, std::memory_order_relaxed))
            continue;

        slot.increase = std::move(increase);
        slot.sequence.store(position + 1, std::memory_order_release);
        return buffer.getSizeApprox() >= (buffer_size + 1) / 2 ? AddResult::AddedAtLeastHalfFull : AddResult::Added;
    }
}

void PendingPriorityIncreases::pop(std::vector<PendingPriorityIncrease> & increases, size_t max_increases)
{
    auto * current_buffers = buffers.load(std::memory_order_acquire);
    if (!current_buffers)
        return;

    const size_t buffers_count = current_buffers->buffers.size();
    increases.reserve(increases.size() + std::min(max_increases, buffers_count * current_buffers->buffer_size));
    for (size_t i = 0; i < buffers_count && max_increases; ++i)
        max_increases -= current_buffers->buffers[(first_buffer_to_pop + i) % buffers_count].pop(increases, max_increases);
    first_buffer_to_pop = (first_buffer_to_pop + 1) % buffers_count;
}

void PendingPriorityIncreases::popBufferOfCurrentThread(std::vector<PendingPriorityIncrease> & increases)
{
    if (auto * current_buffers = buffers.load(std::memory_order_acquire))
    {
        increases.reserve(increases.size() + current_buffers->buffer_size);
        current_buffers->getBufferOfCurrentThread().pop(increases, std::numeric_limits<size_t>::max());
    }
}

size_t PendingPriorityIncreases::getSizeApprox() const
{
    const auto * current_buffers = buffers.load(std::memory_order_acquire);
    if (!current_buffers)
        return 0;

    size_t size = 0;
    for (const auto & buffer : current_buffers->buffers)
        size += buffer.getSizeApprox();
    return size;
}

}
