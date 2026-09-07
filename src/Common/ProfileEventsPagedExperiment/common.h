#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <span>
#include <utility>
#include <vector>

#if defined(__APPLE__)
#include <malloc/malloc.h>
#elif defined(__linux__)
#include <malloc.h>
#else
#error Unsupported allocator diagnostics platform
#endif

namespace ProfileEvents::PagedExperimentStorage
{
using Event = uint16_t;
#ifndef COMPACT_COUNTER_EVENT_COUNT
#define COMPACT_COUNTER_EVENT_COUNT 1562
#endif
inline constexpr size_t EventCount = COMPACT_COUNTER_EVENT_COUNT;
static_assert(EventCount > 0 && EventCount <= 65535);

struct MemoryUsage
{
    size_t requested = 0;
    size_t usable = 0;
    size_t allocations = 0;

    MemoryUsage & operator+=(const MemoryUsage & other)
    {
        requested += other.requested;
        usable += other.usable;
        allocations += other.allocations;
        return *this;
    }
};

struct Layout
{
    size_t hot_count;
    std::array<Event, EventCount> slot_of{};
    /// Shared inverse permutation for traversal; no per-counter allocation.
    std::array<Event, EventCount> event_at_slot{};

    /// Stable partition: mandatory events first, then the remaining frequency rank.
    /// Invalid reservations leave the layout unchanged.
    bool reserveHotEvents(std::span<const Event> required) noexcept
    {
        if (required.size() > hot_count)
            return false;

        std::array<bool, EventCount> reserved{};
        for (const Event event : required)
        {
            if (event >= EventCount || reserved[event])
                return false;
            reserved[event] = true;
        }

        std::array<Event, EventCount> ordered{};
        size_t position = 0;
        for (const Event event : event_at_slot)
            if (reserved[event])
                ordered[position++] = event;
        for (const Event event : event_at_slot)
            if (!reserved[event])
                ordered[position++] = event;

        event_at_slot = ordered;
        for (size_t slot = 0; slot < EventCount; ++slot)
            slot_of[event_at_slot[slot]] = static_cast<Event>(slot);
        return true;
    }

    explicit Layout(size_t hot_count_, const std::vector<Event> & permutation = {})
        : hot_count(hot_count_)
    {
        if (hot_count > EventCount || (!permutation.empty() && permutation.size() != EventCount))
            throw std::invalid_argument("Invalid counter layout size");
        std::array<bool, EventCount> seen{};
        for (size_t slot = 0; slot < EventCount; ++slot)
        {
            Event event = permutation.empty() ? static_cast<Event>(slot) : permutation[slot];
            if (event >= EventCount || seen[event])
                throw std::invalid_argument("Counter layout must be a permutation");
            seen[event] = true;
            slot_of[event] = static_cast<Event>(slot);
            event_at_slot[slot] = event;
        }
    }
};

template <class T>
class Array
{
public:
    Array() = default;
    explicit Array(size_t count_) : count(count_)
    {
        if (!count)
            return;
        ptr = static_cast<T *>(::operator new(count * sizeof(T), std::align_val_t{Alignment}));
        try
        {
            std::uninitialized_value_construct_n(ptr, count);
        }
        catch (...)
        {
            deallocate(ptr);
            throw;
        }
    }

    ~Array()
    {
        clear();
    }
    Array(const Array &) = delete;
    Array & operator=(const Array &) = delete;

    Array(Array && other) noexcept
        : ptr(std::exchange(other.ptr, nullptr)), count(std::exchange(other.count, 0))
    {
    }
    Array & operator=(Array && other) noexcept
    {
        if (this != &other)
        {
            clear();
            ptr = std::exchange(other.ptr, nullptr);
            count = std::exchange(other.count, 0);
        }
        return *this;
    }

    T * data()
    {
        return ptr;
    }
    const T * data() const
    {
        return ptr;
    }
    size_t size() const
    {
        return count;
    }
    T & operator[](size_t i)
    {
        return ptr[i];
    }
    const T & operator[](size_t i) const
    {
        return ptr[i];
    }
    T * release()
    {
        count = 0;
        return std::exchange(ptr, nullptr);
    }

    static Array adopt(T * pointer, size_t count_)
    {
        Array result;
        result.ptr = pointer;
        result.count = pointer ? count_ : 0;
        return result;
    }

    static MemoryUsage allocationMemory(const T * pointer, size_t count_)
    {
        if (!pointer)
            return {};
#if defined(__APPLE__)
        size_t usable = malloc_size(pointer);
#else
        size_t usable = malloc_usable_size(const_cast<T *>(pointer));
#endif
        return {count_ * sizeof(T), usable, 1};
    }

    MemoryUsage memory() const
    {
        return allocationMemory(ptr, count);
    }

private:
    static constexpr size_t Alignment = std::max(size_t{64}, alignof(T));
    T * ptr = nullptr;
    size_t count = 0;

    static void deallocate(T * pointer)
    {
        ::operator delete(pointer, std::align_val_t{Alignment});
    }

    void clear()
    {
        if (ptr)
        {
            std::destroy_n(ptr, count);
            deallocate(ptr);
        }
        ptr = nullptr;
        count = 0;
    }
};
}
