#pragma once

#include <Common/ProfileEventsPagedExperiment/common.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace ProfileEvents::PagedExperimentStorage
{

class HotPaged
{
public:
    static constexpr const char * name = "hot_paged";

    explicit HotPaged(const Layout & layout_, size_t page_cells_ = 32)
        : layout(layout_)
        , page_cells(checkedPageCells(page_cells_))
        , hot(layout.hot_count)
        , pages((EventCount - layout.hot_count + page_cells - 1) / page_cells)
    {
    }

    HotPaged(const HotPaged &) = delete;
    HotPaged & operator=(const HotPaged &) = delete;

    ~HotPaged()
    {
        for (size_t i = 0; i < pages.size(); ++i)
        {
            if (auto * page = pages[i].load(std::memory_order_relaxed))
            {
                auto owner = Array<uint64_t>::adopt(page, page_cells);
            }
        }
    }

    void increment(Event event, uint64_t amount)
    {
        if (amount == 0)
            return;

        const size_t slot = layout.slot_of[event];
        if (slot < layout.hot_count)
        {
            std::atomic_ref<uint64_t>(hot[slot]).fetch_add(amount, std::memory_order_relaxed);
            return;
        }

        const size_t cold_slot = slot - layout.hot_count;
        auto & page_pointer = pages[cold_slot / page_cells];
        auto * page = page_pointer.load(std::memory_order_acquire);
        if (!page)
        {
            Array<uint64_t> fresh(page_cells);
            uint64_t * expected = nullptr;
            if (page_pointer.compare_exchange_strong(
                    expected, fresh.data(), std::memory_order_acq_rel, std::memory_order_acquire))
                page = fresh.release();
            else
                page = expected;
        }

        std::atomic_ref<uint64_t>(page[cold_slot % page_cells]).fetch_add(amount, std::memory_order_relaxed);
    }

    uint64_t load(Event event) const
    {
        const size_t slot = layout.slot_of[event];
        if (slot < layout.hot_count)
            return std::atomic_ref<uint64_t>(const_cast<uint64_t &>(hot[slot])).load(std::memory_order_relaxed);

        const size_t cold_slot = slot - layout.hot_count;
        const auto * page = pages[cold_slot / page_cells].load(std::memory_order_acquire);
        return page ? std::atomic_ref<uint64_t>(const_cast<uint64_t &>(page[cold_slot % page_cells])).load(std::memory_order_relaxed) : 0;
    }

    bool tryIncrementHot(Event event, uint64_t amount) noexcept
    {
        const size_t slot = layout.slot_of[event];
        if (slot >= layout.hot_count)
            return false;
        std::atomic_ref<uint64_t>(hot[slot]).fetch_add(amount, std::memory_order_relaxed);
        return true;
    }

    /// Reset values atomically; never unpublish or reclaim storage under readers.
    void resetCounters()
    {
        for (size_t i = 0; i < hot.size(); ++i)
            std::atomic_ref<uint64_t>(hot[i]).store(0, std::memory_order_relaxed);
        for (size_t i = 0; i < pages.size(); ++i)
        {
            if (auto * page = pages[i].load(std::memory_order_acquire))
            {
                for (size_t cell = 0; cell < page_cells; ++cell)
                    std::atomic_ref<uint64_t>(page[cell]).store(0, std::memory_order_relaxed);
            }
        }
    }

    /// Original event-by-event traversal retained for same-build comparisons.
    void snapshotBaseline(uint64_t * output) const
    {
        for (size_t event = 0; event < EventCount; ++event)
            output[event] = load(static_cast<Event>(event));
    }

    void snapshot(uint64_t * output) const
    {
        /// Absent cells must overwrite previous output, including for empty rows.
        if (layout.hot_count != EventCount)
            std::fill_n(output, EventCount, uint64_t{0});

        for (size_t slot = 0; slot < layout.hot_count; ++slot)
            output[layout.event_at_slot[slot]]
                = std::atomic_ref<uint64_t>(const_cast<uint64_t &>(hot[slot])).load(std::memory_order_relaxed);

        /// Acquire each stable page once, then visit its cells in storage order.
        for (size_t page_index = 0; page_index < pages.size(); ++page_index)
        {
            const auto * page = pages[page_index].load(std::memory_order_acquire);
            if (!page)
                continue;

            const size_t first_slot = layout.hot_count + page_index * page_cells;
            const size_t cells = std::min(page_cells, EventCount - first_slot);
            for (size_t cell = 0; cell < cells; ++cell)
                output[layout.event_at_slot[first_slot + cell]]
                    = std::atomic_ref<uint64_t>(const_cast<uint64_t &>(page[cell])).load(std::memory_order_relaxed);
        }
    }

    /// Diagnostics require quiescence, as does destruction. Published pages never move.
    MemoryUsage memory() const
    {
        auto result = hot.memory();
        result += pages.memory();
        for (size_t i = 0; i < pages.size(); ++i)
        {
            if (auto * page = pages[i].load(std::memory_order_relaxed))
                result += Array<uint64_t>::allocationMemory(page, page_cells);
        }
        return result;
    }

private:
    static size_t checkedPageCells(size_t value)
    {
        if (value == 0 || value > EventCount)
            throw std::invalid_argument("Page size must be between 1 and EventCount");
        return value;
    }

    const Layout & layout;
    const size_t page_cells;
    Array<uint64_t> hot;
    Array<std::atomic<uint64_t *>> pages;
};

}
