#pragma once
#include <atomic>
#include <memory>

namespace DB
{

/// This class helps to calculate rows_before_limit_at_least and rows_before_aggregation.
class RowsBeforeStepCounter
{
public:
    void add(uint64_t rows)
    {
        setAppliedStep();
        rows_before_step.fetch_add(rows, std::memory_order_release);
    }

    void set(uint64_t rows)
    {
        setAppliedStep();
        rows_before_step.store(rows, std::memory_order_release);
    }

    uint64_t get() const { return rows_before_step.load(std::memory_order_acquire); }

    void setAppliedStep() { has_applied_step.store(true, std::memory_order_release); }
    bool hasAppliedStep() const { return has_applied_step.load(std::memory_order_acquire); }

private:
    std::atomic<uint64_t> rows_before_step = 0;
    std::atomic_bool has_applied_step = false;
};

using RowsBeforeStepCounterPtr = std::shared_ptr<RowsBeforeStepCounter>;

}
