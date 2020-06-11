#pragma once
#include <atomic>
#include <memory>

namespace DB
{

/// This class helps to calculate rows_before_limit_at_least.
class RowsBeforeLimitCounter
{
public:
    void add(uint64_t rows)
    {
        setAppliedLimit();
        rows_before_limit.fetch_add(rows, std::memory_order_release);
    }

    void set(uint64_t rows)
    {
        setAppliedLimit();
        rows_before_limit.store(rows, std::memory_order_release);
    }

    uint64_t get() const { return rows_before_limit.load(std::memory_order_acquire); }

    void setAppliedLimit() { has_applied_limit.store(true, std::memory_order_release); }
    bool hasAppliedLimit() const { return has_applied_limit.load(std::memory_order_acquire); }

private:
    std::atomic<uint64_t> rows_before_limit = 0;
    std::atomic_bool has_applied_limit = false;
};

using RowsBeforeLimitCounterPtr = std::shared_ptr<RowsBeforeLimitCounter>;

}
