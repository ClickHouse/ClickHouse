#pragma once

#include <atomic>
#include <cstddef>
#include <limits>

#include <base/types.h>

namespace DB
{

/// Thread-safe, nonnegative row budget used to throttle an optional evaluation: an evaluation that performed poorly
/// replenishes it, and the blocks that come next consume it, skipping the evaluation until it is used up.
class RowsSkipBudget
{
public:
    /// Add `rows * multiplier` to the budget, saturating at the maximum representable value.
    void replenish(UInt64 rows, UInt64 multiplier)
    {
        if (rows == 0 || multiplier == 0)
            return;

        const Int64 max_rows_to_skip = std::numeric_limits<Int64>::max();
        const UInt64 max_rows_to_skip_unsigned = static_cast<UInt64>(max_rows_to_skip);
        const UInt64 rows_to_add = rows > max_rows_to_skip_unsigned / multiplier ? max_rows_to_skip_unsigned : rows * multiplier;
        const Int64 increment = static_cast<Int64>(rows_to_add);

        Int64 current_rows_to_skip = rows_to_skip.load(std::memory_order_relaxed);
        while (current_rows_to_skip < max_rows_to_skip)
        {
            const Int64 available = max_rows_to_skip - current_rows_to_skip;
            const Int64 updated_rows_to_skip = increment >= available ? max_rows_to_skip : current_rows_to_skip + increment;
            if (rows_to_skip.compare_exchange_weak(
                    current_rows_to_skip, updated_rows_to_skip, std::memory_order_relaxed, std::memory_order_relaxed))
                return;
        }
    }

    /// Consume `rows` and return whether enough budget remains to skip the current block.
    bool consume(size_t rows)
    {
        Int64 current_rows_to_skip = rows_to_skip.load(std::memory_order_relaxed);
        while (current_rows_to_skip > 0)
        {
            const bool should_skip = rows < static_cast<UInt64>(current_rows_to_skip);
            const Int64 remaining_rows_to_skip = should_skip ? current_rows_to_skip - static_cast<Int64>(rows) : 0;
            if (rows_to_skip.compare_exchange_weak(
                    current_rows_to_skip, remaining_rows_to_skip, std::memory_order_relaxed, std::memory_order_relaxed))
                return should_skip;
        }

        return false;
    }

private:
    std::atomic<Int64> rows_to_skip = 0;
};

}
