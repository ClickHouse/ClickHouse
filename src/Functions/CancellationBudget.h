#pragma once

#include <cstddef>
#include <functional>


namespace DB
{

/// Throttled cancellation checkpoint for a loop inside one function call. Lives on the stack of a single
/// vector call, so it is never shared between threads. Counted in loop ITERATIONS, not bytes: an iteration
/// can cost a full search or match attempt while advancing almost no data. Bytes are charged on top,
/// scaled down, because one iteration can also touch megabytes.
struct CancellationBudget
{
    /// About 30ms of granularity on the cheapest loop.
    static constexpr size_t units_per_check = 1ULL << 16;
    /// Bytes of data read or written that count as one unit of work.
    static constexpr size_t bytes_per_unit = 16;
    /// Units a caller may accumulate locally before charging them in one call.
    static constexpr size_t units_per_instruction_charge = 4096;

    explicit CancellationBudget(const std::function<void()> & check_)
        : check(check_ ? &check_ : nullptr) {}

    /// One iteration of an unbounded loop, plus the bytes of data that iteration touched.
    void charge(size_t bytes = 0) { chargeUnits(1 + bytes / bytes_per_unit); }

    /// Work that is not proportional to the amount of data, e.g. constructing a matcher from a pattern.
    void chargeUnits(size_t units)
    {
        if (units_left > units)
        {
            units_left -= units;
            return;
        }
        units_left = units_per_check;
        if (check)
            (*check)();
    }

private:
    const std::function<void()> * check;
    size_t units_left = units_per_check;
};

/// Observes the running query's elapsed-time limit and killed flag, and throws when either is reached.
/// Empty when there is no query to observe. Out of line so that including this header does not pull in
/// `ProcessList`.
std::function<void()> makeCancellationCheck(const char * function_name);

}
