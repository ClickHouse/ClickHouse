#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <memory>
#include <mutex>


namespace DB
{

/** Statistics of a lazily executed argument of a short-circuit function with commutative arguments (`and`, `or`),
  * accumulated over the blocks the argument was executed on.
  *
  * The function executes its lazy arguments one after another, each one only on the rows that are still undecided
  * (have not encountered `false` for `and`, or `true` for `or`). For every argument it records how many rows it
  * was executed on, how many of them stayed undecided after it, and how long it took.
  *
  * From these, `getRank` computes the time spent per decided row. For independent arguments, executing them in
  * ascending order of this rank minimizes the expected total time: an argument with cost `c` per row that leaves
  * a fraction `p` of rows undecided should go before an argument with `c'` and `p'` if
  * `c + p * c' < c' + p' * c`, which is `c / (1 - p) < c' / (1 - p')`.
  *
  * The object is shared between the threads that execute the same expression, so it is protected by a mutex.
  * It is locked once per argument per block, which is negligible compared to the execution of the argument.
  */
class ShortCircuitArgumentStatistics
{
public:
    /// `executed_rows` - the number of rows the argument was executed on,
    /// `undecided_rows` - how many of them are still undecided after it.
    void add(size_t executed_rows, size_t undecided_rows, UInt64 elapsed_nanoseconds);

    /// Nanoseconds spent per decided row, or a negative value if the argument has not been executed yet.
    double getRank() const;

private:
    /// When the number of executed rows exceeds this threshold, all counters are halved,
    /// so that the statistics follow the changes in the data.
    static constexpr double decay_threshold_rows = 1 << 20;

    mutable std::mutex mutex;
    double executed_rows TSA_GUARDED_BY(mutex) = 0;
    double decided_rows TSA_GUARDED_BY(mutex) = 0;
    double elapsed_nanoseconds TSA_GUARDED_BY(mutex) = 0;
};

using ShortCircuitArgumentStatisticsPtr = std::shared_ptr<ShortCircuitArgumentStatistics>;

}
