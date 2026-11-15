#include <Columns/ShortCircuitArgumentStatistics.h>


namespace DB
{

void ShortCircuitArgumentStatistics::add(size_t executed_rows_, size_t undecided_rows_, UInt64 elapsed_nanoseconds_)
{
    if (executed_rows_ == 0)
        return;

    std::lock_guard lock(mutex);

    executed_rows += static_cast<double>(executed_rows_);
    decided_rows += static_cast<double>(executed_rows_ - undecided_rows_);
    elapsed_nanoseconds += static_cast<double>(elapsed_nanoseconds_);

    if (executed_rows > decay_threshold_rows)
    {
        executed_rows /= 2;
        decided_rows /= 2;
        elapsed_nanoseconds /= 2;
    }
}

double ShortCircuitArgumentStatistics::getRank() const
{
    std::lock_guard lock(mutex);

    if (executed_rows == 0)
        return -1;

    /// One row is added to the denominator to keep the rank finite for an argument that has not decided any row.
    /// The rank of such an argument is its total time, which is large compared to the time per decided row.
    return elapsed_nanoseconds / (decided_rows + 1);
}

}
