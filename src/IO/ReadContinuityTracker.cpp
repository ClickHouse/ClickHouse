#include <IO/ReadContinuityTracker.h>

#include <algorithm>

namespace DB
{

void ReadContinuityTracker::recordReadRange(size_t start_pos, size_t len)
{
    /// A backward or far-forward jump (gap > bridgeable_gap) is a discontinuity: close the run first.
    if (last_pos && (start_pos < *last_pos || start_pos - *last_pos > options.bridgeable_gap))
        closeRun();
    if (!last_pos)
        run_start = start_pos;
    last_pos = start_pos + len;
}

void ReadContinuityTracker::recordSeek(size_t new_pos)
{
    /// A forward gap within bridgeable_gap keeps the run; any other jump closes it.
    if (last_pos && new_pos >= *last_pos && new_pos - *last_pos <= options.bridgeable_gap)
        return;
    closeRun();
    run_start = new_pos;
    last_pos = new_pos;
}

size_t ReadContinuityTracker::currentRun() const
{
    return last_pos ? *last_pos - run_start : 0;
}

size_t ReadContinuityTracker::predictedForwardLength() const
{
    return std::max<size_t>(currentRun(), static_cast<size_t>(expected_run));
}

void ReadContinuityTracker::closeRun()
{
    expected_run = options.ewma_alpha * static_cast<double>(currentRun())
        + (1.0 - options.ewma_alpha) * expected_run;
    run_start = 0;
    last_pos.reset();
}

}
