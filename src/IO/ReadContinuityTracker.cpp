#include <IO/ReadContinuityTracker.h>

#include <algorithm>

namespace DB
{

void ReadContinuityTracker::recordReadRange(size_t start_pos, size_t len)
{
    /// A past range re-declares an already-fed span; feed only its new tail (backward jumps
    /// come via `recordSeek`).
    if (last_pos && start_pos < *last_pos)
    {
        if (start_pos + len <= *last_pos)
            return;
        len = start_pos + len - *last_pos;
        start_pos = *last_pos;
    }
    /// A far-forward jump (gap > bridgeable_gap) is a discontinuity.
    if (last_pos && start_pos - *last_pos > options.bridgeable_gap)
        closeRun();
    const bool exact_continuation = last_pos && start_pos == *last_pos && *last_pos != run_start;
    if (!last_pos)
        run_start = start_pos;
    last_pos = start_pos + len;
    /// Warm the estimate on each confirmed continuation, else a first unbroken scan (whose run
    /// never closes) would stay at the floor forever.
    if (exact_continuation)
        checkpointRun();
}

void ReadContinuityTracker::recordSeek(size_t new_pos)
{
    /// A gapless seek (to the exact frontier) is the same positive continuity
    /// signal as an exact-continuation serve: checkpoint, keep the run.
    if (last_pos && new_pos == *last_pos)
    {
        if (*last_pos != run_start)
            checkpointRun();
        return;
    }
    /// A forward gap within bridgeable_gap keeps the run; any other jump closes it.
    if (last_pos && new_pos >= *last_pos && new_pos - *last_pos <= options.bridgeable_gap)
        return;
    closeRun();
    run_start = new_pos;
    last_pos = new_pos;
}

double ReadContinuityTracker::foldedEstimate() const
{
    return options.ewma_alpha * static_cast<double>(currentRun())
        + (1.0 - options.ewma_alpha) * expected_run;
}

void ReadContinuityTracker::checkpointRun()
{
    expected_run = foldedEstimate();
}

size_t ReadContinuityTracker::currentRun() const
{
    return last_pos ? *last_pos - run_start : 0;
}

size_t ReadContinuityTracker::predictedEnd() const
{
    if (!last_pos)
        return 0;
    /// The estimate as if the live run checkpointed now, floored at history so live evidence
    /// cannot talk it down before the run outgrows it.
    return *last_pos + std::max<size_t>(
        static_cast<size_t>(foldedEstimate()), static_cast<size_t>(expected_run));
}

void ReadContinuityTracker::closeRun()
{
    checkpointRun();
    run_start = 0;
    last_pos.reset();
}

}
