#include <IO/ReadContinuityTracker.h>

#include <algorithm>

namespace DB
{

void ReadContinuityTracker::recordReadRange(size_t start_pos, size_t len)
{
    /// A range from the past is a re-declaration (overlapping feeds re-declare the
    /// same span), not a pattern signal: skip the covered part, feed only the new
    /// tail. Genuine backward jumps arrive as `recordSeek`.
    if (last_pos && start_pos < *last_pos)
    {
        if (start_pos + len <= *last_pos)
            return;
        len = start_pos + len - *last_pos;
        start_pos = *last_pos;
    }
    /// A far-forward jump (gap > bridgeable_gap) is a discontinuity: close the run first.
    if (last_pos && start_pos - *last_pos > options.bridgeable_gap)
        closeRun();
    /// Only a continuation of a NON-EMPTY run confirms anything: the first read
    /// after a seek also lands exactly at the frontier but carries no evidence.
    const bool exact_continuation = last_pos && start_pos == *last_pos && *last_pos != run_start;
    if (!last_pos)
        run_start = start_pos;
    last_pos = start_pos + len;
    /// An EXACT continuation is a positive continuity signal: checkpoint the grown run
    /// into the estimate (the same fold `closeRun` does, without ending the run).
    /// Trackers start empty on every reader, so without this a first-ever UNBROKEN
    /// scan would stay in the warming state forever - its run never closes, the
    /// estimate never rises, and the prediction stays at the damped floor. With it,
    /// each confirmed continuation warms the estimate toward the observed run.
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
    /// The prediction is "the estimate as if the live run checkpointed right now"
    /// (`foldedEstimate`), floored at the estimate (via the max) so live evidence
    /// never talks history down before the run outgrows it.
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
