#include <IO/ReadContinuityTracker.h>

#include <algorithm>

namespace DB
{

void ReadContinuityTracker::recordReadRange(size_t start_pos, size_t len)
{
    /// A range from the past re-declares an already-covered span; feed only its tail past the frontier.
    if (last_pos && start_pos < *last_pos)
    {
        if (start_pos + len <= *last_pos)
            return;
        len = start_pos + len - *last_pos;
        start_pos = *last_pos;
    }
    if (last_pos && start_pos - *last_pos > options.bridgeable_gap)
        closeRun();   /// far-forward jump: a discontinuity
    /// Only continuing a non-empty run is evidence (the first serve after a seek also lands at the frontier).
    const bool exact_continuation = last_pos && start_pos == *last_pos && *last_pos != run_start;
    if (!last_pos)
        run_start = start_pos;
    last_pos = start_pos + len;
    /// Checkpoint the growing run so an unbroken first scan warms the estimate before the run closes.
    if (exact_continuation)
        checkpointRun();
}

void ReadContinuityTracker::recordSeek(size_t new_pos)
{
    /// A gapless seek to the frontier is a continuation: checkpoint, keep the run.
    if (last_pos && new_pos == *last_pos)
    {
        if (*last_pos != run_start)
            checkpointRun();
        return;
    }
    if (last_pos && new_pos >= *last_pos && new_pos - *last_pos <= options.bridgeable_gap)
        return;   /// forward gap within the bridge: keep the run
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
    /// The estimate as if the live run checkpointed now, floored at the carried estimate.
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
