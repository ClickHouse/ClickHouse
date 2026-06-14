#include <IO/ContinuityTracker.h>

#include <algorithm>

namespace DB
{

void ContinuityTracker::onServe(size_t start_pos, size_t len)
{
    /// A serve that continues the frontier - exactly, or across a bridgeable
    /// forward gap (<= near_gap) - extends the current run. A backward or far
    /// forward jump is an unsignalled discontinuity: close the run first.
    if (last_pos && (start_pos < *last_pos || start_pos - *last_pos > options.near_gap))
        closeRun();
    if (!last_pos)
        run_start = start_pos;
    last_pos = start_pos + len;
}

void ContinuityTracker::onSeek(size_t new_pos)
{
    /// A forward near gap keeps the run (the next serve continues it); any other
    /// jump ends it - fold its span into the estimate and restart at `new_pos`.
    if (last_pos && new_pos >= *last_pos && new_pos - *last_pos <= options.near_gap)
        return;
    closeRun();
    run_start = new_pos;
    last_pos = new_pos;
}

size_t ContinuityTracker::currentRun() const
{
    return last_pos ? *last_pos - run_start : 0;
}

size_t ContinuityTracker::signal() const
{
    return std::max<size_t>(currentRun(), static_cast<size_t>(expected_run));
}

std::optional<size_t> ContinuityTracker::suggestedBound(size_t pos, size_t object_end, size_t extent_end) const
{
    const size_t s = signal();
    if (s < options.trigger)
        return std::nullopt;
    const size_t clamp = std::min(object_end, extent_end);
    /// A CONFIRMED long run (this run, not the carried estimate) opens to the
    /// clamp - full-file mode; otherwise bound to the estimated reach.
    if (currentRun() >= options.eof_confidence)
        return clamp;
    return std::min(pos + s, clamp);
}

void ContinuityTracker::closeRun()
{
    /// Fold the finished run span into the EWMA. A zero-span close (consecutive
    /// seeks with no read between) decays the estimate - the self-correction for
    /// random access.
    expected_run = options.ewma_alpha * static_cast<double>(currentRun())
        + (1.0 - options.ewma_alpha) * expected_run;
    run_start = 0;
    last_pos.reset();
}

}
