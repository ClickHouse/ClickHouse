#pragma once

#include <base/types.h>
#include <cstddef>
#include <optional>

namespace DB
{

/// Estimates how far a read will continue contiguously - a predicted forward length in
/// bytes - from the sequence of served byte ranges and seeks arriving at a reader.
///
/// The run span is the bytes covered forward without a far seek (a forward gap up to
/// `near_gap` is bridged and stays in the run). A far seek folds the finished run into an
/// EWMA of past run lengths and resets the run while keeping the estimate, so the read right
/// after a far seek is still predicted long (trusting the previous run); repeated random
/// seeks decay the EWMA toward zero.
class ContinuityTracker
{
public:
    struct Options
    {
        /// Forward gap up to which a serve still continues the run; the caller sets it
        /// from `min_bytes_for_seek`.
        size_t near_gap = 2 * 1024 * 1024;
        /// EWMA weight for the just-finished run (0..1): higher trusts the most
        /// recent run more, lower is smoother / decays slower.
        double ewma_alpha = 0.5;
    };

    /// All-defaults overload kept separate from the `Options` one: a default
    /// argument `Options{}` in a member declaration would need the initializers
    /// in a complete-class context (same reason `ReaderExecutor` has two ctors).
    ContinuityTracker() = default;
    explicit ContinuityTracker(Options options_)
        : options(options_)
    {
    }

    /// A contiguous serve of `len` bytes starting at `start_pos`. Call once per
    /// window served forward. Extends the current run when `start_pos` continues
    /// the frontier (exactly, or across a forward gap <= `near_gap`); any other
    /// jump closes the run first (treated like an unsignalled seek).
    void onServe(size_t start_pos, size_t len);

    /// A seek to `new_pos`. A forward near gap (<= `near_gap`) keeps the run (the
    /// next serve continues it); any other jump - backward, or forward beyond
    /// `near_gap` - ends the run: fold its span into the estimate and restart.
    void onSeek(size_t new_pos);

    /// The predicted contiguous length (bytes) the read will cover going forward:
    /// `max(currentRun, estimate)` - "we have read this far contiguously (or did last
    /// time), so expect about as far again".
    size_t predictedReach() const;

    /// The current contiguous run span (frontier - run start).
    size_t currentRun() const;

    /// The carried EWMA estimate of past run lengths.
    size_t estimate() const { return static_cast<size_t>(expected_run); }

private:
    /// Fold the current run span into the EWMA estimate and clear the run.
    void closeRun();

    Options options;
    /// Start of the current contiguous run. Meaningful only while `last_pos` is set.
    size_t run_start = 0;
    /// Frontier (end of the last serve); `nullopt` before the first serve or
    /// right after a reset.
    std::optional<size_t> last_pos;
    /// EWMA of completed run spans - the carry-over that survives a far seek.
    double expected_run = 0.0;
};

}
