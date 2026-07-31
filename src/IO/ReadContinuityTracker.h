#pragma once

#include <base/types.h>
#include <base/unit.h>
#include <cstddef>
#include <optional>

namespace DB
{

/// Estimates how far a read continues contiguously, from the served byte ranges and seeks. A run
/// is the bytes covered forward without a far seek (a forward gap up to `bridgeable_gap` stays in
/// the run); a far seek folds the run into an EWMA of past run lengths and resets, so the read
/// after a far seek is still predicted long, while repeated random seeks decay the EWMA toward zero.
class ReadContinuityTracker
{
public:
    struct Options
    {
        /// Forward gap up to which a serve still continues the run; the caller sets it
        /// from `min_bytes_for_seek`.
        size_t bridgeable_gap = 2 * MiB;
        /// EWMA weight for the just-finished run (0..1): higher trusts the recent run more.
        double ewma_alpha = 0.7;
    };

    /// All-defaults overload kept separate from the `Options` one: a default
    /// argument `Options{}` in a member declaration would need the initializers
    /// in a complete-class context (same reason `ReaderExecutor` has two ctors).
    ReadContinuityTracker() = default;
    explicit ReadContinuityTracker(Options options_)
        : options(options_)
    {
    }

    /// Record a `len`-byte window served forward from `start_pos`: extends the run within
    /// `bridgeable_gap`, else closes it first. A past range re-declares an already-fed span (tail only).
    void recordReadRange(size_t start_pos, size_t len);

    /// Record a seek to `new_pos`: a forward gap within `bridgeable_gap` keeps the run; any
    /// other jump closes it, folding its span into the estimate.
    void recordSeek(size_t new_pos);

    /// The predicted ABSOLUTE end of the current run: `frontier + max(foldedEstimate, estimate)`.
    /// Anchored at the run start, not the caller's offset -- anchoring at the offset would re-anchor
    /// forward as the cursor advances, inflating the prediction. 0 before the first serve / after a reset.
    size_t predictedEnd() const;

    /// The current contiguous run span (frontier - run start).
    size_t currentRun() const;

    /// The carried EWMA estimate of past run lengths.
    size_t estimate() const { return static_cast<size_t>(expected_run); }

private:
    /// The EWMA fold of the live run into the estimate: `alpha * currentRun + (1 - alpha) * expected_run`.
    double foldedEstimate() const;

    /// Fold the run into the estimate without ending it (the positive-signal checkpoint).
    void checkpointRun();

    /// Fold the run into the estimate and clear it.
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
