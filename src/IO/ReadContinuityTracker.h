#pragma once

#include <base/types.h>
#include <base/unit.h>
#include <cstddef>
#include <optional>

namespace DB
{

/// Estimates how far a read will continue contiguously - a predicted forward length in
/// bytes - from the sequence of served byte ranges and seeks arriving at a reader.
///
/// The run span is the bytes covered forward without a far seek (a forward gap up to
/// `bridgeable_gap` is bridged and stays in the run). A far seek folds the finished run into an
/// EWMA of past run lengths and resets the run while keeping the estimate, so the read right
/// after a far seek is still predicted long (trusting the previous run); repeated random
/// seeks decay the EWMA toward zero.
class ReadContinuityTracker
{
public:
    struct Options
    {
        /// Forward gap up to which a serve still continues the run; the caller sets it
        /// from `min_bytes_for_seek`.
        size_t bridgeable_gap = 2 * MiB;
        /// EWMA weight for the just-finished run (0..1): higher trusts the most
        /// recent run more, lower is smoother / decays slower. Also the damping
        /// of the live run inside `predictedEnd`.
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

    /// Record a `len`-byte window served forward from `start_pos`: extends the run when
    /// it continues the frontier (within `bridgeable_gap`); a far-forward range closes
    /// the run first. A range from the past is a re-declaration, not a pattern signal:
    /// the covered part is skipped and only the tail past the frontier feeds the run.
    void recordReadRange(size_t start_pos, size_t len);

    /// Record a seek to `new_pos`: a forward gap within `bridgeable_gap` keeps the run; any
    /// other jump closes it, folding its span into the estimate.
    void recordSeek(size_t new_pos);

    /// The predicted ABSOLUTE end of the current run, anchored at the run (the
    /// last seek position), not at the caller's offset:
    /// `frontier + max(ewma_alpha * currentRun + (1 - ewma_alpha) * estimate, estimate)`.
    /// The first read of a
    /// run predicts only the historical estimate; as the run accumulates
    /// evidence the end grows as `run_start + (1 + alpha) * run` - proportional,
    /// and identical for every caller wherever they ask from (anchoring the full
    /// length at the caller's offset would re-anchor it forward as the cursor
    /// advances, inflating the prediction at twice the consumption rate).
    /// 0 before the first serve / right after a reset.
    size_t predictedEnd() const;

    /// The current contiguous run span (frontier - run start).
    size_t currentRun() const;

    /// The carried EWMA estimate of past run lengths.
    size_t estimate() const { return static_cast<size_t>(expected_run); }

private:
    /// The estimator's single defining formula: the EWMA fold of the live run into
    /// the carried estimate, `alpha * currentRun + (1 - alpha) * expected_run`.
    double foldedEstimate() const;

    /// Fold the current run span into the EWMA estimate WITHOUT ending the run -
    /// the positive-signal checkpoint for exact continuations and gapless seeks.
    void checkpointRun();

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
