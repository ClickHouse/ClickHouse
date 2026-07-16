#pragma once

#include <base/types.h>
#include <base/unit.h>
#include <cstddef>
#include <optional>

namespace DB
{

/// Predicts how far a read continues contiguously, from the served ranges and seeks it is fed. It
/// keeps an EWMA of past run lengths; a far seek folds the finished run into the estimate and resets
/// the run, so the read right after a far seek is still predicted long and repeated random seeks
/// decay the estimate toward zero.
class ReadContinuityTracker
{
public:
    struct Options
    {
        /// Forward gap still counted as continuing the run (the caller sets it from `min_bytes_for_seek`).
        size_t bridgeable_gap = 2 * MiB;
        /// EWMA weight of the latest run (0..1): higher trusts it more, lower decays slower.
        double ewma_alpha = 0.7;
    };

    ReadContinuityTracker() = default;
    explicit ReadContinuityTracker(Options options_)
        : options(options_)
    {
    }

    /// Record a `len`-byte serve from `start_pos`. A far-forward range closes the run first; a range
    /// from the past feeds only its tail past the frontier.
    void recordReadRange(size_t start_pos, size_t len);

    /// Record a seek: a forward gap within `bridgeable_gap` keeps the run, else closes it.
    void recordSeek(size_t new_pos);

    /// Predicted absolute end of the current run, anchored at the run start (not the caller's offset,
    /// which would inflate it as the cursor advances). 0 before the first serve.
    size_t predictedEnd() const;

    size_t currentRun() const;
    size_t estimate() const { return static_cast<size_t>(expected_run); }

private:
    /// EWMA fold of the live run into the carried estimate: `alpha * run + (1 - alpha) * estimate`.
    double foldedEstimate() const;
    /// Fold the run into the estimate without ending it (a continuation checkpoint).
    void checkpointRun();
    /// Fold the run into the estimate and clear it.
    void closeRun();

    Options options;
    size_t run_start = 0;
    /// Frontier (end of the last serve); `nullopt` before the first serve or after a reset.
    std::optional<size_t> last_pos;
    /// EWMA of completed run spans; survives a far seek.
    double expected_run = 0.0;
};

}
