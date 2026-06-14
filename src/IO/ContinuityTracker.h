#pragma once

#include <base/types.h>
#include <cstddef>
#include <optional>

namespace DB
{

/// Detects a continuous (sequential-ish) read pattern from the sequence of
/// served byte ranges and seeks arriving at a reader, and estimates how far the
/// read will continue. It is the signal for opening a "long connection" - a
/// source connection opened with a range beyond the current look-ahead window
/// and reused across windows - versus a one-shot per-window read.
///
/// The signal is the contiguous run span: bytes covered forward without a far
/// seek (a forward gap up to `near_gap` is bridged and stays part of the run). A
/// far seek folds the finished run into an EWMA of past run lengths and resets
/// the current run but KEEPS the estimate, so the read right after a far seek
/// can still be predicted long (trusting the previous run). Repeated random
/// seeks decay the EWMA, so the tracker self-corrects back to "short" without an
/// explicit mode flag.
///
/// Pure value type: no I/O, no ownership, position-space agnostic (it only sees
/// deltas, so the caller's offsets just need to be in one consistent space). One
/// per reader, foreground-only.
class ContinuityTracker
{
public:
    struct Options
    {
        /// Forward gap up to which a read still continues the run (a bridgeable
        /// hole, not a real seek). Mirrors the executor's `min_bytes_for_seek`.
        size_t near_gap = 2 * 1024 * 1024;
        /// A run span / estimate at or above this triggers a long connection.
        /// Defaults to the executor's window size.
        size_t trigger = 8 * 1024 * 1024;
        /// A CONFIRMED run span at or above this opens the connection to its
        /// clamp (object end / read extent) - full-file mode - rather than to
        /// `pos + signal`. Only the current run counts here, never the carried
        /// estimate, so EOF mode needs a long run actually observed this time.
        size_t eof_confidence = 32 * 1024 * 1024;
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

    /// A seek to `new_pos`. A forward near gap (<= `near_gap`) keeps the run
    /// (the next serve continues it); any other jump - backward, or forward
    /// beyond `near_gap` - ends the run: fold its span into the estimate and
    /// reset at `new_pos`.
    void onSeek(size_t new_pos);

    /// The suggested right bound for a connection opened to serve a read at
    /// `pos`, or `nullopt` for "use a one-shot". `object_end` / `extent_end`
    /// clamp the bound (pass `SIZE_MAX` for "no clamp"). A bound is returned only
    /// once the signal reaches `trigger`; it opens to the clamp once the current
    /// run reaches `eof_confidence`, otherwise to `pos + signal`.
    std::optional<size_t> suggestedBound(size_t pos, size_t object_end, size_t extent_end) const;

    /// max(current run span, carried EWMA estimate) - the continuity signal.
    size_t signal() const;

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
