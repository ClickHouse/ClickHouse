#pragma once

#include <IO/ChainedBuffers.h>
#include <IO/LongConnectionLimit.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/Logger.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <memory>
#include <optional>

namespace DB
{

struct MachineBase;

/// One held source connection for a sequential run: a bounded GET opened ONCE
/// (only by the foreground - a machine never opens one, it can only RECEIVE one
/// at launch) and drained incrementally across windows while reads continue
/// forward within its bound. A small forward gap is bridged by discarding it on
/// the open stream (`skipForward`); a read it cannot continue reopens. Move-only
/// so it can ride the `FetchMachine` payload as a SINGLE owner (foreground member
/// <-> machine payload, never shared across threads). Offsets are OBJECT-LOCAL (a
/// GET streams one object); `read_until` is the predicted-reach bound (>= the read
/// extent), set ONCE.
struct LongConnection
{
    std::unique_ptr<ReadBufferFromFileBase> buffer;
    String object_path;
    size_t opened_at = 0;
    size_t current_position = 0;
    size_t read_until = 0;
    LongConnectionSlot slot;
    /// The stream returned EOF before `read_until`. Only reachable on unknown-size
    /// sources (a known size clamps the bound to the object end): the GET actually
    /// completed, so the connection is exhausted (not abandoned mid-response) and
    /// must not count as incomplete on drop.
    bool saw_eof = false;

    /// Read to its bound - fully consumed and pool-reusable.
    bool atBound() const { return current_position >= read_until; }
    /// Nothing more will come off this stream: read to the bound or ended at EOF.
    bool exhausted() const { return atBound() || saw_eof; }
    /// At least one byte crossed the wire. The GET is issued lazily, so a
    /// never-read connection returns to the pool untouched and must not count
    /// as incomplete.
    bool consumedAnyBytes() const { return current_position > opened_at; }
    bool servesObject(const String & path) const { return object_path == path; }
    /// Forward and `[off, off+want)` stays inside the bound. A contiguous read
    /// (`off == current_position`) always continues - it is not a bridge. A
    /// forward HOLE is bridged (over-read on the open GET) only if STRICTLY smaller
    /// than `bridgeable_gap`; a hole of exactly `bridgeable_gap` reopens instead - over-reading
    /// it costs about as much.
    bool canContinue(size_t off, size_t want, size_t bridgeable_gap) const
    {
        return canStartServing(off, bridgeable_gap) && off + want <= read_until;
    }

    /// Whether the channel can START serving at `off` - forward and inside the bound -
    /// even if the read would cross `read_until`. `readFromSource` serves the prefix up to
    /// the bound (the channel then drains clean) and reopens for the remainder, so a held
    /// channel that `canStartServing` must NOT be dropped as un-continuable.
    bool canStartServing(size_t off, size_t bridgeable_gap) const
    {
        return off >= current_position
            && (off == current_position || off - current_position < bridgeable_gap)
            && off < read_until;
    }

    /// Read the pre-allocated `blocks` off the open stream into a ChainedBuffers,
    /// advancing the frontier; `stop` (nullable) is polled between blocks.
    ChainedBuffers readInto(VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks,
        size_t file_pos, const MachineBase * stop);
    /// Discard up to `gap` bytes so the frontier advances over an already-cached
    /// hole (over-read; the request is saved). Returns bytes skipped (< `gap`
    /// only at EOF).
    size_t skipForward(size_t gap, size_t block_bytes);
    /// If only a tail <= `max_tail` remains to the bound, read it out so the
    /// connection completes. Returns bytes drained.
    struct DrainResult
    {
        size_t bytes = 0;      /// bytes actually drained
        bool failed = false;   /// a read error interrupted the drain
    };
    /// If only a tail <= `max_tail` remains to the bound, read it out so the connection
    /// completes (pool-reusable). The drained bytes are discarded (keep-alive only), so a
    /// read error must not fail the query: it is caught, logged, and reported via
    /// `DrainResult::failed`. Best-effort - never throws.
    DrainResult drainTail(size_t max_tail, size_t block_bytes, LoggerPtr log) noexcept;
};

/// Move a long connection out of `src`, leaving `src` EMPTY. A plain `std::optional`
/// move leaves the source ENGAGED (with a moved-from value), so every hand-off goes
/// through this to keep the connection a single owner (and to stop a moved-from
/// husk from being seen as a held connection or counted as an incomplete drop).
std::optional<LongConnection> takeLongConnection(std::optional<LongConnection> & src);

/// Zero-copy set()+next() path when the buffer supports it. Asynchronous
/// readers (`pread_threadpool`, io_uring) read into their own allocation
/// assuming `memory.size() == internal_buffer.size()`, so `set()` would
/// corrupt the heap when `chunk` exceeds the buffer's constructor-time size —
/// for those, fall back to `read()`.
///
/// Returned value is `0` only when the source signals EOF. Short positive
/// `next` returns are looped so a partial fill never reaches the caller as
/// `actual < pr.size`.
size_t readIntoBlock(ReadBuffer & buf, char * dest, size_t chunk);

}
