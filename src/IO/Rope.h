#pragma once

#include <cstddef>
#include <deque>
#include <memory>
#include <vector>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/DequeWithMemoryTracking.h>

class MemoryTracker;

namespace DB
{

struct ByteRange
{
    size_t offset = 0;
    size_t size = 0;
    size_t end() const { return offset + size; }
};

/// Abstract backing memory for a rope node.
class RopeBuffer
{
public:
    virtual ~RopeBuffer() = default;
    virtual char * data() = 0;
    virtual const char * data() const = 0;
    virtual size_t size() const = 0;
    virtual void transferTo(MemoryTracker * new_tracker) = 0;
};

/// Owns a block of memory.
class OwnedRopeBuffer : public RopeBuffer
{
public:
    explicit OwnedRopeBuffer(size_t size);
    ~OwnedRopeBuffer() override;

    OwnedRopeBuffer(const OwnedRopeBuffer &) = delete;
    OwnedRopeBuffer & operator=(const OwnedRopeBuffer &) = delete;

    char * data() override { return buf_data; }
    const char * data() const override { return buf_data; }
    size_t size() const override { return buf_size; }
    void transferTo(MemoryTracker * new_tracker) override;

private:
    char * buf_data;
    size_t buf_size;
};


/// Single node in a rope. References a slice of a RopeBuffer.
struct RopeNode
{
    std::shared_ptr<RopeBuffer> buffer;
    size_t buffer_offset = 0;
    size_t size = 0;
    size_t logical_offset = 0;

    char * data() { return buffer->data() + buffer_offset; } // NOLINT(readability-make-member-function-const)
    const char * data() const { return buffer->data() + buffer_offset; }
    ByteRange range() const { return {logical_offset, size}; }
};

/// Sequence of RopeNodes covering a logical range, with a built-in
/// consumption cursor.
///
/// Two invariants maintained on every `append`:
///   1. `nodes` are sorted by `logical_offset` (stable on tie — equal-offset
///      nodes keep insertion order). So consumption proceeds in
///      monotonically-increasing logical order and `copyTo` can write
///      contiguous output without sorting.
///   2. `intervals` is a sorted, disjoint, merged coverage set —
///      `intervals[i].end() < intervals[i+1].offset` (strictly disjoint, no
///      touching). Coverage queries (`covers` / `gaps` / `coveredBytes` /
///      `range`) consult this set, so they are O(log intervals) for hits.
///
/// `advance` / `tryRewind` keep both invariants in sync:
///   * `advance(bytes)` moves the cursor forward; nodes whose data falls
///     fully behind the cursor are released (their `shared_ptr` is
///     dropped), and `intervals.front()` is shrunk from the front to
///     match.
///   * `tryRewind(pos)` moves the cursor (forward or backward) inside the
///     currently-held nodes; backward moves extend `intervals.front()`
///     so coverage queries still report the rewound-into bytes.
///
/// The cursor's effective position is `nodes.front().logical_offset +
/// front_offset`. `peek()` returns the unconsumed prefix of the front
/// node (memory stays valid until the next `advance` / `tryRewind` call).
class Rope
{
public:
    void append(RopeNode node);
    void append(Rope && other);

    // ─── Streaming consumption ──────────────────────────────────────────

    struct Span
    {
        char *  data = nullptr;
        size_t  size = 0;
        size_t  logical_offset = 0;
    };

    /// True iff there is no more data to read at-or-after the cursor.
    bool atEnd() const { return nodes.empty(); }

    /// The span starting at the cursor (= unconsumed prefix of the front
    /// node). Empty when `atEnd()`. Memory stays alive until the next
    /// `advance` or `tryRewind`.
    Span peek() const;

    /// Move the cursor forward by `bytes`. Releases nodes that fall
    /// entirely behind the new cursor position. `bytes` should not exceed
    /// the remaining reachable bytes — extra bytes are silently clamped.
    void advance(size_t bytes);

    /// Move the cursor to `new_position`. Succeeds iff `new_position` is
    /// inside the currently-held nodes, i.e. in
    /// `[nodes.front().logical_offset, nodes.back().end())`. Backward
    /// moves restore intervals so coverage queries report the rewound
    /// bytes. Returns true on success; false leaves the rope unchanged.
    bool tryRewind(size_t new_position);

    // ─── Coverage queries (reflect still-reachable bytes) ───────────────

    /// `[lowest_reachable_offset, highest_reachable_end)`. Empty iff
    /// `atEnd()`.
    ByteRange range() const;

    /// True when every byte in `req` is reachable from the cursor.
    bool covers(ByteRange req) const;

    /// Sub-ranges of `req` not reachable. Empty iff `covers(req)`.
    VectorWithMemoryTracking<ByteRange> gaps(ByteRange req) const;

    /// Number of bytes in `req` reachable from the cursor.
    size_t coveredBytes(ByteRange req) const;

    /// Sum of node sizes still held (counts overlapping bytes twice).
    /// `coveredBytes(range())` is the unique-byte equivalent.
    size_t totalBytes() const;

    /// Alias for `atEnd()`; kept for readability at call sites that mean
    /// "is there anything in this rope at all".
    bool empty() const { return nodes.empty(); }

    // ─── Slicing / flattening ───────────────────────────────────────────

    /// Extract the parts of this rope that overlap `req`. Partial coverage
    /// is fine; non-overlapping nodes are dropped. The returned Rope's
    /// cursor starts at the front of its first node. Operates on the
    /// rope's still-reachable bytes (post-advance).
    Rope slice(ByteRange req) const;

    /// Same as `slice(req)` but asserts the rope fully covers `req`.
    Rope extract(ByteRange req) const;

    /// Flatten this rope's coverage of `req` into the contiguous buffer at
    /// `dst`. Asserts `covers(req)`. Returns bytes written.
    size_t copyTo(char * dst, ByteRange req) const;

    // ─── Diagnostics / shifting ─────────────────────────────────────────

    /// Shift every node's `logical_offset` (and every interval's
    /// `offset`) by `delta`. Used when relocating a rope's logical
    /// coordinates (e.g. stripping the encryption header).
    void shift(ssize_t delta);

    /// Read-only view of the still-held nodes (sorted, post-advance).
    /// The first node's `data()` is the buffer start; the cursor is at
    /// `data() + front_offset_for_test()`. No non-`const` overload —
    /// mutating the deque would silently break the sort invariant.
    const DequeWithMemoryTracking<RopeNode> & getNodes() const { return nodes; }

    /// Read-only view of the disjoint coverage intervals. Mostly for
    /// tests; production callers should use `covers` / `gaps` / `range`.
    const VectorWithMemoryTracking<ByteRange> & getIntervals() const { return intervals; }

    /// Test-only: bytes already consumed inside `nodes.front()`.
    size_t frontOffsetForTest() const { return front_offset; }

private:
    /// Merge `iv` into `intervals`, coalescing with any overlapping or
    /// touching existing intervals.
    void mergeInterval(ByteRange iv);

    /// Drop `bytes` from the front of `intervals` (used by `advance` /
    /// `tryRewind`). The dropped bytes must be at the front of
    /// `intervals.front()`.
    void shrinkIntervalsFront(size_t bytes);

    /// Extend `intervals.front()` backward by `bytes` (used by
    /// `tryRewind` going backward). Asserts the front interval's offset
    /// is at least `bytes` (so we don't underflow).
    void extendIntervalsFront(size_t bytes);

    DequeWithMemoryTracking<RopeNode> nodes;
    VectorWithMemoryTracking<ByteRange> intervals;

    /// Bytes inside `nodes.front()` that have already been consumed by
    /// `advance` but whose buffer is still alive (the front node hasn't
    /// been released yet). `peek` returns from
    /// `nodes.front().data() + front_offset`. Always `0` after the
    /// front node is released or when nodes are empty.
    size_t front_offset = 0;
};

}
