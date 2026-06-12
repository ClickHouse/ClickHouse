#pragma once

#include <cstddef>
#include <memory>
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

/// Sequence of RopeNodes covering a logical range, with a built-in consumption
/// cursor. Invariants maintained on every `append`: `nodes` are sorted by
/// `logical_offset` (stable on tie) and `intervals` is a sorted, disjoint,
/// merged coverage set consulted by all coverage queries. The cursor is
/// `nodes.front().logical_offset + front_offset`; `advance` releases nodes
/// that fall fully behind it, `tryRewind` moves it within the held nodes.
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

    bool atEnd() const { return nodes.empty(); }

    /// The unconsumed prefix of the front node; empty when `atEnd`. Memory
    /// stays alive until the next `advance` or `tryRewind`.
    Span peek() const;

    /// Move the cursor forward, releasing nodes that fall fully behind it.
    /// Bytes past the reachable end are silently clamped.
    void advance(size_t bytes);

    /// Move the cursor anywhere inside the currently-held nodes; backward
    /// moves restore interval coverage. False (rope unchanged) outside them.
    bool tryRewind(size_t new_position);

    // ─── Coverage queries (reflect still-reachable bytes) ───────────────

    /// `[lowest_reachable_offset, highest_reachable_end)`; empty iff `atEnd`.
    ByteRange range() const;

    bool covers(ByteRange req) const;

    /// Sub-ranges of `req` not reachable; empty iff `covers(req)`.
    VectorWithMemoryTracking<ByteRange> gaps(ByteRange req) const;

    size_t coveredBytes(ByteRange req) const;

    /// Sum of node sizes still held (counts overlapping bytes twice).
    size_t totalBytes() const;

    bool empty() const { return nodes.empty(); }

    // ─── Slicing / flattening ───────────────────────────────────────────

    /// The parts of this rope overlapping `req` (partial coverage is fine),
    /// as a fresh rope with its cursor at the front.
    Rope slice(ByteRange req) const;

    /// `slice(req)`, asserting the rope fully covers `req`.
    Rope extract(ByteRange req) const;

    /// Flatten the coverage of `req` into `dst` (asserts `covers(req)`).
    /// Returns bytes written.
    size_t copyTo(char * dst, ByteRange req) const;

    // ─── Diagnostics / shifting ─────────────────────────────────────────

    /// Shift all logical coordinates by `delta` (e.g. stripping the
    /// encryption header).
    void shift(ssize_t delta);

    /// No non-`const` overload - mutating the deque would silently break the
    /// sort invariant.
    const DequeWithMemoryTracking<RopeNode> & getNodes() const { return nodes; }

    const VectorWithMemoryTracking<ByteRange> & getIntervals() const { return intervals; }

    size_t frontOffsetForTest() const { return front_offset; }

private:
    /// Merge `iv` into `intervals`, coalescing overlapping/touching ones.
    void mergeInterval(ByteRange iv);

    /// Drop `bytes` from the front of `intervals.front()`.
    void shrinkIntervalsFront(size_t bytes);

    /// Extend `intervals.front()` backward (rewind support).
    void extendIntervalsFront(size_t bytes);

    DequeWithMemoryTracking<RopeNode> nodes;
    VectorWithMemoryTracking<ByteRange> intervals;

    /// Bytes already consumed inside `nodes.front()` while its buffer is
    /// still alive; `0` whenever `nodes` is empty.
    size_t front_offset = 0;
};

}
