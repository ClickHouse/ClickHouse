#pragma once

#include <cstddef>
#include <deque>
#include <memory>
#include <vector>

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

/// Sequence of RopeNodes covering a logical range.
/// Nodes are refcounted via shared_ptr — slicing is metadata-only,
/// the backing buffers stay alive as long as any Rope references them.
///
/// Two invariants maintained on every `append`:
///   1. `nodes` are sorted by `logical_offset` (stable on tie — equal-offset
///      nodes keep insertion order). So `popFront` always returns the
///      lowest-offset node, and `copyTo` can write contiguous output without
///      sorting.
///   2. `intervals` is a sorted, disjoint, merged coverage set —
///      `intervals[i].end() < intervals[i+1].offset` (strictly disjoint, no
///      touching). Coverage queries (`covers` / `gaps` / `coveredBytes` /
///      `range`) consult this set, so they are O(log intervals) for hits and
///      O(intervals∩req) overall — they do NOT scan `nodes`. Overlapping
///      appends collapse into the same interval; their data nodes are still
///      kept (in case different callers want different physical sources for
///      the overlap), but coverage stays correct regardless of duplication.
///
/// `popFront` rebuilds `intervals` from the remaining nodes so that
/// coverage queries stay consistent after consumption (`PipelineReadBuffer`
/// uses `range()` in its seek path to decide whether `new_pos` is still
/// reachable in the unconsumed nodes).
class Rope
{
public:
    ByteRange range() const;
    void append(RopeNode node);
    void append(Rope && other);

    /// Extract the parts of this rope that overlap `req`. Partial coverage is
    /// fine: nodes that don't overlap `req` are dropped, and overlapping
    /// nodes are trimmed to `req`'s bounds. Returns an empty Rope if nothing
    /// in this rope falls inside `req`.
    Rope slice(ByteRange req) const;

    /// Same as `slice(req)` but asserts the rope fully covers `req` (no gaps).
    /// Use when the caller knows the rope must contain `req` in full —
    /// catches a class of off-by-one bugs at the boundary.
    Rope extract(ByteRange req) const;

    /// True when every byte in `req` is held by some node of this rope.
    bool covers(ByteRange req) const;

    /// Sub-ranges of `req` not covered by any node. Empty iff `covers(req)`.
    std::vector<ByteRange> gaps(ByteRange req) const;

    /// Number of bytes in `req` covered by this rope.
    size_t coveredBytes(ByteRange req) const;

    /// Shift every node's `logical_offset` (and every interval's `offset`)
    /// by `delta`. Used when relocating a rope's logical coordinates (e.g.
    /// stripping the encryption header before exposing data to the caller).
    void shift(ssize_t delta);

    /// Flatten this rope's coverage of `req` into the contiguous buffer at
    /// `dst`. Asserts `covers(req)` — partial-coverage callers should use
    /// `slice(req)` and walk the nodes themselves. Returns bytes written
    /// (== `req.size` on success).
    size_t copyTo(char * dst, ByteRange req) const;

    /// Remove and return the lowest-offset node (the front of `nodes`).
    /// Does NOT update `intervals` — coverage queries after popFront return
    /// the appended (pre-pop) coverage. Intended for streaming consumers that
    /// don't query coverage after consumption.
    RopeNode popFront();

    /// Read-only view of the nodes deque. Sorted by `logical_offset`.
    /// No non-`const` overload: mutating the deque would silently break the
    /// sort invariant and the coverage tracking.
    const std::deque<RopeNode> & getNodes() const { return nodes; }
    bool empty() const { return nodes.empty(); }

    /// Sum of `node.size` over all nodes — counts overlap twice.
    /// `coveredBytes(range())` is the unique-byte equivalent.
    size_t totalBytes() const;

    /// Read-only view of the disjoint coverage intervals. Mostly for tests
    /// and diagnostics; production callers should use `covers` / `gaps` /
    /// `range`.
    const std::vector<ByteRange> & getIntervals() const { return intervals; }

private:
    /// Merge `iv` into `intervals`, coalescing with any overlapping or
    /// touching existing intervals.
    void mergeInterval(ByteRange iv);

    std::deque<RopeNode> nodes;
    std::vector<ByteRange> intervals;
};

}
