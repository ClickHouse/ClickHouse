#include <IO/Rope.h>
#include <Common/Allocator.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Defines.h>
#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <cstring>

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorRopeBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    Allocator<false, false> rope_allocator;

    /// SIMD-padded allocation size, with the same overflow check `Memory::withPadding` uses.
    size_t paddedSize(size_t size)
    {
        size_t res = 0;
        if (common::addOverflow<size_t>(size, PADDING_FOR_SIMD, res))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "OwnedRopeBuffer size {} is too big to pad", size);
        return res;
    }
}

OwnedRopeBuffer::OwnedRopeBuffer(size_t size)
    : buf_data(static_cast<char *>(rope_allocator.alloc(paddedSize(size))))
    , buf_size(size)
{
    CurrentMetrics::add(CurrentMetrics::ReaderExecutorRopeBytes, buf_size);
}

OwnedRopeBuffer::~OwnedRopeBuffer()
{
    CurrentMetrics::sub(CurrentMetrics::ReaderExecutorRopeBytes, buf_size);
    /// `paddedSize` succeeded in the ctor, so `buf_size + PADDING_FOR_SIMD` cannot overflow.
    rope_allocator.free(buf_data, buf_size + PADDING_FOR_SIMD);
}

void OwnedRopeBuffer::transferTo(MemoryTracker * /* new_tracker */)
{
    /// Will be implemented when PageCacheProvider needs it.
}

ByteRange Rope::range() const
{
    if (intervals.empty())
        return {0, 0};
    size_t start = intervals.front().offset;
    size_t end = intervals.back().end();
    return {start, end - start};
}

Rope::Span Rope::peek() const
{
    if (nodes.empty())
        return {};
    const RopeNode & n = nodes.front();
    return Span{
        const_cast<char *>(n.data()) + front_offset,
        n.size - front_offset,
        n.logical_offset + front_offset,
    };
}

void Rope::shrinkIntervalsFront(size_t bytes)
{
    if (bytes == 0 || intervals.empty())
        return;
    ByteRange & first = intervals.front();
    if (bytes >= first.size)
    {
        intervals.erase(intervals.begin());
    }
    else
    {
        first.offset += bytes;
        first.size -= bytes;
    }
}

void Rope::extendIntervalsFront(size_t bytes)
{
    if (bytes == 0)
        return;
    chassert(!intervals.empty());
    chassert(intervals.front().offset >= bytes);
    intervals.front().offset -= bytes;
    intervals.front().size += bytes;
}

void Rope::advance(size_t bytes)
{
    if (nodes.empty())
        return;

    /// Move the cursor by `bytes` (clamped to EOF) and drop everything now behind it.
    /// Working from an absolute logical position -- rather than popping the front node by
    /// its physical size -- is what makes consumption correct for overlapping nodes: a
    /// node that sits entirely behind the new position is released even if it overlaps a
    /// node still ahead, so its bytes are never re-served.
    const size_t cur = nodes.front().logical_offset + front_offset;
    /// Clamp the delta to EOF, not `cur + bytes` (which could overflow past EOF and wrap
    /// below `cur`). `cur <= range().end()` by the cursor invariant.
    const size_t new_position = cur + std::min(bytes, range().end() - cur);

    /// Drop coverage before the new position. Cascades across intervals (a forward
    /// `tryRewind` can jump over a gap straight to a later node).
    while (!intervals.empty() && intervals.front().end() <= new_position)
        intervals.erase(intervals.begin());
    if (!intervals.empty() && intervals.front().offset < new_position)
    {
        intervals.front().size -= new_position - intervals.front().offset;
        intervals.front().offset = new_position;
    }

    while (!nodes.empty() && nodes.front().logical_offset + nodes.front().size <= new_position)
        nodes.pop_front();

    /// Offset into the new front node: with overlap it may start before `new_position`;
    /// for a contiguous cover it starts exactly there (front_offset 0).
    front_offset = (!nodes.empty() && new_position > nodes.front().logical_offset)
        ? new_position - nodes.front().logical_offset
        : 0;
}

bool Rope::tryRewind(size_t new_position)
{
    if (nodes.empty())
        return false;

    /// Reachable from the cursor = entire held nodes (their buffers are
    /// alive). The lowest reachable byte is the ORIGINAL start of the
    /// front node (`logical_offset`, not `+ front_offset`), because a
    /// backward rewind into the buffer is supported. The highest reachable byte is the
    /// merged-coverage end, not `nodes.back()` — nodes are sorted by start, so under
    /// overlap the last node by start can end before an earlier, longer one.
    const size_t reachable_lo = nodes.front().logical_offset;
    const size_t reachable_hi = range().end();
    if (new_position < reachable_lo || new_position > reachable_hi)
        return false;

    const RopeNode & front = nodes.front();
    const size_t front_end = front.logical_offset + front.size;

    if (new_position < front_end)
    {
        /// Inside the front node — just adjust `front_offset`. Going
        /// backward extends `intervals.front()` so coverage queries
        /// report the rewound-into bytes; going forward shrinks it.
        const size_t new_front_offset = new_position - front.logical_offset;
        if (new_front_offset > front_offset)
            shrinkIntervalsFront(new_front_offset - front_offset);
        else if (new_front_offset < front_offset)
            extendIntervalsFront(front_offset - new_front_offset);
        front_offset = new_front_offset;
        return true;
    }

    /// new_position is past the front node. Walk later nodes to reject positions that land
    /// in a gap (no held node covers them — `tryRewind` must land exactly on new_position),
    /// then let `advance` move the cursor there by logical distance.
    const size_t cur = front.logical_offset + front_offset;
    for (size_t i = 1; i < nodes.size(); ++i)
    {
        const RopeNode & node = nodes[i];
        if (new_position < node.logical_offset)
            return false;
        if (new_position < node.logical_offset + node.size)
        {
            advance(new_position - cur);
            return true;
        }
    }

    /// new_position == reachable_hi (exclusive end of the last node): EOF.
    advance(new_position - cur);
    return true;
}

void Rope::mergeInterval(ByteRange iv)
{
    if (iv.size == 0)
        return;

    /// Find the first existing interval that touches or overlaps `iv` — i.e.
    /// the first one whose `end() >= iv.offset`. Anything strictly before
    /// (end() < iv.offset) is left alone.
    auto it = std::lower_bound(intervals.begin(), intervals.end(), iv.offset,
        [](const ByteRange & ex, size_t v) { return ex.end() < v; });

    size_t merged_start = iv.offset;
    size_t merged_end = iv.end();
    auto erase_begin = it;
    while (it != intervals.end() && it->offset <= merged_end)
    {
        merged_start = std::min(merged_start, it->offset);
        merged_end = std::max(merged_end, it->end());
        ++it;
    }
    auto erase_end = it;
    auto pos = intervals.erase(erase_begin, erase_end);
    intervals.insert(pos, ByteRange{merged_start, merged_end - merged_start});
}

void Rope::append(RopeNode node)
{
    /// Drop zero-size nodes: they carry no bytes and are not merged into
    /// `intervals` (which skips empty ranges), so keeping them would leave `nodes`
    /// non-empty while `peek` returns an empty span -- a drain loop advancing by the
    /// span size would never progress.
    if (node.size == 0)
        return;

    /// Never insert behind the cursor of a partly-consumed rope. `front_offset` applies to
    /// `nodes.front()`, so a node inserted before it would steal that offset (an out-of-bounds
    /// `peek`), and merging its range would re-cover already-consumed bytes. Drop a node
    /// entirely behind the cursor; trim one straddling it to its still-reachable tail.
    /// (A fresh rope -- `front_offset == 0` -- accepts out-of-order appends unchanged.)
    if (front_offset != 0)
    {
        const size_t cursor = nodes.front().logical_offset + front_offset;
        if (node.logical_offset + node.size <= cursor)
            return;
        if (node.logical_offset < cursor)
        {
            const size_t trim = cursor - node.logical_offset;
            node.buffer_offset += trim;
            node.size -= trim;
            node.logical_offset = cursor;
        }
    }

    /// Insert into `nodes` keeping the sort by logical_offset (stable on tie:
    /// equal-offset nodes keep insertion order).
    ByteRange node_range = node.range();
    auto it = std::upper_bound(nodes.begin(), nodes.end(), node.logical_offset,
        [](size_t v, const RopeNode & n) { return v < n.logical_offset; });
    nodes.insert(it, std::move(node));
    mergeInterval(node_range);
}

void Rope::append(Rope && other)
{
    if (other.nodes.empty())
        return;

    /// A partially-consumed `other` keeps its consumed prefix in the front node
    /// (at the original `logical_offset`), while its intervals already start past
    /// it - splicing the raw node would let `peek` resurrect those bytes that
    /// `range`/`covers` report as gone. Normalize to the live range first; `slice`
    /// trims the consumed prefix and yields `front_offset == 0`.
    if (other.front_offset != 0)
    {
        append(other.slice(other.range()));
        other.nodes.clear();
        other.intervals.clear();
        other.front_offset = 0;
        return;
    }

    /// Splice nodes then in-place merge — both halves are individually sorted
    /// by `logical_offset` by invariant.
    size_t split_idx = nodes.size();
    nodes.insert(
        nodes.end(),
        std::make_move_iterator(other.nodes.begin()),
        std::make_move_iterator(other.nodes.end()));
    std::inplace_merge(
        nodes.begin(),
        nodes.begin() + static_cast<std::ptrdiff_t>(split_idx),
        nodes.end(),
        [](const RopeNode & a, const RopeNode & b) { return a.logical_offset < b.logical_offset; });

    for (const auto & iv : other.intervals)
        mergeInterval(iv);

    other.nodes.clear();
    other.intervals.clear();
}

Rope Rope::slice(ByteRange req) const
{
    Rope result;
    /// Bytes before the cursor are consumed and not slice-able. With overlap that applies
    /// to ANY node, not just the front one (a later node can start before the cursor), so
    /// clamp every node's reachable range to start at the cursor.
    const size_t cursor = nodes.empty() ? 0 : nodes.front().logical_offset + front_offset;
    for (const auto & node : nodes)
    {
        size_t effective_buffer_offset = node.buffer_offset;
        size_t effective_size = node.size;
        size_t effective_logical = node.logical_offset;
        if (effective_logical < cursor)
        {
            const size_t skip = cursor - effective_logical;
            if (skip >= effective_size)
                continue;  /// node entirely consumed
            effective_buffer_offset += skip;
            effective_size -= skip;
            effective_logical = cursor;
        }

        size_t node_start = effective_logical;
        size_t node_end = node_start + effective_size;
        size_t req_end = req.end();

        if (node_start >= req_end)
            break;
        if (node_end <= req.offset)
            continue;

        size_t overlap_start = std::max(node_start, req.offset);
        size_t overlap_end = std::min(node_end, req_end);
        size_t trim_front = overlap_start - node_start;

        RopeNode sliced;
        sliced.buffer = node.buffer;
        sliced.buffer_offset = effective_buffer_offset + trim_front;
        sliced.size = overlap_end - overlap_start;
        sliced.logical_offset = overlap_start;
        /// Go through `append` so intervals on the result are maintained.
        result.append(std::move(sliced));
    }
    return result;
}

size_t Rope::totalBytes() const
{
    if (nodes.empty())
        return 0;
    size_t total = 0;
    for (const auto & node : nodes)
        total += node.size;
    return total - front_offset;
}

size_t Rope::coveredBytes(ByteRange req) const
{
    if (req.size == 0)
        return 0;
    size_t total = 0;
    /// First interval whose `end > req.offset` — the first one that could
    /// contribute coverage.
    auto it = std::lower_bound(intervals.begin(), intervals.end(), req.offset,
        [](const ByteRange & ex, size_t v) { return ex.end() <= v; });
    for (; it != intervals.end() && it->offset < req.end(); ++it)
    {
        size_t lo = std::max(it->offset, req.offset);
        size_t hi = std::min(it->end(), req.end());
        if (lo < hi)
            total += hi - lo;
    }
    return total;
}

VectorWithMemoryTracking<ByteRange> Rope::gaps(ByteRange req) const
{
    VectorWithMemoryTracking<ByteRange> result;
    if (req.size == 0)
        return result;

    size_t cur = req.offset;
    auto it = std::lower_bound(intervals.begin(), intervals.end(), req.offset,
        [](const ByteRange & ex, size_t v) { return ex.end() <= v; });
    for (; it != intervals.end() && it->offset < req.end(); ++it)
    {
        if (it->offset > cur)
            result.push_back({cur, it->offset - cur});
        cur = std::max(cur, it->end());
    }
    if (cur < req.end())
        result.push_back({cur, req.end() - cur});
    return result;
}

bool Rope::covers(ByteRange req) const
{
    if (req.size == 0)
        return true;
    /// Find the first interval whose end is past req.offset. By the disjoint
    /// invariant, `req` is fully covered iff that interval starts at or
    /// before `req.offset` AND ends at or after `req.end()`.
    auto it = std::lower_bound(intervals.begin(), intervals.end(), req.offset,
        [](const ByteRange & ex, size_t v) { return ex.end() <= v; });
    return it != intervals.end() && it->offset <= req.offset && it->end() >= req.end();
}

Rope Rope::extract(ByteRange req) const
{
    chassert(covers(req)); /// caller's invariant
    return slice(req);
}

void Rope::shift(ssize_t delta)
{
    for (auto & node : nodes)
        node.logical_offset = static_cast<size_t>(static_cast<ssize_t>(node.logical_offset) + delta);
    for (auto & iv : intervals)
        iv.offset = static_cast<size_t>(static_cast<ssize_t>(iv.offset) + delta);
}

size_t Rope::copyTo(char * dst, ByteRange req) const
{
    chassert(covers(req));
    /// Flatten assumes non-overlapping nodes (see the overlap contract). Overlap would
    /// double-write `dst` and over-count `written`; producers must hand a disjoint rope.
    chassert(coveredBytes(range()) == totalBytes());
    /// Nodes are sorted by logical_offset (invariant). The first node's
    /// `front_offset` bytes are already consumed — they're not part of
    /// the reachable bytes and `covers(req)` must have ruled them out.
    size_t written = 0;
    bool first = true;
    for (const auto & n : nodes)
    {
        size_t node_logical = n.logical_offset;
        size_t node_buffer_off = n.buffer_offset;
        size_t node_size = n.size;
        if (first)
        {
            node_logical += front_offset;
            node_buffer_off += front_offset;
            node_size -= front_offset;
            first = false;
        }

        if (node_logical >= req.end())
            break;
        size_t lo = std::max(node_logical, req.offset);
        size_t hi = std::min(node_logical + node_size, req.end());
        if (lo >= hi)
            continue;
        size_t src_off = node_buffer_off + (lo - node_logical);
        size_t copy = hi - lo;
        std::memcpy(dst + (lo - req.offset), n.buffer->data() + src_off, copy);
        written += copy;
    }
    return written;
}

}
