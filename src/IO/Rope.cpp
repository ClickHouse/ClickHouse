#include <IO/Rope.h>
#include <Common/Allocator.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Defines.h>

#include <algorithm>
#include <cstring>

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorRopeBytes;
}

namespace DB
{

namespace
{
    Allocator<false, false> rope_allocator;
}

OwnedRopeBuffer::OwnedRopeBuffer(size_t size)
    : buf_data(static_cast<char *>(rope_allocator.alloc(size + PADDING_FOR_SIMD)))
    , buf_size(size)
{
    CurrentMetrics::add(CurrentMetrics::ReaderExecutorRopeBytes, buf_size);
}

OwnedRopeBuffer::~OwnedRopeBuffer()
{
    CurrentMetrics::sub(CurrentMetrics::ReaderExecutorRopeBytes, buf_size);
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
    while (bytes > 0 && !nodes.empty())
    {
        const size_t available = nodes.front().size - front_offset;
        if (bytes >= available)
        {
            /// Consume the rest of the front node and release it.
            shrinkIntervalsFront(available);
            nodes.pop_front();
            front_offset = 0;
            bytes -= available;
        }
        else
        {
            /// Partial consumption inside the front node.
            shrinkIntervalsFront(bytes);
            front_offset += bytes;
            bytes = 0;
        }
    }
    /// If `bytes > 0` here the caller advanced past EOF — silently clamp.
}

bool Rope::tryRewind(size_t new_position)
{
    if (nodes.empty())
        return false;

    /// Reachable from the cursor = entire held nodes (their buffers are
    /// alive). The lowest reachable byte is the ORIGINAL start of the
    /// front node (`logical_offset`, not `+ front_offset`), because a
    /// backward rewind into the buffer is supported.
    const size_t reachable_lo = nodes.front().logical_offset;
    const size_t reachable_hi = nodes.back().logical_offset + nodes.back().size;
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

    /// new_position is past the front node. Walk later nodes by physical size
    /// (advance() pops whole nodes, so a logical gap contributes no bytes).
    /// Reject positions that land in a gap - no held node covers them, and a
    /// successful tryRewind must leave the cursor exactly on new_position.
    size_t phys = front.size - front_offset;
    for (size_t i = 1; i < nodes.size(); ++i)
    {
        const RopeNode & node = nodes[i];
        if (new_position < node.logical_offset)
            return false;
        if (new_position < node.logical_offset + node.size)
        {
            advance(phys + (new_position - node.logical_offset));
            return true;
        }
        phys += node.size;
    }

    /// new_position == reachable_hi (exclusive end of the last node): EOF.
    advance(phys);
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
    /// Nodes are sorted by `logical_offset`, so we can stop early. The
    /// first node has `front_offset` bytes already consumed by the cursor
    /// — those bytes are not slice-able. Subsequent nodes are unaffected.
    bool first = true;
    for (const auto & node : nodes)
    {
        size_t effective_buffer_offset = node.buffer_offset;
        size_t effective_size = node.size;
        size_t effective_logical = node.logical_offset;
        if (first)
        {
            effective_buffer_offset += front_offset;
            effective_size -= front_offset;
            effective_logical += front_offset;
            first = false;
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
