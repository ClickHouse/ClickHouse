#include <IO/Rope.h>
#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Core/Defines.h>

#include <algorithm>
#include <cstring>

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
}

OwnedRopeBuffer::~OwnedRopeBuffer()
{
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

RopeNode Rope::popFront()
{
    RopeNode node = std::move(nodes.front());
    nodes.pop_front();

    /// Rebuild `intervals` from the remaining nodes so that coverage queries
    /// (`range`, `covers`, `gaps`, `coveredBytes`) reflect what is actually
    /// still available. `PipelineReadBuffer::seek` reads `current_rope.range`
    /// to decide whether `new_pos` is reachable in the unconsumed nodes —
    /// leaving `intervals` stale here was a silent data-corruption bug
    /// (callers saw a "found in remaining rope" path even when the popped
    /// nodes contained the requested offset, and `nextImpl` then served
    /// bytes from a different region of the file as if they were the
    /// requested ones).
    ///
    /// Rebuild is O(n^2) worst case but n is small (~window_size /
    /// source_read_block_size, typically <10 nodes) and `popFront` is only
    /// called by streaming consumers.
    intervals.clear();
    for (const auto & n : nodes)
        mergeInterval(n.range());

    return node;
}

Rope Rope::slice(ByteRange req) const
{
    Rope result;
    /// Nodes are sorted by `logical_offset`, so we can stop early.
    for (const auto & node : nodes)
    {
        size_t node_start = node.logical_offset;
        size_t node_end = node_start + node.size;
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
        sliced.buffer_offset = node.buffer_offset + trim_front;
        sliced.size = overlap_end - overlap_start;
        sliced.logical_offset = overlap_start;
        /// Go through `append` so intervals on the result are maintained.
        result.append(std::move(sliced));
    }
    return result;
}

size_t Rope::totalBytes() const
{
    size_t total = 0;
    for (const auto & node : nodes)
        total += node.size;
    return total;
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

std::vector<ByteRange> Rope::gaps(ByteRange req) const
{
    std::vector<ByteRange> result;
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
    /// Nodes are sorted by logical_offset (invariant) and overlap, if any, is
    /// resolved by "later-inserted wins for the overlap bytes" — which falls
    /// out of the memcpy order naturally below.
    size_t written = 0;
    for (const auto & n : nodes)
    {
        if (n.logical_offset >= req.end())
            break;
        size_t lo = std::max(n.logical_offset, req.offset);
        size_t hi = std::min(n.logical_offset + n.size, req.end());
        if (lo >= hi)
            continue;
        size_t src_off = n.buffer_offset + (lo - n.logical_offset);
        size_t copy = hi - lo;
        std::memcpy(dst + (lo - req.offset), n.buffer->data() + src_off, copy);
        written += copy;
    }
    return written;
}

}
