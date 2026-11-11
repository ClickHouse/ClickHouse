#include <IO/ChainedBuffers.h>
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
    extern const Metric ReaderExecutorChainedBufferBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    Allocator<false, false> chained_buffer_allocator;

    /// SIMD-padded allocation size, with the same overflow check `Memory::withPadding` uses.
    size_t paddedSize(size_t size)
    {
        size_t res = 0;
        if (common::addOverflow<size_t>(size, PADDING_FOR_SIMD, res))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "OwnedChainedBuffer size {} is too big to pad", size);
        return res;
    }
}

OwnedChainedBuffer::OwnedChainedBuffer(size_t size)
    /// Allocate with SIMD over-read padding, but keep `buf_size` the logical
    /// (usable) size: `size()` and every node / coverage bound must exclude the
    /// `PADDING_FOR_SIMD` bytes, or a read would expose them and the
    /// `ReaderExecutorChainedBufferBytes` gauge would over-count live memory.
    : buf_data(static_cast<char *>(chained_buffer_allocator.alloc(paddedSize(size))))
    , buf_size(size)
{
    CurrentMetrics::add(CurrentMetrics::ReaderExecutorChainedBufferBytes, buf_size);
}

OwnedChainedBuffer::~OwnedChainedBuffer()
{
    CurrentMetrics::sub(CurrentMetrics::ReaderExecutorChainedBufferBytes, buf_size);
    /// `paddedSize` succeeded in the ctor, so `buf_size + PADDING_FOR_SIMD` cannot overflow.
    chained_buffer_allocator.free(buf_data, buf_size + PADDING_FOR_SIMD);
}

ByteRange ChainedBuffers::range() const
{
    if (intervals.empty())
        return {0, 0};
    size_t start = intervals.front().offset;
    size_t end = intervals.back().end();
    return {start, end - start};
}

ChainedBuffers::Span ChainedBuffers::peek() const
{
    if (nodes.empty())
        return {};
    const ChainedBufferNode & n = nodes.front();
    return Span{
        const_cast<char *>(n.data()) + front_offset,
        n.size - front_offset,
        n.logical_offset + front_offset,
    };
}

void ChainedBuffers::extendIntervalsFront(size_t bytes)
{
    if (bytes == 0)
        return;
    chassert(!intervals.empty());
    chassert(intervals.front().offset >= bytes);
    intervals.front().offset -= bytes;
    intervals.front().size += bytes;
}

void ChainedBuffers::advance(size_t bytes)
{
    /// advance(0) must stay a pure no-op: anchoring `consumed_pos` to the front without
    /// consuming anything would make a fresh chain reject legal out-of-order appends.
    if (bytes == 0 || nodes.empty())
        return;

    /// Work from an absolute position, not by popping the front node's physical size: that
    /// is what drops an overlapping node sitting entirely behind the cursor (so it is never
    /// re-served). Clamp the delta, not `cur + bytes`, to avoid overflow.
    const size_t cur = nodes.front().logical_offset + front_offset;
    const size_t new_position = cur + std::min(bytes, range().end() - cur);
    consumed_pos = new_position;

    /// Drop coverage before the new position (cascades across intervals, e.g. over a gap).
    while (!intervals.empty() && intervals.front().end() <= new_position)
        intervals.erase(intervals.begin());
    if (!intervals.empty() && intervals.front().offset < new_position)
    {
        intervals.front().size -= new_position - intervals.front().offset;
        intervals.front().offset = new_position;
    }

    while (!nodes.empty() && nodes.front().logical_offset + nodes.front().size <= new_position)
        nodes.pop_front();

    front_offset = (!nodes.empty() && new_position > nodes.front().logical_offset)
        ? new_position - nodes.front().logical_offset
        : 0;
}

bool ChainedBuffers::tryRewind(size_t new_position)
{
    if (nodes.empty())
        return false;

    /// Lowest reachable byte is the ORIGINAL front start (`logical_offset`, not
    /// `+ front_offset`) so a backward rewind into the buffer works; highest is the
    /// merged-coverage end (`nodes` are sorted by start, so a later node can end earlier).
    const size_t reachable_lo = nodes.front().logical_offset;
    const size_t reachable_hi = range().end();
    if (new_position < reachable_lo || new_position > reachable_hi)
        return false;

    const ChainedBufferNode & front = nodes.front();
    const size_t front_end = front.logical_offset + front.size;
    const size_t cur = front.logical_offset + front_offset;

    if (new_position < front_end)
    {
        /// Inside the front node. A forward move reuses `advance` (it trims the
        /// front interval and updates `front_offset` / `consumed_pos`); a backward
        /// move re-opens the rewound-into coverage by extending the front interval,
        /// which `advance` cannot do.
        if (new_position >= cur)
            advance(new_position - cur);
        else
        {
            extendIntervalsFront(cur - new_position);
            front_offset = new_position - front.logical_offset;
            consumed_pos = new_position;
        }
        return true;
    }

    /// Past the front node: reject a position landing in a gap, else let `advance` move
    /// the cursor there by logical distance.
    for (size_t i = 1; i < nodes.size(); ++i)
    {
        const ChainedBufferNode & node = nodes[i];
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

void ChainedBuffers::mergeInterval(ByteRange iv)
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

void ChainedBuffers::append(ChainedBufferNode node)
{
    /// Drop zero-size nodes -- not merged into `intervals`, they would leave a non-empty
    /// chain whose `peek` span is empty (hanging a drain loop).
    if (node.size == 0)
        return;

    /// Never insert behind the consumed frontier: drop a node entirely behind it, trim one
    /// straddling it. This keeps consumed bytes from being re-covered and keeps `front_offset`
    /// applying to the front node. A fresh chain has `consumed_pos == 0`, so out-of-order
    /// appends are unaffected.
    if (node.logical_offset + node.size <= consumed_pos)
        return;
    if (node.logical_offset < consumed_pos)
    {
        const size_t trim = consumed_pos - node.logical_offset;
        node.buffer_offset += trim;
        node.size -= trim;
        node.logical_offset = consumed_pos;
    }

    /// Insert into `nodes` keeping the sort by logical_offset (stable on tie:
    /// equal-offset nodes keep insertion order).
    ByteRange node_range = node.range();
    auto it = std::upper_bound(nodes.begin(), nodes.end(), node.logical_offset,
        [](size_t v, const ChainedBufferNode & n) { return v < n.logical_offset; });
    nodes.insert(it, std::move(node));
    mergeInterval(node_range);
}

void ChainedBuffers::append(ChainedBuffers && other)
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

    /// Route each moved node through single-node `append` so the cursor-trim and zero-size
    /// guard apply uniformly -- a raw splice would bypass them when `*this` is consumed.
    for (auto & node : other.nodes)
        append(std::move(node));

    other.nodes.clear();
    other.intervals.clear();
}

ChainedBuffers ChainedBuffers::slice(ByteRange req) const
{
    ChainedBuffers result;
    /// Bytes before the consumed frontier are not slice-able; clamp every node to it (with
    /// overlap a later node can start before it too, not just the front).
    const size_t cursor = consumed_pos;
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

        ChainedBufferNode sliced;
        sliced.buffer = node.buffer;
        sliced.buffer_offset = effective_buffer_offset + trim_front;
        sliced.size = overlap_end - overlap_start;
        sliced.logical_offset = overlap_start;
        /// Go through `append` so intervals on the result are maintained.
        result.append(std::move(sliced));
    }
    return result;
}

size_t ChainedBuffers::totalBytes() const
{
    if (nodes.empty())
        return 0;
    size_t total = 0;
    for (const auto & node : nodes)
        total += node.size;
    return total - front_offset;
}

size_t ChainedBuffers::coveredBytes(ByteRange req) const
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

VectorWithMemoryTracking<ByteRange> ChainedBuffers::gaps(ByteRange req) const
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

bool ChainedBuffers::covers(ByteRange req) const
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

ChainedBuffers ChainedBuffers::extract(ByteRange req) const
{
    chassert(covers(req)); /// caller's invariant
    return slice(req);
}

void ChainedBuffers::shift(ssize_t delta)
{
    for (auto & node : nodes)
        node.logical_offset = static_cast<size_t>(static_cast<ssize_t>(node.logical_offset) + delta);
    for (auto & iv : intervals)
        iv.offset = static_cast<size_t>(static_cast<ssize_t>(iv.offset) + delta);
    /// `consumed_pos` is a logical coordinate too; re-base it (clamped at 0 so a fresh
    /// chain's 0 with a negative delta doesn't underflow).
    consumed_pos = static_cast<size_t>(std::max<ssize_t>(0, static_cast<ssize_t>(consumed_pos) + delta));
}

size_t ChainedBuffers::copyTo(char * dst, ByteRange req) const
{
    chassert(covers(req));
    /// Flatten assumes non-overlapping nodes; overlap would double-write `dst` / over-count.
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
