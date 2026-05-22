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
    if (nodes.empty())
        return {0, 0};
    size_t start = nodes.front().logical_offset;
    const auto & last = nodes.back();
    size_t end = last.logical_offset + last.size;
    return {start, end - start};
}

void Rope::append(RopeNode node)
{
    nodes.push_back(std::move(node));
}

void Rope::append(Rope && other)
{
    nodes.insert(
        nodes.end(),
        std::make_move_iterator(other.nodes.begin()),
        std::make_move_iterator(other.nodes.end()));
    other.nodes.clear();
}

RopeNode Rope::popFront()
{
    RopeNode node = std::move(nodes.front());
    nodes.pop_front();
    return node;
}

Rope Rope::slice(ByteRange req) const
{
    Rope result;
    for (const auto & node : nodes)
    {
        size_t node_start = node.logical_offset;
        size_t node_end = node_start + node.size;
        size_t req_end = req.end();

        if (node_end <= req.offset || node_start >= req_end)
            continue;

        size_t overlap_start = std::max(node_start, req.offset);
        size_t overlap_end = std::min(node_end, req_end);
        size_t trim_front = overlap_start - node_start;

        RopeNode sliced;
        sliced.buffer = node.buffer;
        sliced.buffer_offset = node.buffer_offset + trim_front;
        sliced.size = overlap_end - overlap_start;
        sliced.logical_offset = overlap_start;
        result.nodes.push_back(std::move(sliced));
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
    size_t covered = 0;
    for (const auto & node : nodes)
    {
        size_t node_start = node.logical_offset;
        size_t node_end = node_start + node.size;
        size_t lo = std::max(node_start, req.offset);
        size_t hi = std::min(node_end, req.end());
        if (lo < hi)
            covered += hi - lo;
    }
    return covered;
}

std::vector<ByteRange> Rope::gaps(ByteRange req) const
{
    /// Collect the intervals of `req` that each node covers, then return the
    /// complement. Nodes may be in any order and may overlap each other — we
    /// merge as we go via a sorted intervals list.
    std::vector<std::pair<size_t, size_t>> intervals;
    intervals.reserve(nodes.size());
    for (const auto & node : nodes)
    {
        size_t lo = std::max(node.logical_offset, req.offset);
        size_t hi = std::min(node.logical_offset + node.size, req.end());
        if (lo < hi)
            intervals.emplace_back(lo, hi);
    }
    std::sort(intervals.begin(), intervals.end());

    std::vector<ByteRange> result;
    size_t cur = req.offset;
    const size_t end = req.end();
    for (const auto & [lo, hi] : intervals)
    {
        if (lo > cur)
            result.push_back({cur, lo - cur});
        cur = std::max(cur, hi);
        if (cur >= end)
            break;
    }
    if (cur < end)
        result.push_back({cur, end - cur});
    return result;
}

bool Rope::covers(ByteRange req) const
{
    if (req.size == 0)
        return true;
    return coveredBytes(req) == req.size;
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
}

size_t Rope::copyTo(char * dst, ByteRange req) const
{
    chassert(covers(req));
    /// Walk nodes in sorted order so the output is contiguous regardless of
    /// node insertion order. (gaps() does the same.)
    std::vector<const RopeNode *> sorted;
    sorted.reserve(nodes.size());
    for (const auto & node : nodes)
        if (node.logical_offset + node.size > req.offset && node.logical_offset < req.end())
            sorted.push_back(&node);
    std::sort(sorted.begin(), sorted.end(),
        [](const RopeNode * a, const RopeNode * b) { return a->logical_offset < b->logical_offset; });

    size_t written = 0;
    for (const auto * n : sorted)
    {
        size_t lo = std::max(n->logical_offset, req.offset);
        size_t hi = std::min(n->logical_offset + n->size, req.end());
        if (lo >= hi)
            continue;
        size_t src_off = n->buffer_offset + (lo - n->logical_offset);
        size_t copy = hi - lo;
        std::memcpy(dst + (lo - req.offset), n->buffer->data() + src_off, copy);
        written += copy;
    }
    return written;
}

}
