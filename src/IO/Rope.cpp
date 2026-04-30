#include <IO/Rope.h>

#include <algorithm>

namespace DB
{

OwnedRopeBuffer::OwnedRopeBuffer(size_t size)
    : buf_data(static_cast<char *>(::operator new(size)))
    , buf_size(size)
{
}

OwnedRopeBuffer::~OwnedRopeBuffer()
{
    ::operator delete(buf_data);
}

void OwnedRopeBuffer::transferTo(MemoryTracker * /* new_tracker */)
{
    /// Will be implemented when PageCacheProvider needs it.
}

Range Rope::range() const
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

Rope Rope::slice(Range req) const
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

}
