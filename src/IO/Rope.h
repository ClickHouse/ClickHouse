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
class Rope
{
public:
    ByteRange range() const;
    void append(RopeNode node);
    void append(Rope && other);
    Rope slice(ByteRange req) const;
    /// Remove and return the first node.
    RopeNode popFront();

    std::deque<RopeNode> & getNodes() { return nodes; }
    const std::deque<RopeNode> & getNodes() const { return nodes; }
    bool empty() const { return nodes.empty(); }
    size_t totalBytes() const;

private:
    std::deque<RopeNode> nodes;
};

}
