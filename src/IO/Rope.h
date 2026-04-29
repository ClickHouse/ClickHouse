#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace DB
{

struct Range
{
    size_t offset = 0;
    size_t size = 0;
    size_t end() const { return offset + size; }
};

class MemoryTracker;

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

    char * data() { return buffer->data() + buffer_offset; }
    const char * data() const { return buffer->data() + buffer_offset; }
    Range range() const { return {logical_offset, size}; }
};

class RopeSlice;

/// Contiguous sequence of RopeNodes covering a logical range.
class Rope
{
public:
    Range range() const;
    void append(RopeNode node);
    void append(Rope && other);
    RopeSlice slice(Range req) const;
    const std::vector<RopeNode> & getNodes() const { return nodes; }
    bool empty() const { return nodes.empty(); }

private:
    std::vector<RopeNode> nodes;
};

/// Lightweight view into a Rope. Holds shared_ptr refs to buffers.
class RopeSlice
{
public:
    RopeSlice() = default;
    Range range() const;
    const std::vector<RopeNode> & getNodes() const { return nodes; }
    bool empty() const { return nodes.empty(); }
    size_t totalBytes() const;

private:
    std::vector<RopeNode> nodes;
    friend class Rope;
};

}
