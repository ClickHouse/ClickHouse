#pragma once

#include <cstddef>
#include <memory>

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

    char * data() override { return data_; }
    const char * data() const override { return data_; }
    size_t size() const override { return size_; }
    void transferTo(MemoryTracker * new_tracker) override;

private:
    char * data_;
    size_t size_;
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

}
