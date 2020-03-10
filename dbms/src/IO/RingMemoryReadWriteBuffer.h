#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Allocator.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class RingMemoryReadWriteBuffer
    : public WriteBuffer, boost::noncopyable, private Allocator<false>, public std::enable_shared_from_this<RingMemoryReadWriteBuffer>
{
public:
    ~RingMemoryReadWriteBuffer() override;

    RingMemoryReadWriteBuffer(size_t buffers_size_ = 8, size_t each_buffer_capacity_ = DBMS_DEFAULT_BUFFER_SIZE);

    void finalize() override;

    std::shared_ptr<ReadBuffer> tryGetReadBuffer(size_t max_read_bytes, UInt64 milliseconds = 0);

private:
    const size_t buffers_size;
    const size_t each_buffer_capacity;
    const size_t total_buffers_capacity;

    std::mutex mutex;
    std::condition_variable fill_condition;
    std::condition_variable empty_condition;

    size_t size = 0;
    size_t write_buffers_offset = 0;
    std::atomic_bool finished = false;
    std::pair<size_t, size_t> read_offsets = {0, 0};
    std::vector<std::shared_ptr<BufferBase::Buffer>> buffers;

    void nextImpl() override;

    void revert(size_t revert_size);

    void setChunkToWorkingBuffer(size_t chunk_index);

    size_t tryReadBufferImpl(BufferBase::Buffer & read_buffer, size_t read_bytes, size_t wait_milliseconds);

    friend class RingMemoryReadBuffer;
};

}
