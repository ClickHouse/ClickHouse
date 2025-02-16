#pragma once
#include <forward_list>

#include <IO/WriteBuffer.h>
#include <IO/IReadableWriteBuffer.h>
#include <Common/Allocator.h>
#include <Core/Defines.h>
#include <boost/noncopyable.hpp>


namespace DB
{

/// Stores data in memory chunks, size of chunks are exponentially increasing during write
/// Written data could be reread after write
class MemoryWriteBuffer : public WriteBuffer, public IReadableWriteBuffer, boost::noncopyable, private Allocator<false>
{
public:
    /// Special exception to throw when the current MemoryWriteBuffer cannot receive data
    class CurrentBufferExhausted : public std::exception
    {
    public:
        const char * what() const noexcept override { return "WriteBuffer limit is exhausted"; }
    };

    /// Use max_total_size_ = 0 for unlimited storage
    explicit MemoryWriteBuffer(
        size_t max_total_size_ = 0,
        size_t initial_chunk_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        double growth_rate_ = 2.0,
        size_t max_chunk_size_ = 128 * DBMS_DEFAULT_BUFFER_SIZE);

    ~MemoryWriteBuffer() override;

protected:

    void nextImpl() override;

    void finalizeImpl() override { /* no op */ }

    std::unique_ptr<ReadBuffer> getReadBufferImpl() override;

    const size_t max_total_size;
    const size_t initial_chunk_size;
    const size_t max_chunk_size;
    const double growth_rate;

    using Container = std::forward_list<BufferBase::Buffer>;

    Container chunk_list;
    Container::iterator chunk_tail;
    size_t total_chunks_size = 0;

    void addChunk();

    friend class ReadBufferFromMemoryWriteBuffer;
};


}
