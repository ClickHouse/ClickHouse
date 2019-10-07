#pragma once
#include <forward_list>

#include <IO/WriteBuffer.h>
#include <IO/IReadableWriteBuffer.h>
#include <Common/Allocator.h>
#include <Core/Defines.h>
#include <boost/noncopyable.hpp>


namespace DB
{

/// Stores data in memory chunks, size of cunks are exponentially increasing during write
/// Written data could be reread after write
class MemoryWriteBuffer : public WriteBuffer, public IReadableWriteBuffer, boost::noncopyable, private Allocator<false>
{
public:

    /// Use max_total_size_ = 0 for unlimited storage
    MemoryWriteBuffer(
        size_t max_total_size_ = 0,
        size_t initial_chunk_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        double growth_rate_ = 2.0,
        size_t max_chunk_size_ = 128 * DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    ~MemoryWriteBuffer() override;

protected:

    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

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
