#pragma once
#include <forward_list>

#include <optional>
#include <Core/Defines.h>
#include <IO/IReadableWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <boost/noncopyable.hpp>
#include <Common/Allocator.h>
#include <Common/ConcurrentBoundedQueue.h>


namespace DB
{
struct BufferDeleter : private Allocator<false>
{
    void operator()(BufferBase::Buffer * buf);
};

/// Stores data in a single producer, multiple consumers queue, size of chunks are exponentially increasing during write
/// Written data is consumed only once
class QueueWriteBuffer : public WriteBuffer, public IReadableWriteBuffer, boost::noncopyable, private Allocator<false>
{
public:
    QueueWriteBuffer(size_t chunk_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    void flush();

    ~QueueWriteBuffer() override;

protected:
    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

    const size_t chunk_size;

    using Element = std::unique_ptr<BufferBase::Buffer, BufferDeleter>;
    using Container = ConcurrentBoundedQueue<std::pair<Element, Position>>;
    using ContainerPtr = std::shared_ptr<Container>;
    ContainerPtr chunk_list;
    Element current;

    void addChunk();

    friend class ReadBufferFromQueueWriteBuffer;
};


}
