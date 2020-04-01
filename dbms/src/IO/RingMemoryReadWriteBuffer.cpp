#include <IO/RingMemoryReadWriteBuffer.h>
#include <common/defines.h>
#include <Common/Stopwatch.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class RingMemoryReadBuffer : public ReadBuffer
{
public:
    RingMemoryReadBuffer(size_t max_read_bytes_, UInt64 max_wait_millisecond_, const std::shared_ptr<RingMemoryReadWriteBuffer> & owner_buffer_)
        : ReadBuffer(nullptr, 0), max_read_bytes(max_read_bytes_), max_wait_millisecond(max_wait_millisecond_), owner_buffer(owner_buffer_)
    {}

private:
    size_t max_read_bytes;
    UInt64 max_wait_millisecond;
    std::shared_ptr<RingMemoryReadWriteBuffer> owner_buffer;

    Stopwatch watch;
    size_t read_bytes = 0;

    bool nextImpl() override
    {
        owner_buffer->revert(pos - buffer().begin());

        if (unlikely(hasPendingData()))
        {
            working_buffer = BufferBase::Buffer(pos, buffer().end());
            return true;
        }

        read_bytes += owner_buffer->tryReadBufferImpl(
            working_buffer, max_read_bytes - read_bytes,
            watch.elapsedMilliseconds() > max_wait_millisecond ? 0 : max_wait_millisecond - watch.elapsedMilliseconds()
        );

        return hasPendingData();
    }
};

RingMemoryReadWriteBuffer::RingMemoryReadWriteBuffer(size_t buffers_size_, size_t each_buffer_capacity_)
    : WriteBuffer(nullptr, 0), buffers_size(buffers_size_), each_buffer_capacity(each_buffer_capacity_)
    , total_buffers_capacity(buffers_size * each_buffer_capacity)
{
    setChunkToWorkingBuffer(0);
}

void RingMemoryReadWriteBuffer::finalize()
{
    if (!finished)
    {
        {
            finished = true;
            std::lock_guard lock{mutex};
        }

        nextImpl();
    }
}

void RingMemoryReadWriteBuffer::nextImpl()
{
    std::unique_lock<std::mutex> lock(mutex);

    size += offset();

    if (unlikely(hasPendingData()))
        WriteBuffer::set(pos, buffer().end() - pos);
    else
    {
        if (size >= total_buffers_capacity)
            empty_condition.wait(lock, [&]() { return size < total_buffers_capacity; });

        setChunkToWorkingBuffer(++write_buffers_offset % buffers_size);
    }

    fill_condition.notify_one();    /// notify on every buffer flush
}

void RingMemoryReadWriteBuffer::revert(size_t revert_size)
{
    {
        std::unique_lock<std::mutex> lock(mutex);

        size -= revert_size;
    }

    empty_condition.notify_one();
}

void RingMemoryReadWriteBuffer::setChunkToWorkingBuffer(size_t chunk_index)
{
    if (buffers.size() <= chunk_index)
    {
        Position begin = reinterpret_cast<Position>(alloc(each_buffer_capacity));
        buffers.emplace_back(std::make_shared<BufferBase::Buffer>(begin, begin + each_buffer_capacity));
    }

    size_t next_chunk_size = std::min(total_buffers_capacity - size, buffers[chunk_index]->size());
    WriteBuffer::set(buffers[chunk_index]->begin(), next_chunk_size);
}

std::shared_ptr<ReadBuffer> RingMemoryReadWriteBuffer::tryGetReadBuffer(size_t max_read_bytes, UInt64 milliseconds)
{
    if (max_read_bytes >= total_buffers_capacity)
        throw Exception("LOGICAL ERROR: RingMemoryBuffer read size greater than total_buffers_capacity.", ErrorCodes::LOGICAL_ERROR);

    return std::make_shared<RingMemoryReadBuffer>(max_read_bytes, milliseconds, shared_from_this());
}

size_t RingMemoryReadWriteBuffer::tryReadBufferImpl(BufferBase::Buffer & read_buffer, size_t read_bytes, size_t wait_milliseconds)
{
    std::unique_lock<std::mutex> lock(mutex);

    if (wait_milliseconds && !size && !finished)
        fill_condition.wait_for(lock, std::chrono::milliseconds(wait_milliseconds));

    const auto & [read_buffers_offset, read_bytes_offset] = read_offsets;
    size_t buffer_remain_bytes = buffers[read_buffers_offset]->size() - read_bytes_offset;

    size_t current_read_bytes = std::min({buffer_remain_bytes, read_bytes, size});

    if (current_read_bytes)
    {
        read_buffer = BufferBase::Buffer(buffers[read_buffers_offset]->begin() + read_bytes_offset,
            buffers[read_buffers_offset]->begin() + read_bytes_offset + current_read_bytes);

        if (buffer_remain_bytes == current_read_bytes)
            read_offsets = std::make_pair(read_buffers_offset + 1 % buffers_size, 0);
        else
            read_offsets = std::make_pair(read_buffers_offset, read_bytes_offset + current_read_bytes);
    }

    return current_read_bytes;
}

RingMemoryReadWriteBuffer::~RingMemoryReadWriteBuffer()
{
    try
    {
        std::unique_lock<std::mutex> lock(mutex);

        for (size_t index = 0; index < buffers.size(); ++index)
            free(buffers[index]->begin(), buffers[index]->size());
    }
    catch(...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


}

