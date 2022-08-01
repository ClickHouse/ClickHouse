#include <IO/MemoryReadWriteBuffer.h>
#include <boost/noncopyable.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int CURRENT_WRITE_BUFFER_IS_EXHAUSTED;
}


class ReadBufferFromMemoryWriteBuffer : public ReadBuffer, boost::noncopyable, private Allocator<false>
{
public:
    explicit ReadBufferFromMemoryWriteBuffer(MemoryWriteBuffer && origin)
    : ReadBuffer(nullptr, 0),
        chunk_list(std::move(origin.chunk_list)),
        end_pos(origin.position())
    {
        chunk_head = chunk_list.begin();
        setChunk();
    }

    bool nextImpl() override
    {
        if (chunk_head == chunk_list.end())
            return false;

        ++chunk_head;
        return setChunk();
    }

    ~ReadBufferFromMemoryWriteBuffer() override
    {
        for (const auto & range : chunk_list)
            free(range.begin(), range.size());
    }

private:

    /// update buffers and position according to chunk_head pointer
    bool setChunk()
    {
        if (chunk_head != chunk_list.end())
        {
            internalBuffer() = *chunk_head;

            /// It is last chunk, it should be truncated
            if (std::next(chunk_head) != chunk_list.end())
                buffer() = internalBuffer();
            else
                buffer() = Buffer(internalBuffer().begin(), end_pos);

            position() = buffer().begin();
        }
        else
        {
            buffer() = internalBuffer() = Buffer(nullptr, nullptr);
            position() = nullptr;
        }

        return !buffer().empty();
    }

    using Container = std::forward_list<BufferBase::Buffer>;

    Container chunk_list;
    Container::iterator chunk_head;
    Position end_pos;
};


MemoryWriteBuffer::MemoryWriteBuffer(size_t max_total_size_, size_t initial_chunk_size_, double growth_rate_, size_t max_chunk_size_)
    : WriteBufferWithoutFinalize(nullptr, 0),
    max_total_size(max_total_size_),
    initial_chunk_size(initial_chunk_size_),
    max_chunk_size(max_chunk_size_),
    growth_rate(growth_rate_)
{
    addChunk();
}


void MemoryWriteBuffer::nextImpl()
{
    if (unlikely(hasPendingData()))
    {
        /// ignore flush
        buffer() = Buffer(pos, buffer().end());
        return;
    }

    addChunk();
}


void MemoryWriteBuffer::addChunk()
{
    size_t next_chunk_size;
    if (chunk_list.empty())
    {
        chunk_tail = chunk_list.before_begin();
        next_chunk_size = initial_chunk_size;
    }
    else
    {
        next_chunk_size = std::max(static_cast<size_t>(1), static_cast<size_t>(chunk_tail->size() * growth_rate));
        next_chunk_size = std::min(next_chunk_size, max_chunk_size);
    }

    if (max_total_size)
    {
        if (total_chunks_size + next_chunk_size > max_total_size)
            next_chunk_size = max_total_size - total_chunks_size;

        if (0 == next_chunk_size)
        {
            set(position(), 0);
            throw Exception("MemoryWriteBuffer limit is exhausted", ErrorCodes::CURRENT_WRITE_BUFFER_IS_EXHAUSTED);
        }
    }

    Position begin = reinterpret_cast<Position>(alloc(next_chunk_size));
    chunk_tail = chunk_list.emplace_after(chunk_tail, begin, begin + next_chunk_size);
    total_chunks_size += next_chunk_size;

    set(chunk_tail->begin(), chunk_tail->size());
}


std::shared_ptr<ReadBuffer> MemoryWriteBuffer::getReadBufferImpl()
{
    auto res = std::make_shared<ReadBufferFromMemoryWriteBuffer>(std::move(*this));

    /// invalidate members
    chunk_list.clear();
    chunk_tail = chunk_list.begin();

    return res;
}


MemoryWriteBuffer::~MemoryWriteBuffer()
{
    for (const auto & range : chunk_list)
        free(range.begin(), range.size());
}

}
