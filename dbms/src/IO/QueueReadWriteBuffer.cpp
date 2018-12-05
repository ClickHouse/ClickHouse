#include <IO/QueueReadWriteBuffer.h>
#include <boost/noncopyable.hpp>
#include <common/likely.h>


namespace DB
{
void BufferDeleter::operator()(BufferBase::Buffer * buf)
{
    free(buf->begin(), buf->size());
    delete buf;
}


class ReadBufferFromQueueWriteBuffer : public ReadBuffer, boost::noncopyable
{
public:
    explicit ReadBufferFromQueueWriteBuffer(QueueWriteBuffer::ContainerPtr & chunk_list_) : ReadBuffer(nullptr, 0), chunk_list(chunk_list_)
    {
    }

    bool nextImpl() override { return setChunk(); }

private:
    bool setChunk()
    {
        while (!chunk_list->tryPop(current))
            ;
        if (current.second)
        {
            internalBuffer() = *current.first;
            buffer() = Buffer(internalBuffer().begin(), current.second);
            position() = buffer().begin();
        }
        else // poison
        {
            buffer() = internalBuffer() = Buffer(nullptr, nullptr);
            position() = nullptr;
            chunk_list->emplace(nullptr, nullptr); // repoison
        }

        return buffer().size() != 0;
    }

    QueueWriteBuffer::ContainerPtr chunk_list;
    std::pair<QueueWriteBuffer::Element, Position> current;
};


QueueWriteBuffer::QueueWriteBuffer(size_t chunk_size_)
    : WriteBuffer(nullptr, 0), chunk_size(chunk_size_), chunk_list(std::make_shared<Container>(2)) // double buffering
{
    addChunk();
}


void QueueWriteBuffer::nextImpl()
{
    if (unlikely(hasPendingData()))
    {
        /// ignore flush
        buffer() = Buffer(pos, buffer().end());
        return;
    }

    addChunk();
}


void QueueWriteBuffer::addChunk()
{
    if (current)
        chunk_list->emplace(std::move(current), position());
    Position begin = reinterpret_cast<Position>(alloc(chunk_size));
    set(begin, chunk_size);
    current.reset(new BufferBase::Buffer(internalBuffer()));
}


std::shared_ptr<ReadBuffer> QueueWriteBuffer::getReadBufferImpl()
{
    return std::make_shared<ReadBufferFromQueueWriteBuffer>(chunk_list);
}


void QueueWriteBuffer::flush()
{
    if (current)
        chunk_list->emplace(std::move(current), position());
    chunk_list->emplace(std::move(current), nullptr); // poison
}

QueueWriteBuffer::~QueueWriteBuffer()
{
    flush();
}


}
