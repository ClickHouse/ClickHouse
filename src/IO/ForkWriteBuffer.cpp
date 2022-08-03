#include <IO/ForkWriteBuffer.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_IO_BUFFER;
}

ForkWriteBuffer::ForkWriteBuffer(WriteBufferPtrs && sources_)
    : WriteBuffer(nullptr, 0), sources(std::move(sources_))
{
    if (sources.empty())
    {
        throw Exception("Expected non-zero number of buffers for `ForkWriteBuffer`", ErrorCodes::CANNOT_CREATE_IO_BUFFER);
    }
    set(sources.front()->buffer().begin(), sources.front()->buffer().size());
}


void ForkWriteBuffer::nextImpl()
{
    sources.front()->position() = position();

    try
    {
        auto & source_buffer = sources.front();
        for (auto it = sources.begin() + 1; it != sources.end(); ++it)
        {
            auto & buffer = *it;
            buffer->write(source_buffer->buffer().begin(), source_buffer->offset());
            buffer->next();
        }
        source_buffer->next();
    }
    catch (Exception & exception)
    {
        exception.addMessage("While writing to ForkWriteBuffer");
        throw;
    }

}

void ForkWriteBuffer::finalizeImpl()
{
    for (const WriteBufferPtr & buffer : sources)
    {
        buffer->finalize();
    }
}

ForkWriteBuffer::~ForkWriteBuffer()
{
    finalize();
}


}
