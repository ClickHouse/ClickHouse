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
        throw Exception(ErrorCodes::CANNOT_CREATE_IO_BUFFER, "Expected non-zero number of buffers for `ForkWriteBuffer`");
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
            buffer->write(
                source_buffer->buffer().begin() + secondary_buffer_offset,
                source_buffer->offset() - secondary_buffer_offset);
            buffer->next();
        }
        source_buffer->next();
        set(sources.front()->buffer().begin(), sources.front()->buffer().size());
        secondary_buffer_offset = source_buffer->offset();
        nextimpl_working_buffer_offset = secondary_buffer_offset;
    }
    catch (Exception & exception)
    {
        exception.addMessage("While writing to ForkWriteBuffer");
        throw;
    }

}

void ForkWriteBuffer::preFinalize()
{
    WriteBuffer::preFinalize();
    for (const WriteBufferPtr & buffer : sources)
        buffer->preFinalize();
}

void ForkWriteBuffer::finalizeImpl()
{
    preFinalize();
    for (const WriteBufferPtr & buffer : sources)
        buffer->finalize();
}

void ForkWriteBuffer::cancelImpl() noexcept
{
    WriteBuffer::cancelImpl();
    for (const WriteBufferPtr & buffer : sources)
        buffer->cancel();
}

}
