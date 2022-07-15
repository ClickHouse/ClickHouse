#include <IO/ForkWriteBuffer.h>
#include <Common/Exception.h>
#include <ranges>


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
        throw Exception("ForkWriteBuffer required WriteBuffer is not provided", ErrorCodes::CANNOT_CREATE_IO_BUFFER);
    }
    set(sources.front()->buffer().begin(), sources.front()->buffer().size());
}


void ForkWriteBuffer::nextImpl()
{
    sources.front()->position() = position();

    try
    {
        for (const WriteBufferPtr & write_buffer : sources | std::views::reverse)
        {
            if (write_buffer != sources.front())
            {
                write_buffer->write(sources.front()->buffer().begin(), sources.front()->offset());
            }
            write_buffer->next();
        }
    }
    catch (Exception & exception)
    {
        exception.addMessage("While writing to ForkWriteBuffer");
        throw;
    }

}

ForkWriteBuffer::~ForkWriteBuffer()
{
    finalize();
}


}
