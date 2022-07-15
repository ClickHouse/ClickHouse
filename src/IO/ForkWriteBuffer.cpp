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
        first_buffer = nullptr;
        throw Exception("ForkWriteBuffer required WriteBuffer is not provided", ErrorCodes::CANNOT_CREATE_IO_BUFFER);
    }
    else
    {
        first_buffer = sources.begin()->get();
        set(first_buffer->buffer().begin(), first_buffer->buffer().size());
    }
}


void ForkWriteBuffer::nextImpl()
{
    if (!first_buffer)
        return;

    first_buffer->position() = position();

    try
    {
        for (const WriteBufferPtr & write_buffer : sources | std::views::reverse)
        {
            if (write_buffer.get() != first_buffer)
            {
                //if buffer size if not enough to write, then split the message with buffer length
                if (write_buffer->available() < first_buffer->offset())
                {
                    size_t bytes_written = 0;
                    auto to_be_written = first_buffer->offset();
                    while (to_be_written != 0)
                    {
                        int bytes_to_copy = std::min(to_be_written, write_buffer->available());
                        write_buffer->write(first_buffer->buffer().begin()+bytes_written, bytes_to_copy);
                        write_buffer->next();
                        bytes_written += bytes_to_copy;
                        to_be_written -= bytes_to_copy;
                    }
                }
                else
                    write_buffer->write(first_buffer->buffer().begin(), first_buffer->offset());
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

void ForkWriteBuffer::finalizeImpl()
{
    next();
}


ForkWriteBuffer::~ForkWriteBuffer()
{
    finalize();
}


}
