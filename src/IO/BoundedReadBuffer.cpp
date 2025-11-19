#include <IO/BoundedReadBuffer.h>
#include <IO/SwapHelper.h>

namespace DB
{

BoundedReadBuffer::BoundedReadBuffer(std::unique_ptr<SeekableReadBuffer> impl_)
    : ReadBufferFromFileDecorator(std::move(impl_))
{
}

void BoundedReadBuffer::setReadUntilPosition(size_t position)
{
    read_until_position = position;
}

void BoundedReadBuffer::setReadUntilEnd()
{
    read_until_position.reset();
}

off_t BoundedReadBuffer::getPosition()
{
    return file_offset_of_buffer_end - (working_buffer.end() - pos);
}

bool BoundedReadBuffer::nextImpl()
{
    if (read_until_position && file_offset_of_buffer_end == *read_until_position)
        return false;

    bool result;
    {
        SwapHelper swap(*this, *impl);
        result = impl->next();
    }
    chassert(file_offset_of_buffer_end + available() == impl->getFileOffsetOfBufferEnd());
    if (result && read_until_position)
    {
        size_t remaining_size_to_read = *read_until_position - file_offset_of_buffer_end;
        if (working_buffer.size() > remaining_size_to_read)
        {
            ///  file:            [______________________________]
            ///  working buffer:       [_______________]
            ///                                 ^
            ///                                 read_until_position
            ///                        ^
            ///                        file_offset_of_buffer_end
            working_buffer.resize(remaining_size_to_read);
        }
    }
    file_offset_of_buffer_end += available();
    return result;
}

off_t BoundedReadBuffer::seek(off_t off, int whence)
{
    swap(*impl);
    auto result = impl->seek(off, whence);
    swap(*impl);

    file_offset_of_buffer_end = impl->getFileOffsetOfBufferEnd();
    return result;
}

}
