#include "BoundedReadBuffer.h"

namespace DB
{

BoundedReadBuffer::BoundedReadBuffer(std::unique_ptr<SeekableReadBuffer> impl_) : ReadBufferFromFileDecorator(std::move(impl_)) {}

bool BoundedReadBuffer::nextImpl()
{
    if (read_until_position && file_offset_of_buffer_end == *read_until_position)
        return false;

    swap(*impl);
    auto result = impl->next();
    swap(*impl);

    if (read_until_position)
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

    file_offset_of_buffer_end += working_buffer.size();
    return result;
}

off_t BoundedReadBuffer::seek(off_t off, int whence)
{
    swap(*impl);
    auto result = impl->seek(off, whence);
    swap(*impl);

    file_offset_of_buffer_end = result;
    return result;
}

}
