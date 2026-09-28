#include <IO/ReadBufferFromFileView.h>
#include <cstddef>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

ReadBufferFromFileView::ReadBufferFromFileView(
    std::unique_ptr<ReadBufferFromFileBase> impl_, const String & file_name_, off_t left_bound_, off_t right_bound_)
    : impl(std::move(impl_))
    , file_name(file_name_)
    , left_bound(left_bound_)
    , right_bound(right_bound_)
    , file_offset_of_buffer_end(left_bound_)
    , original_working_buffer(working_buffer)
{
    /// Seek to the begin of file.
    impl->seek(left_bound, SEEK_SET);
    swap(*impl);

    file_offset_of_buffer_end += available();
    original_working_buffer = working_buffer;
    resizeWorkingBuffer();
}

void ReadBufferFromFileView::prefetch(Priority priority)
{
    executeWithOriginalBuffer([&]{ impl->prefetch(priority); });
}

void ReadBufferFromFileView::setReadUntilPosition(size_t position)
{
    read_until_position = left_bound + position;
    if (*read_until_position > right_bound)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Cannot read until position: {}. File size is {}", position, getFileSize());

    executeWithOriginalBuffer([&]{ impl->setReadUntilPosition(*read_until_position); });
    resizeWorkingBuffer();
}

void ReadBufferFromFileView::setReadUntilEnd()
{
    read_until_position.reset();
    executeWithOriginalBuffer([&]{ impl->setReadUntilPosition(right_bound); });
    resizeWorkingBuffer();
}

off_t ReadBufferFromFileView::getPosition()
{
    return (file_offset_of_buffer_end - left_bound) - (working_buffer.end() - pos);
}

bool ReadBufferFromFileView::nextImpl()
{
    size_t current_position = file_offset_of_buffer_end - (working_buffer.end() - pos);
    if (current_position == getRightBound())
        return false;

    bool result = false;
    executeWithOriginalBuffer([&] { result = impl->next(); });

    if (result)
    {
        file_offset_of_buffer_end += available();
        resizeWorkingBuffer();
    }

    return result;
}

off_t ReadBufferFromFileView::seek(off_t off, int whence)
{
    size_t new_pos = 0;
    size_t current_position = file_offset_of_buffer_end - (working_buffer.end() - pos);

    if (whence == SEEK_CUR)
        new_pos = current_position + off;
    else if (whence == SEEK_SET)
        new_pos = left_bound + off;
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "ReadBufferFromFileView::seek expects SEEK_SET or SEEK_CUR as whence");

    off_t result = 0;
    executeWithOriginalBuffer([&] { result = impl->seek(new_pos, SEEK_SET); });

    if (result < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position ({}) underflow", result);

    if (static_cast<size_t>(result) < left_bound || static_cast<size_t>(result) > right_bound)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
            "Seek position ({}) is out of bound. Available range: [{}, {}]", result, left_bound, right_bound);

    file_offset_of_buffer_end = result + available();
    resizeWorkingBuffer();

    return result - left_bound;
}

template <typename Op>
void ReadBufferFromFileView::executeWithOriginalBuffer(Op && op)
{
    /// Set original buffer as working buffer.
    working_buffer.swap(original_working_buffer);

    /// Set working buffer and other internal into impl.
    swap(*impl);
    op();
    swap(*impl);

    original_working_buffer = working_buffer;
}

size_t ReadBufferFromFileView::getRightBound() const
{
    return read_until_position ? *read_until_position : right_bound;
}

void ReadBufferFromFileView::resizeWorkingBuffer()
{
    if (file_offset_of_buffer_end > getRightBound())
    {
        size_t extra_bytes = file_offset_of_buffer_end - getRightBound();
        size_t new_size = std::max(static_cast<Int64>(working_buffer.size() - extra_bytes), static_cast<Int64>(0));

        working_buffer.resize(new_size);
        file_offset_of_buffer_end = getRightBound();
    }
}

}
