#include <errno.h>
#include <time.h>
#include <optional>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <IO/AsynchronousReadBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


namespace DB
{


std::string AsynchronousReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


void AsynchronousReadBufferFromFileDescriptor::prefetch()
{
    if (prefetch_request_id)
        return;

    /// Will request the same amount of data that is read in nextImpl.
    prefetch_buffer.resize(internal_buffer.size());

    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<IAsynchronousReader::LocalFileDescriptor>(fd);
    request.buf = prefetch_buffer.data();
    request.size = prefetch_buffer.size();
    request.offset = file_offset_of_buffer_end;

    prefetch_request_id = reader->submit(request);
}


bool AsynchronousReadBufferFromFileDescriptor::nextImpl()
{
    if (!prefetch_request_id)
        prefetch();

    auto response = reader->wait(*prefetch_request_id, {});
    prefetch_request_id.reset();

    if (response->exception)
        std::rethrow_exception(response->exception);

    file_offset_of_buffer_end += response->size;

    if (response->size)
    {
        prefetch_buffer.swap(memory);
        set(memory.data(), memory.size());
        working_buffer.resize(response->size);
        return true;
    }

    return false;
}


AsynchronousReadBufferFromFileDescriptor::~AsynchronousReadBufferFromFileDescriptor()
{
    if (prefetch_request_id)
    {
        reader->wait(*prefetch_request_id, {});
        prefetch_request_id.reset();
    }
}


/// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
off_t AsynchronousReadBufferFromFileDescriptor::seek(off_t offset, int whence)
{
    size_t new_pos;
    if (whence == SEEK_SET)
    {
        assert(offset >= 0);
        new_pos = offset;
    }
    else if (whence == SEEK_CUR)
    {
        new_pos = file_offset_of_buffer_end - (working_buffer.end() - pos) + offset;
    }
    else
    {
        throw Exception("ReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    /// Position is unchanged.
    if (new_pos + (working_buffer.end() - pos) == file_offset_of_buffer_end)
        return new_pos;

    /// file_offset_of_buffer_end corresponds to working_buffer.end(); it's a past-the-end pos,
    /// so the second inequality is strict.
    if (file_offset_of_buffer_end - working_buffer.size() <= static_cast<size_t>(new_pos)
        && new_pos < file_offset_of_buffer_end)
    {
        /// Position is still inside the buffer.

        pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
        assert(pos >= working_buffer.begin());
        assert(pos < working_buffer.end());

        return new_pos;
    }
    else
    {
        if (prefetch_request_id)
        {
            reader->wait(*prefetch_request_id, {});
            prefetch_request_id.reset();
        }

        /// Position is out of the buffer, we need to do real seek.
        off_t seek_pos = required_alignment > 1
            ? new_pos / required_alignment * required_alignment
            : new_pos;

        off_t offset_after_seek_pos = new_pos - seek_pos;

        /// First put position at the end of the buffer so the next read will fetch new data to the buffer.
        pos = working_buffer.end();

        /// Just update the info about the next position in file.

        file_offset_of_buffer_end = seek_pos;

        if (offset_after_seek_pos > 0)
            ignore(offset_after_seek_pos);

        return seek_pos;
    }
}


void AsynchronousReadBufferFromFileDescriptor::rewind()
{
    if (prefetch_request_id)
    {
        reader->wait(*prefetch_request_id, {});
        prefetch_request_id.reset();
    }

    /// Clearing the buffer with existing data. New data will be read on subsequent call to 'next'.
    working_buffer.resize(0);
    pos = working_buffer.begin();
    file_offset_of_buffer_end = 0;
}

}

