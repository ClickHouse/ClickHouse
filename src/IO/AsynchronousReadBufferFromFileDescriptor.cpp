#include <cerrno>
#include <ctime>
#include <optional>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <IO/AsynchronousReadBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


namespace ProfileEvents
{
    extern const Event AsynchronousReadWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}


std::string AsynchronousReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


std::future<IAsynchronousReader::Result> AsynchronousReadBufferFromFileDescriptor::readInto(char * data, size_t size)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<IAsynchronousReader::LocalFileDescriptor>(fd);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = priority;
    request.ignore = bytes_to_ignore;
    bytes_to_ignore = 0;

    /// This is a workaround of a read pass EOF bug in linux kernel with pread()
    if (file_size.has_value() && file_offset_of_buffer_end >= *file_size)
    {
        return std::async(std::launch::deferred, []
        {
            return IAsynchronousReader::Result{ .size = 0, .offset = 0 };
        });
    }

    return reader->submit(request);
}


void AsynchronousReadBufferFromFileDescriptor::prefetch()
{
    if (prefetch_future.valid())
        return;

    /// Will request the same amount of data that is read in nextImpl.
    prefetch_buffer.resize(internal_buffer.size());
    prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
}


bool AsynchronousReadBufferFromFileDescriptor::nextImpl()
{
    if (prefetch_future.valid())
    {
        /// Read request already in flight. Wait for its completion.

        size_t size = 0;
        size_t offset = 0;
        {
            Stopwatch watch;
            CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
            auto result = prefetch_future.get();
            size = result.size;
            offset = result.offset;
            assert(offset < size || size == 0);
            ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
        }

        prefetch_future = {};
        file_offset_of_buffer_end += size;

        if (size)
        {
            prefetch_buffer.swap(memory);
            /// Adjust the working buffer so that it ignores `offset` bytes.
            setWithBytesToIgnore(memory.data(), size, offset);
            return true;
        }

        return false;
    }
    else
    {
        /// No pending request. Do synchronous read.

        auto [size, offset] = readInto(memory.data(), memory.size()).get();
        file_offset_of_buffer_end += size;

        if (size)
        {
            /// Adjust the working buffer so that it ignores `offset` bytes.
            setWithBytesToIgnore(memory.data(), size, offset);
            return true;
        }

        return false;
    }
}


void AsynchronousReadBufferFromFileDescriptor::finalize()
{
    if (prefetch_future.valid())
    {
        prefetch_future.wait();
        prefetch_future = {};
    }
}


AsynchronousReadBufferFromFileDescriptor::AsynchronousReadBufferFromFileDescriptor(
    AsynchronousReaderPtr reader_,
    Int32 priority_,
    int fd_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(buf_size, existing_memory, alignment, file_size_)
    , reader(std::move(reader_))
    , priority(priority_)
    , required_alignment(alignment)
    , fd(fd_)
{
    if (required_alignment > buf_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Too large alignment. Cannot have required_alignment greater than buf_size: {} > {}. It is a bug",
            required_alignment,
            buf_size);

    prefetch_buffer.alignment = alignment;
}

AsynchronousReadBufferFromFileDescriptor::~AsynchronousReadBufferFromFileDescriptor()
{
    finalize();
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

    while (true)
    {
        if (file_offset_of_buffer_end - working_buffer.size() <= new_pos && new_pos <= file_offset_of_buffer_end)
        {
            /// Position is still inside the buffer.
            /// Probably it is at the end of the buffer - then we will load data on the following 'next' call.

            pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return new_pos;
        }
        else if (prefetch_future.valid())
        {
            /// Read from prefetch buffer and recheck if the new position is valid inside.

            if (nextImpl())
                continue;
        }

        break;
    }

    assert(!prefetch_future.valid());

    /// Position is out of the buffer, we need to do real seek.
    off_t seek_pos = required_alignment > 1
        ? new_pos / required_alignment * required_alignment
        : new_pos;

    /// First reset the buffer so the next read will fetch new data to the buffer.
    resetWorkingBuffer();

    /// Just update the info about the next position in file.

    file_offset_of_buffer_end = seek_pos;
    bytes_to_ignore = new_pos - seek_pos;

    assert(bytes_to_ignore < internal_buffer.size());

    return seek_pos;
}


void AsynchronousReadBufferFromFileDescriptor::rewind()
{
    if (prefetch_future.valid())
    {
        prefetch_future.wait();
        prefetch_future = {};
    }

    /// Clearing the buffer with existing data. New data will be read on subsequent call to 'next'.
    working_buffer.resize(0);
    pos = working_buffer.begin();
    file_offset_of_buffer_end = 0;
}

}
