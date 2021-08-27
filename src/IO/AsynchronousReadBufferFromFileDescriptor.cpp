#include <errno.h>
#include <time.h>
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
        {
            Stopwatch watch;
            CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
            size = prefetch_future.get();
            ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
        }

        prefetch_future = {};
        file_offset_of_buffer_end += size;

        if (size)
        {
            prefetch_buffer.swap(memory);
            set(memory.data(), memory.size());
            working_buffer.resize(size);
            return true;
        }

        return false;
    }
    else
    {
        /// No pending request. Do synchronous read.

        auto size = readInto(memory.data(), memory.size()).get();
        file_offset_of_buffer_end += size;

        if (size)
        {
            set(memory.data(), memory.size());
            working_buffer.resize(size);
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

    if (file_offset_of_buffer_end - working_buffer.size() <= static_cast<size_t>(new_pos)
        && new_pos <= file_offset_of_buffer_end)
    {
        /// Position is still inside the buffer.
        /// Probably it is at the end of the buffer - then we will load data on the following 'next' call.

        pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return new_pos;
    }
    else
    {
        if (prefetch_future.valid())
        {
            //std::cerr << "Ignoring prefetched data" << "\n";
            prefetch_future.wait();
            prefetch_future = {};
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

