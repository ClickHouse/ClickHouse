#include "AsynchronousReadIndirectBufferFromRemoteFS.h"

#include <Common/Stopwatch.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <base/logger_useful.h>


namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}

namespace ProfileEvents
{
    extern const Event AsynchronousReadWaitMicroseconds;
    extern const Event RemoteFSSeeks;
    extern const Event RemoteFSPrefetches;
    extern const Event RemoteFSCancelledPrefetches;
    extern const Event RemoteFSUnusedPrefetches;
    extern const Event RemoteFSPrefetchedReads;
    extern const Event RemoteFSUnprefetchedReads;
    extern const Event RemoteFSBuffers;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


AsynchronousReadIndirectBufferFromRemoteFS::AsynchronousReadIndirectBufferFromRemoteFS(
        AsynchronousReaderPtr reader_,
        Int32 priority_,
        std::shared_ptr<ReadBufferFromRemoteFSGather> impl_,
        size_t buf_size_,
        size_t min_bytes_for_seek_)
    : ReadBufferFromFileBase(buf_size_, nullptr, 0)
    , reader(reader_)
    , priority(priority_)
    , impl(impl_)
    , prefetch_buffer(buf_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);
}


String AsynchronousReadIndirectBufferFromRemoteFS::getFileName() const
{
    return impl->getFileName();
}


size_t AsynchronousReadIndirectBufferFromRemoteFS::getNumBytesToRead()
{
    size_t num_bytes_to_read;

    /// Position is set only for MergeTree tables.
    if (read_until_position)
    {
        /// Everything is already read.
        if (file_offset_of_buffer_end == *read_until_position)
            return 0;

        if (file_offset_of_buffer_end > *read_until_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Read beyond last offset ({} > {})",
                            file_offset_of_buffer_end, *read_until_position);

        /// Read range [file_offset_of_buffer_end, read_until_position).
        num_bytes_to_read = *read_until_position - file_offset_of_buffer_end;
        num_bytes_to_read = std::min(num_bytes_to_read, internal_buffer.size());
    }
    else
    {
        num_bytes_to_read = internal_buffer.size();
    }

    return num_bytes_to_read;
}


std::future<IAsynchronousReader::Result> AsynchronousReadIndirectBufferFromRemoteFS::readInto(char * data, size_t size)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<ThreadPoolRemoteFSReader::RemoteFSFileDescriptor>(impl);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = priority;

    if (bytes_to_ignore)
    {
        request.ignore = bytes_to_ignore;
        bytes_to_ignore = 0;
    }
    return reader->submit(request);
}


void AsynchronousReadIndirectBufferFromRemoteFS::prefetch()
{
    if (prefetch_future.valid())
        return;

    auto num_bytes_to_read = getNumBytesToRead();
    if (!num_bytes_to_read)
        return;

    /// Prefetch even in case hasPendingData() == true.
    prefetch_buffer.resize(num_bytes_to_read);
    prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
}


void AsynchronousReadIndirectBufferFromRemoteFS::setReadUntilPosition(size_t position)
{
    if (prefetch_future.valid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prefetch is valid in readUntilPosition");

    read_until_position = position;
    impl->setReadUntilPosition(position);
}


bool AsynchronousReadIndirectBufferFromRemoteFS::nextImpl()
{
    size_t size = 0;

    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
        Stopwatch watch;
        {
            size = prefetch_future.get();
            if (size)
            {
                memory.swap(prefetch_buffer);
                set(memory.data(), memory.size());
                working_buffer.resize(size);
                file_offset_of_buffer_end += size;
            }
        }

        watch.stop();
        ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
    }
    else
    {
        auto num_bytes_to_read = getNumBytesToRead();
        if (!num_bytes_to_read) /// Nothing to read.
            return false;

        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);
        size = readInto(memory.data(), num_bytes_to_read).get();

        if (size)
        {
            set(memory.data(), memory.size());
            working_buffer.resize(size);
            file_offset_of_buffer_end += size;
        }
    }

    prefetch_future = {};
    return size;
}


off_t AsynchronousReadIndirectBufferFromRemoteFS::seek(off_t offset_, int whence)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);

    if (whence == SEEK_CUR)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty() && static_cast<size_t>(getPosition() + offset_) < file_offset_of_buffer_end)
        {
            pos += offset_;
            return getPosition();
        }
        else
        {
            file_offset_of_buffer_end += offset_;
        }
    }
    else if (whence == SEEK_SET)
    {
        /// If position is within current working buffer - shift pos.
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= file_offset_of_buffer_end - working_buffer.size()
            && size_t(offset_) < file_offset_of_buffer_end)
        {
            pos = working_buffer.end() - (file_offset_of_buffer_end - offset_);

            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return getPosition();
        }
        else
        {
            file_offset_of_buffer_end = offset_;
        }
    }
    else
        throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSCancelledPrefetches);
        prefetch_future.wait();
        prefetch_future = {};
    }

    pos = working_buffer.end();

    /// Note: we read in range [file_offset_of_buffer_end, read_until_position).
    if (file_offset_of_buffer_end < read_until_position
        && static_cast<off_t>(file_offset_of_buffer_end) >= getPosition()
        && static_cast<off_t>(file_offset_of_buffer_end) < getPosition() + static_cast<off_t>(min_bytes_for_seek))
    {
       /**
        * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
        */
        bytes_to_ignore = file_offset_of_buffer_end - getPosition();
    }
    else
    {
        impl->reset();
    }

    return file_offset_of_buffer_end;
}


void AsynchronousReadIndirectBufferFromRemoteFS::finalize()
{
    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSUnusedPrefetches);
        prefetch_future.wait();
        prefetch_future = {};
    }
}


AsynchronousReadIndirectBufferFromRemoteFS::~AsynchronousReadIndirectBufferFromRemoteFS()
{
    finalize();
}

}
