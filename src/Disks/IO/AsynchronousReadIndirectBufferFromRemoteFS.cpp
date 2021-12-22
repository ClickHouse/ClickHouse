#include "AsynchronousReadIndirectBufferFromRemoteFS.h"

#include <Common/Stopwatch.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <IO/ReadSettings.h>
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
    extern const Event RemoteFSLazySeeks;
    extern const Event RemoteFSSeeksWithReset;
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
        const ReadSettings & settings_,
        std::shared_ptr<ReadBufferFromRemoteFSGather> impl_,
        size_t min_bytes_for_seek_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0)
    , reader(reader_)
    , priority(settings_.priority)
    , impl(impl_)
    , prefetch_buffer(settings_.remote_fs_buffer_size)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , must_read_until_position(settings_.must_read_until_position)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);
}


String AsynchronousReadIndirectBufferFromRemoteFS::getFileName() const
{
    return impl->getFileName();
}


bool AsynchronousReadIndirectBufferFromRemoteFS::hasPendingDataToRead()
{
    /**
     * Note: read_until_position here can be std::nullopt only for non-MergeTree tables.
     * For mergeTree tables it must be guaranteed that setReadUntilPosition() or
     * setReadUntilEnd() is called before any read or prefetch.
     * setReadUntilEnd() always sets read_until_position to file size.
     * setReadUntilPosition(pos) always has pos > 0, because if
     * right_offset_in_compressed_file is 0, then setReadUntilEnd() is used.
     */
    if (read_until_position)
    {
        /// Everything is already read.
        if (file_offset_of_buffer_end == *read_until_position)
            return false;

        if (file_offset_of_buffer_end > *read_until_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Read beyond last offset ({} > {})",
                            file_offset_of_buffer_end, *read_until_position);
    }
    else if (must_read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Reading for MergeTree family tables must be done with last position boundary");

    return true;
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

    /// Check boundary, which was set in readUntilPosition().
    if (!hasPendingDataToRead())
        return;

    /// Prefetch even in case hasPendingData() == true.
    prefetch_future = readInto(prefetch_buffer.data(), prefetch_buffer.size());
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetches);
}


void AsynchronousReadIndirectBufferFromRemoteFS::setReadUntilPosition(size_t position)
{
    if (prefetch_future.valid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prefetch is valid in readUntilPosition");

    read_until_position = position;
    impl->setReadUntilPosition(*read_until_position);
}


void AsynchronousReadIndirectBufferFromRemoteFS::setReadUntilEnd()
{
    if (prefetch_future.valid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prefetch is valid in readUntilEnd");

    read_until_position = impl->getFileSize();
    impl->setReadUntilPosition(*read_until_position);
}


bool AsynchronousReadIndirectBufferFromRemoteFS::nextImpl()
{
    if (!hasPendingDataToRead())
        return false;

    size_t size = 0;

    if (prefetch_future.valid())
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
        Stopwatch watch;
        {
            auto result = prefetch_future.get();
            size = result.size;
            auto offset = result.offset;
            assert(offset < size);

            if (size)
            {
                memory.swap(prefetch_buffer);
                size -= offset;
                set(memory.data() + offset, size);
                working_buffer.resize(size);
                file_offset_of_buffer_end += size;
            }
        }

        watch.stop();
        ProfileEvents::increment(ProfileEvents::AsynchronousReadWaitMicroseconds, watch.elapsedMicroseconds());
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);
        auto result = readInto(memory.data(), memory.size()).get();
        size = result.size;
        auto offset = result.offset;
        assert(offset < size);

        if (size)
        {
            size -= offset;
            set(memory.data() + offset, size);
            working_buffer.resize(size);
            file_offset_of_buffer_end += size;
        }
    }

    if (file_offset_of_buffer_end != impl->offset())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected equality {} == {}. It's a bug", file_offset_of_buffer_end, impl->offset());

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

    /**
    * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
    * Note: we read in range [file_offset_of_buffer_end, read_until_position).
    */
    off_t file_offset_before_seek = impl->offset();
    if (impl->initialized()
        && read_until_position && file_offset_of_buffer_end < *read_until_position
        && static_cast<off_t>(file_offset_of_buffer_end) > file_offset_before_seek
        && static_cast<off_t>(file_offset_of_buffer_end) < file_offset_before_seek + static_cast<off_t>(min_bytes_for_seek))
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSLazySeeks);
        bytes_to_ignore = file_offset_of_buffer_end - file_offset_before_seek;
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSSeeksWithReset);
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
