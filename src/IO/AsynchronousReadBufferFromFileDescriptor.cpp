#include <ctime>
#include <optional>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/Throttler.h>
#include <Common/filesystemHelpers.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/getRandomASCIIString.h>
#include <IO/AsynchronousReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <base/getThreadId.h>


namespace ProfileEvents
{
    extern const Event AsynchronousReadWaitMicroseconds;
    extern const Event SynchronousReadWaitMicroseconds;
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


std::future<IAsynchronousReader::Result> AsynchronousReadBufferFromFileDescriptor::asyncReadInto(char * data, size_t size, Priority priority)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<IAsynchronousReader::LocalFileDescriptor>(fd);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = Priority{base_priority.value + priority.value};
    request.ignore = bytes_to_ignore;
    request.direct_io = direct_io;
    bytes_to_ignore = 0;

    return reader.submit(request);
}


void AsynchronousReadBufferFromFileDescriptor::prefetch(Priority priority)
{
    if (prefetch_future.valid())
        return;

    last_prefetch_info.submit_time = std::chrono::system_clock::now();
    last_prefetch_info.priority = priority;

    /// Will request the same amount of data that is read in nextImpl.
    prefetch_buffer.resize(internal_buffer.size());
    prefetch_future = asyncReadInto(prefetch_buffer.data(), prefetch_buffer.size(), priority);
}


void AsynchronousReadBufferFromFileDescriptor::appendToPrefetchLog(FilesystemPrefetchState state, int64_t size, const std::unique_ptr<Stopwatch> & execution_watch)
{
    FilesystemReadPrefetchesLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .path = getFileName(),
        .offset = file_offset_of_buffer_end,
        .size = size,
        .prefetch_submit_time = last_prefetch_info.submit_time,
        .execution_watch = execution_watch ? std::optional<Stopwatch>(*execution_watch) : std::nullopt,
        .priority = last_prefetch_info.priority,
        .state = state,
        .thread_id = getThreadId(),
        .reader_id = current_reader_id,
    };

    prefetches_log->add(std::move(elem));
}


bool AsynchronousReadBufferFromFileDescriptor::nextImpl()
{
    /// If internal_buffer size is empty, then read() cannot be distinguished from EOF
    assert(!internal_buffer.empty());

    IAsynchronousReader::Result result;
    if (prefetch_future.valid())
    {
        /// Read request already in flight. Wait for its completion.

        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AsynchronousReadWaitMicroseconds);

        result = prefetch_future.get();
        prefetch_future = {};
        if (result.size - result.offset > 0)
            prefetch_buffer.swap(memory);

        if (prefetches_log)
            appendToPrefetchLog(FilesystemPrefetchState::USED, result.size, result.execution_watch);
    }
    else
    {
        /// No pending request. Do synchronous read.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::SynchronousReadWaitMicroseconds);
        /// * If prefetch() was never called, internal_buffer may point to existing_memory, so we want
        ///   to read into it while leaving `memory` empty.
        /// * If prefetch() was called, `memory` is not empty, and internal_buffer may point to
        ///   prefetch_buffer (if it initially pointed to `memory`, then `memory` and prefetch_buffer
        ///   were swapped). In this case it's important that we use `memory` instead of
        ///   `internal_buffer` here. This ensures that we never point working_buffer into
        ///   prefetch_buffer as that would cause data race if prefetch() is called afterwards.
        char * buf = memory.size() == 0 ? internal_buffer.begin() : memory.data();
        chassert(memory.size() == 0 || memory.size() == internal_buffer.size());
        result = asyncReadInto(buf, internal_buffer.size(), DEFAULT_PREFETCH_PRIORITY).get();
    }

    chassert(!result.page_cache_cell);
    chassert(result.size >= result.offset);
    size_t bytes_read = result.size - result.offset;
    file_offset_of_buffer_end = result.file_offset_of_buffer_end;

    if (throttler)
        throttler->throttle(result.size);

    if (bytes_read)
    {
        /// Adjust the working buffer so that it ignores `offset` bytes.
        working_buffer = Buffer(result.buf + result.offset, result.buf + result.size);
        pos = working_buffer.begin();
    }

    return bytes_read;
}


void AsynchronousReadBufferFromFileDescriptor::finalize()
{
    if (prefetch_future.valid())
    {
        prefetch_future.wait();
        prefetch_future = {};
        last_prefetch_info = {};
    }
}


AsynchronousReadBufferFromFileDescriptor::AsynchronousReadBufferFromFileDescriptor(
    IAsynchronousReader & reader_,
    Priority priority_,
    int fd_,
    size_t buf_size,
    int flags,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_,
    ThrottlerPtr throttler_,
    FilesystemReadPrefetchesLogPtr prefetches_log_)
    : ReadBufferFromFileBase(buf_size, existing_memory, alignment, file_size_)
    , reader(reader_)
    , base_priority(priority_)
    , required_alignment(alignment)
    , fd(fd_)
    , throttler(throttler_)
    , query_id(CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() != nullptr ? CurrentThread::getQueryId() : "")
    , current_reader_id(getRandomASCIIString(8))
    , prefetches_log(prefetches_log_)
{
    if (required_alignment > buf_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Too large alignment. Cannot have required_alignment greater than buf_size: {} > {}. It is a bug",
            required_alignment,
            buf_size);

    prefetch_buffer.alignment = alignment;

    direct_io = (flags != -1) && (flags & O_DIRECT);
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
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "ReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence");
    }

    /// Position is unchanged.
    if (new_pos + (working_buffer.end() - pos) == file_offset_of_buffer_end)
        return new_pos;

    bool read_from_prefetch = false;
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
        if (prefetch_future.valid())
        {
            read_from_prefetch = true;

            /// Read from prefetch buffer and recheck if the new position is valid inside.
            if (nextImpl())
                continue;
        }

        /// Prefetch is cancelled because of seek.
        if (read_from_prefetch)
        {
            if (prefetches_log)
                appendToPrefetchLog(FilesystemPrefetchState::CANCELLED_WITH_SEEK, -1, nullptr);
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

    if (bytes_to_ignore >= internal_buffer.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Logical error in AsynchronousReadBufferFromFileDescriptor, bytes_to_ignore ({}"
                        ") >= internal_buffer.size() ({})", bytes_to_ignore, internal_buffer.size());

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

std::optional<size_t> AsynchronousReadBufferFromFileDescriptor::tryGetFileSize()
{
    return getSizeFromFileDescriptor(fd, getFileName());
}

}
