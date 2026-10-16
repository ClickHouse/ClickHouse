#include <fcntl.h>
#include <cerrno>
#include <ctime>
#include <algorithm>
#include <limits>
#include <optional>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/CurrentMetrics.h>
#include <Common/Throttler.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/PlatformFileIO.h>
#include <IO/WriteHelpers.h>
#include <IO/preadNoWait.h>
#include <Common/filesystemHelpers.h>
#if defined(OS_WINDOWS)
#include <io.h>
#include <Poco/UnWindows.h>
#else
#include <poll.h>
#endif
#include <sys/stat.h>
#include <Interpreters/Context.h>


#pragma clang diagnostic ignored "-Wreserved-identifier"

namespace ProfileEvents
{
    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;
    extern const Event ReadBufferFromFileDescriptorPageCacheHitBytes;
    extern const Event DiskReadElapsedMicroseconds;
    extern const Event Seek;
}

namespace CurrentMetrics
{
    extern const Metric Read;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_ADVISE;
}


#if defined(OS_WINDOWS)
namespace
{

/// `WaitForSingleObject` on a console input handle reports that the input buffer holds *some*
/// record - mouse movement, focus changes and window resizes among them - while `ReadFile` and
/// `ReadConsole` consume only key-down records that carry a character. Reporting readiness on
/// anything else would let the following read block past the timeout, so peek the queue and
/// answer only for the records a read would actually consume, discarding the rest (a read would
/// discard them too).
///
/// In line mode (`ENABLE_LINE_INPUT`, the console default) even a character is not enough:
/// `ReadFile` returns only once a whole line has been entered, so a partly typed line must not
/// count as readiness until its Enter is in the queue.
bool pollConsoleInput(HANDLE handle, int timeout_milliseconds, const std::string & file_name)
{
    DWORD console_mode = 0;
    const bool line_input = GetConsoleMode(handle, &console_mode) && (console_mode & ENABLE_LINE_INPUT);

    VectorWithMemoryTracking<INPUT_RECORD> records;
    Stopwatch watch;
    for (;;)
    {
        const auto elapsed = watch.elapsedMilliseconds();
        const auto remaining = static_cast<UInt64>(timeout_milliseconds) > elapsed
            ? static_cast<UInt64>(timeout_milliseconds) - elapsed
            : 0;

        const DWORD waited = WaitForSingleObject(handle, static_cast<DWORD>(remaining));
        if (waited == WAIT_FAILED)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throw Exception(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                "Cannot poll file {} (WaitForSingleObject), error code: {}",
                file_name,
                GetLastError());
        }
        if (waited != WAIT_OBJECT_0)
            return false;

        /// Peek the whole queue: in line mode the Enter that completes a line can be behind any
        /// number of characters.
        DWORD queued = 0;
        if (!GetNumberOfConsoleInputEvents(handle, &queued))
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throw Exception(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                "Cannot poll file {} (GetNumberOfConsoleInputEvents), error code: {}",
                file_name,
                GetLastError());
        }
        records.resize(std::max<DWORD>(queued, 1));

        DWORD count = 0;
        if (!PeekConsoleInputW(handle, records.data(), static_cast<DWORD>(records.size()), &count))
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throw Exception(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                "Cannot poll file {} (PeekConsoleInput), error code: {}",
                file_name,
                GetLastError());
        }

        auto is_character = [](const INPUT_RECORD & record)
        {
            return record.EventType == KEY_EVENT && record.Event.KeyEvent.bKeyDown && record.Event.KeyEvent.uChar.UnicodeChar != 0;
        };

        /// The records before the first character are invisible to a read.
        DWORD to_discard = 0;
        while (to_discard < count && !is_character(records[to_discard]))
            ++to_discard;

        bool partial_line = false;
        if (to_discard < count)
        {
            if (!line_input)
                return true;
            for (DWORD i = to_discard; i < count; ++i)
                if (is_character(records[i]) && records[i].Event.KeyEvent.uChar.UnicodeChar == L'\r')
                    return true;
            partial_line = true;
        }

        /// Consume the invisible records, or the handle stays signaled by them and this loop would
        /// spin until the timeout instead of waiting.
        if (to_discard != 0)
        {
            DWORD discarded = 0;
            if (!ReadConsoleInputW(handle, records.data(), to_discard, &discarded))
            {
                ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
                throw Exception(
                    ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                    "Cannot poll file {} (ReadConsoleInput), error code: {}",
                    file_name,
                    GetLastError());
            }
        }

        if (partial_line)
        {
            /// The characters of the line stay queued until a read takes the whole line, so the handle
            /// stays signaled and cannot tell us when the rest arrives: look at the queue again a bit
            /// later, until the timeout.
            if (remaining == 0)
                return false;
            Sleep(static_cast<DWORD>(std::min<UInt64>(remaining, 10)));
        }
    }
}

/// A pipe handle is not a readiness signal: it is signaled when an I/O operation on it completes,
/// not when unread bytes arrive, and anonymous pipes cannot be waited on for readability at all.
/// So probe the pipe with `PeekNamedPipe` until it has data or the timeout expires. Between probes
/// the thread waits a short, growing interval: this is the polling period, there is no event to wait for.
bool pollPipeInput(HANDLE handle, int timeout_milliseconds, const std::string & file_name)
{
    Stopwatch watch;
    DWORD interval_milliseconds = 1;
    for (;;)
    {
        DWORD available = 0;
        if (!PeekNamedPipe(handle, nullptr, 0, nullptr, &available, nullptr))
        {
            const DWORD error = GetLastError();
            /// The writer has closed its end: the next read returns EOF without blocking, as with `POLLHUP`.
            if (error == ERROR_BROKEN_PIPE)
                return true;

            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throw Exception(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                "Cannot poll file {} (PeekNamedPipe), error code: {}",
                file_name,
                error);
        }
        if (available > 0)
            return true;

        const auto elapsed = watch.elapsedMilliseconds();
        if (elapsed >= static_cast<UInt64>(timeout_milliseconds))
            return false;

        const auto remaining = static_cast<UInt64>(timeout_milliseconds) - elapsed;
        Sleep(static_cast<DWORD>(std::min<UInt64>(interval_milliseconds, remaining)));
        interval_milliseconds = std::min<DWORD>(interval_milliseconds * 2, 50);
    }
}

/// A console input handle, as opposed to any other character device (`NUL`, a serial port).
bool isConsoleInput(HANDLE handle)
{
    DWORD mode = 0;
    return GetFileType(handle) == FILE_TYPE_CHAR && GetConsoleMode(handle, &mode);
}

}
#endif


std::string ReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


size_t ReadBufferFromFileDescriptor::readImpl(char * to, size_t min_bytes, size_t max_bytes, size_t offset) const
{
    chassert(min_bytes <= max_bytes);

    /// This is a workaround of a read past EOF bug in linux kernel with pread()
    if (file_size.has_value() && offset >= *file_size)
         return 0;

    size_t bytes_read = 0;
    while (bytes_read < min_bytes)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);

        Stopwatch watch(profile_callback ? clock_type : CLOCK_MONOTONIC);

        ssize_t res = 0;
        bool from_os_page_cache = false;
        size_t to_read = max_bytes - bytes_read;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};

            if (use_pread)
            {
                if (detect_os_page_cache_reads.load(std::memory_order_relaxed))
                {
                    /// Fails with `EAGAIN` without waiting for the disk if the data is not in the page cache.
                    /// In this and any other failure, the regular `pread` below reads the data or reports the error.
                    res = preadNoWait(fd, to + bytes_read, to_read, offset + bytes_read);
                    from_os_page_cache = res > 0;
                    if (res == -1 && isPreadNoWaitUnavailable(errno))
                        detect_os_page_cache_reads.store(false, std::memory_order_relaxed);
                }
                if (!from_os_page_cache)
                    res = platformPRead(fd, to + bytes_read, to_read, offset + bytes_read);
            }
            else
                res = platformRead(fd, to + bytes_read, to_read);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, getFileName(), "Cannot read from file {}", getFileName());
        }

        if (res > 0)
        {
            bytes_read += res;
            if (throttler)
            {
                /// See the comment in `AsynchronousReadBufferFromFileDescriptor::nextImpl`.
                if (from_os_page_cache)
                {
                    ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorPageCacheHitBytes, res);
                    throttler->throttleOSPageCacheRead(res);
                }
                else
                    throttler->throttle(res);
            }
        }


        /// It reports real time spent including the time spent while thread was preempted doing nothing.
        /// And it is Ok for the purpose of this watch (it is used to lower the number of threads to read from tables).
        /// Sometimes it is better to use taskstats::blkio_delay_total, but it is quite expensive to get it.
        watch.stop();
        ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());

        if (profile_callback)
        {
            ProfileInfo info{};
            info.bytes_requested = to_read;
            info.bytes_read = res;
            info.nanoseconds = watch.elapsed();
            profile_callback(info);
        }
    }

    if (bytes_read)
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);

    return bytes_read;
}


void ReadBufferFromFileDescriptor::enableOSPageCacheReadsDetection(int flags)
{
    /// `RWF_NOWAIT` is ignored for `O_DIRECT` (see the comment in `ThreadPoolReader::submit`),
    /// so such a read could reach the device while looking like it was served from the page cache.
    /// The `O_DIRECT` check comes before `preadNoWaitUnavailableReason` for the same reason as there.
    /// The detection costs an extra system call for the data that is not in the page cache,
    /// which is why it is used only when there is a throttler that ignores such reads
    /// (e.g. not for merges, mutations or backups that are throttled only by their own bandwidth limits).
    bool enable = use_pread
        && throttler
        && throttler->ignoresOSPageCacheReads()
        && (flags == -1 || !(flags & O_DIRECT))
        && preadNoWaitUnavailableReason().empty();
    detect_os_page_cache_reads.store(enable, std::memory_order_relaxed);
}


bool ReadBufferFromFileDescriptor::nextImpl()
{
    /// If internal_buffer size is empty, then read() cannot be distinguished from EOF
    chassert(!internal_buffer.empty());

    size_t bytes_read = readImpl(internal_buffer.begin(), 1, internal_buffer.size(), file_offset_of_buffer_end);

    file_offset_of_buffer_end += bytes_read;

    if (bytes_read)
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
    }
    else
        return false;

    return true;
}


void ReadBufferFromFileDescriptor::prefetch(Priority)
{
#if defined(POSIX_FADV_WILLNEED)
    /// For direct IO, loading data into page cache is pointless.
    if (required_alignment)
        return;

    /// Ask OS to prefetch data into page cache.
    if (0 != posix_fadvise(fd, file_offset_of_buffer_end, internal_buffer.size(), POSIX_FADV_WILLNEED))
        throw ErrnoException(ErrorCodes::CANNOT_ADVISE, "Cannot posix_fadvise");
#endif
}

bool ReadBufferFromFileDescriptor::poll(size_t timeout_microseconds)
{
    if (hasPendingData())
        return true;

    auto timeout_milliseconds = static_cast<int>(
        std::min<size_t>((timeout_microseconds + 999) / 1000, static_cast<size_t>(std::numeric_limits<int>::max())));

#if defined(OS_WINDOWS)
    /// Windows has no `poll` for file descriptors - `WSAPoll` takes sockets only. The console is
    /// waitable with `WaitForSingleObject`, pipes are probed with `PeekNamedPipe`; a disk file
    /// is not waitable but is also always ready to read, which is what `poll` reports for one.
    auto * handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (handle == INVALID_HANDLE_VALUE)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
        throw Exception(
            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot poll file {}: not an open descriptor", getFileName());
    }

    if (GetFileType(handle) == FILE_TYPE_DISK)
        return true;

    if (isConsoleInput(handle))
        return pollConsoleInput(handle, timeout_milliseconds, getFileName());

    if (GetFileType(handle) == FILE_TYPE_PIPE)
        return pollPipeInput(handle, timeout_milliseconds, getFileName());

    const DWORD waited = WaitForSingleObject(handle, static_cast<DWORD>(timeout_milliseconds));
    if (waited == WAIT_FAILED)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
        throw Exception(
            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
            "Cannot poll file {} (WaitForSingleObject), error code: {}",
            getFileName(),
            GetLastError());
    }
    return waited == WAIT_OBJECT_0;
#else
    pollfd poll_fd{};
    poll_fd.fd = fd;
    poll_fd.events = POLLIN | POLLPRI;

    /// Retry EINTR with the remaining time, otherwise a periodic signal (e.g. the query profiler's)
    /// would reset the deadline on every retry and the poll could never expire. Same pattern as
    /// Epoll::getManyReady, with microsecond accounting so a sub-millisecond signal period still makes progress.
    Stopwatch watch;
    int result = 0;
    for (;;)
    {
        result = ::poll(&poll_fd, 1, timeout_milliseconds);
        if (result >= 0 || errno != EINTR)
            break;

        /// A zero timeout is a non-blocking readiness probe (used e.g. to check for a pending cancel):
        /// there is no deadline to exhaust, so just retry the probe on EINTR. Returning early here
        /// would let a signal hide an already-ready fd for that check. Only a positive timeout accrues
        /// against the deadline.
        if (timeout_microseconds == 0)
            continue;

        const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();
        if (elapsed_microseconds >= timeout_microseconds)
        {
            result = 0;
            break;
        }
        timeout_milliseconds = static_cast<int>(std::min<size_t>(
            (timeout_microseconds - elapsed_microseconds + 999) / 1000, static_cast<size_t>(std::numeric_limits<int>::max())));
    }

    if (result < 0)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
        ErrnoException::throwFromPath(
            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, getFileName(), "Cannot poll file {}", getFileName());
    }

    return result > 0;
#endif
}


/// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
off_t ReadBufferFromFileDescriptor::seek(off_t offset, int whence)
{
    size_t new_pos = 0;
    if (whence == SEEK_SET)
    {
        chassert(offset >= 0);
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

    /// NOLINTBEGIN(readability-else-after-return)
    if (file_offset_of_buffer_end - working_buffer.size() <= new_pos
        && new_pos <= file_offset_of_buffer_end)
    {
        /// Position is still inside the buffer.
        /// Probably it is at the end of the buffer - then we will load data on the following 'next' call.

        pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
        chassert(pos >= working_buffer.begin());
        chassert(pos <= working_buffer.end());

        return new_pos;
    }
    else
    {
        /// Position is out of the buffer, we need to do real seek.
        off_t seek_pos = required_alignment > 1
            ? new_pos / required_alignment * required_alignment
            : new_pos;

        off_t offset_after_seek_pos = new_pos - seek_pos;

        /// First reset the buffer so the next read will fetch new data to the buffer.
        resetWorkingBuffer();

        /// In case of using 'pread' we just update the info about the next position in file.
        /// In case of using 'read' we call 'lseek'.

        /// We account both cases as seek event as it leads to non-contiguous reads from file.
        ProfileEvents::increment(ProfileEvents::Seek);

        if (!use_pread)
        {
            Stopwatch watch(profile_callback ? clock_type : CLOCK_MONOTONIC);

            off_t res = ::lseek(fd, seek_pos, SEEK_SET);
            if (-1 == res)
                ErrnoException::throwFromPath(
                    ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    getFileName(),
                    "Cannot seek through file {} at offset {}",
                    getFileName(),
                    seek_pos);

            /// Also note that seeking past the file size is not allowed.
            if (res != seek_pos)
                throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    "The 'lseek' syscall returned value ({}) that is not expected ({})", res, seek_pos);

            watch.stop();
            ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());
        }

        file_offset_of_buffer_end = seek_pos;

        if (offset_after_seek_pos > 0)
            ignore(offset_after_seek_pos);

        /// Return the position we are actually at, not `seek_pos`. With O_DIRECT (`required_alignment > 1`)
        /// `seek_pos` is `new_pos` rounded down to the alignment, and the difference has just been skipped
        /// by `ignore` above, so the buffer is positioned at `new_pos`. Returning `seek_pos` would break
        /// callers that take the returned value as the new position (see `ReadBufferFromEncryptedFile`).
        return static_cast<off_t>(new_pos);
    }
    /// NOLINTEND(readability-else-after-return)
}


void ReadBufferFromFileDescriptor::rewind()
{
    if (!use_pread)
    {
        ProfileEvents::increment(ProfileEvents::Seek);
        off_t res = ::lseek(fd, 0, SEEK_SET);
        if (-1 == res)
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_SEEK_THROUGH_FILE, getFileName(), "Cannot seek through file {}", getFileName());
    }
    /// In case of pread, the ProfileEvents::Seek is not accounted, but it's Ok.

    /// Clearing the buffer with existing data. New data will be read on subsequent call to 'next'.
    working_buffer.resize(0);
    pos = working_buffer.begin();
    file_offset_of_buffer_end = 0;

    /// A previous read cycle may have failed, leaving the buffer in a canceled state.
    /// Reset so the next read cycle can proceed normally after rewind.
    canceled = false;
}

std::optional<size_t> ReadBufferFromFileDescriptor::tryGetFileSize()
{
    return getSizeFromFileDescriptor(fd, getFileName());
}

bool ReadBufferFromFileDescriptor::checkIfActuallySeekable()
{
    struct stat stat{};
    auto res = ::fstat(fd, &stat);
    return res == 0 && S_ISREG(stat.st_mode);
}

size_t ReadBufferFromFileDescriptor::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> &) const
{
    chassert(use_pread);
    return readImpl(to, n, n, offset);
}

}
