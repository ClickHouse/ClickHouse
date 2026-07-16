#include <unistd.h>
#include <cerrno>
#include <climits>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <algorithm>

#include <Common/Throttler.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


namespace ProfileEvents
{
    extern const Event WriteBufferFromFileDescriptorWrite;
    extern const Event WriteBufferFromFileDescriptorWriteFailed;
    extern const Event WriteBufferFromFileDescriptorWriteBytes;
    extern const Event DiskWriteElapsedMicroseconds;
    extern const Event FileSync;
    extern const Event FileSyncElapsedMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric Write;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_FSTAT;
}


void WriteBufferFromFileDescriptor::setCancellationHook(std::function<bool()> cancellation_hook_)
{
    cancellation_hook = std::move(cancellation_hook_);

    /// Decide whether the responsive write path is needed. Only a pipe/FIFO, a socket or a
    /// terminal can block in write() when the sink is slow or stuck, so while the hook is
    /// installed we wait for writability and write in a way that cannot sleep indefinitely for
    /// these to stay responsive. A regular file never blocks on write, and a non-tty character
    /// device such as /dev/null does not block either, so such descriptors keep using a single
    /// large write for throughput - otherwise a common pattern like
    /// `clickhouse-client --query ... > /dev/null` would regress to one poll and one small write
    /// per chunk. If fstat fails, assume the descriptor can block and use the safe (responsive)
    /// path.
    cancellation_fd_can_block = false;
    cancellation_fd_is_socket = false;
    if (cancellation_hook)
    {
        struct stat stat_buf{};
        if (0 != ::fstat(fd, &stat_buf))
            cancellation_fd_can_block = true;
        else
        {
            const bool is_tty = (0 != ::isatty(fd));
            cancellation_fd_is_socket = S_ISSOCK(stat_buf.st_mode);
            cancellation_fd_can_block = S_ISFIFO(stat_buf.st_mode) || cancellation_fd_is_socket || is_tty;

            /// For a terminal, poll() + a blocking write capped at PIPE_BUF is not enough to stay
            /// responsive: POLLOUT only promises *some* room, and a blocking tty write() sleeps
            /// until the whole chunk is accepted, so it can hang on a terminal that stops draining
            /// (the headline case of #22426). A non-blocking write is needed, but O_NONBLOCK must
            /// not be set on `fd` itself: the flag lives on the open file description, which a
            /// terminal fd shares with fd 2 and the parent shell, so it would leak to unrelated
            /// writers (see 3f8b12c2736). Instead, re-open the terminal by its path to get a private
            /// open file description with O_NONBLOCK - unlike dup() (and the BSD/macOS /dev/fd
            /// equivalent, which behaves like dup() and would reintroduce the leak), a plain
            /// path-based open() of a device file always creates an independent OFD. On Linux,
            /// /proc/self/fd/<fd> resolves to that path without needing readlink(); on Darwin,
            /// fcntl(F_GETPATH) recovers it explicitly; elsewhere (e.g. FreeBSD, which has neither)
            /// ttyname_r() recovers it portably - the re-open is attempted only for a terminal, so
            /// it is always applicable. A failed re-open falls back to the bounded blocking write
            /// below. The descriptor is kept for the lifetime of this buffer, so repeated hook
            /// installations (one per query in the client) reuse it.
#if defined(OS_LINUX)
            if (is_tty && nonblocking_write_fd < 0)
            {
                const std::string proc_fd_path = "/proc/self/fd/" + toString(fd);
                nonblocking_write_fd = ::open(proc_fd_path.c_str(), O_WRONLY | O_CLOEXEC | O_NOCTTY | O_NONBLOCK);
            }
#elif defined(OS_DARWIN)
            if (is_tty && nonblocking_write_fd < 0)
            {
                char tty_path[PATH_MAX];
                if (0 == ::fcntl(fd, F_GETPATH, tty_path))
                    nonblocking_write_fd = ::open(tty_path, O_WRONLY | O_CLOEXEC | O_NOCTTY | O_NONBLOCK);
            }
#else
            if (is_tty && nonblocking_write_fd < 0)
            {
                char tty_path[PATH_MAX];
                if (0 == ::ttyname_r(fd, tty_path, sizeof(tty_path)))
                    nonblocking_write_fd = ::open(tty_path, O_WRONLY | O_CLOEXEC | O_NOCTTY | O_NONBLOCK);
            }
#endif
        }
    }
}


void WriteBufferFromFileDescriptor::nextImpl()
{
    if (!offset())
        return;

    /// The operation was cancelled (e.g. the user pressed Ctrl+C in the client) - discard the
    /// buffered data instead of writing it, so the output stops promptly.
    if (cancellation_hook && cancellation_hook())
        return;

    Stopwatch watch;

    /// When a cancellation hook is installed (e.g. the client output during a query) and the
    /// descriptor can block (a pipe, socket or terminal), keep the write responsive to
    /// cancellation. Otherwise a Ctrl+C would only set the cancellation flag while we stay stuck
    /// in the write(), because the interrupting signal can be delivered to another thread and thus
    /// not interrupt this write() at all. Wait for the descriptor to become writable in small
    /// steps, checking for cancellation in between, issue writes that cannot sleep indefinitely,
    /// and discard the rest of the buffer once cancellation is requested. A terminal is written
    /// through a private non-blocking descriptor of the same sink (see setCancellationHook), so
    /// the write fails with EAGAIN instead of sleeping when the terminal stops draining.
    const bool responsive_writes = cancellation_hook && cancellation_fd_can_block;
    const int write_fd = (responsive_writes && nonblocking_write_fd >= 0) ? nonblocking_write_fd : fd;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        size_t bytes_to_write = offset() - bytes_written;

        if (responsive_writes)
        {
            if (cancellation_hook())
                return;

            pollfd poll_fd{.fd = write_fd, .events = POLLOUT, .revents = 0};
            int poll_res = ::poll(&poll_fd, 1, 100);

            if (poll_res < 0 && errno != EINTR)
            {
                String poll_error_file_name = file_name.empty() ? "(fd = " + toString(fd) + ")" : file_name;
                ErrnoException::throwFromPath(
                    ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, poll_error_file_name, "Cannot write to file {}", poll_error_file_name);
            }

            /// Timed out or interrupted by a signal - the descriptor is not writable yet.
            if (poll_res <= 0)
                continue;

            /// After poll() reports the descriptor is writable, writing at most PIPE_BUF bytes is
            /// guaranteed not to block on a pipe, so a blocking write bounded this way stays
            /// responsive for pipes/FIFOs. No chunk size gives that guarantee for a terminal or a
            /// socket (poll() only promises that *some* space is available, and their blocking
            /// write sleeps until the whole chunk is accepted), which is why those do not rely on
            /// it: a terminal goes through the private non-blocking descriptor and a socket is
            /// written with MSG_DONTWAIT - both fail with EAGAIN instead of sleeping when the sink
            /// has no room, handled below like a poll() timeout. The cap is kept as a fallback for
            /// a terminal without a private non-blocking descriptor: not a full guarantee there,
            /// but it bounds how much a single write() can wait for.
            if (write_fd == fd && !cancellation_fd_is_socket)
                bytes_to_write = std::min(bytes_to_write, static_cast<size_t>(PIPE_BUF));
        }

        ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            if (responsive_writes && cancellation_fd_is_socket)
                res = ::send(fd, working_buffer.begin() + bytes_written, bytes_to_write, MSG_DONTWAIT);
            else
                res = ::write(write_fd, working_buffer.begin() + bytes_written, bytes_to_write);
        }

        /// In the responsive path the write cannot sleep (a private non-blocking descriptor for a
        /// terminal, MSG_DONTWAIT for a socket), so a sink without room fails with EAGAIN instead.
        /// Treat it like a poll() timeout: wait for writability again, checking the cancellation
        /// hook in between.
        if (responsive_writes && -1 == res && (errno == EAGAIN || errno == EWOULDBLOCK))
            continue;

        /// A write()/send() returning 0 for the non-empty request here is always an error - unlike
        /// -1, it is not how an interruption is reported, so it does not need an errno check (which
        /// would read errno without it having been set by the call, since these calls only set it
        /// on the -1 return path).
        if ((-1 == res && errno != EINTR) || 0 == res)
        {
            ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteFailed);

            /// Don't use getFileName() here because this method can be called from destructor
            String error_file_name = file_name;
            if (error_file_name.empty())
                error_file_name = "(fd = " + toString(fd) + ")";
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, error_file_name, "Cannot write to file {}", error_file_name);
        }

        /// The write was interrupted by a signal. If meanwhile the operation was cancelled,
        /// stop writing and discard the rest of the buffer instead of restarting the write.
        if (-1 == res && errno == EINTR && cancellation_hook && cancellation_hook())
            return;

        if (res > 0)
        {
            bytes_written += res;
            if (throttler)
                throttler->throttle(res);
        }
    }

    ProfileEvents::increment(ProfileEvents::DiskWriteElapsedMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);

    /// Increase buffer size for next data if adaptive buffer size is used and nextImpl was called because of end of buffer.
    if (!available() && use_adaptive_buffer_size && memory.size() < adaptive_max_buffer_size)
    {
        memory.resize(std::min(memory.size() * 2, adaptive_max_buffer_size));
        BufferBase::set(memory.data(), memory.size(), 0);
    }
}

/// NOTE: This class can be used as a very low-level building block, for example
/// in trace collector. In such places allocations of memory can be dangerous,
/// so don't allocate anything in this constructor.
WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor(
    int fd_,
    size_t buf_size,
    char * existing_memory,
    ThrottlerPtr throttler_,
    size_t alignment,
    std::string file_name_,
    bool use_adaptive_buffer_size_,
    size_t adaptive_buffer_initial_size)
    /// The adaptive buffer grows from the initial size up to buf_size (the max), so the
    /// initial allocation must not exceed it. An out-of-range initial size would otherwise
    /// be passed straight to the allocator (e.g. a fuzzed adaptive_write_buffer_initial_size).
    : WriteBufferFromFileBase(use_adaptive_buffer_size_ ? std::min(adaptive_buffer_initial_size, buf_size) : buf_size, existing_memory, alignment)
    , fd(fd_)
    , throttler(throttler_)
    , file_name(std::move(file_name_))
    , use_adaptive_buffer_size(use_adaptive_buffer_size_)
    , adaptive_max_buffer_size(buf_size)
{
}

WriteBufferFromFileDescriptor::~WriteBufferFromFileDescriptor()
{
    if (nonblocking_write_fd >= 0)
    {
        [[maybe_unused]] int err = ::close(nonblocking_write_fd);
        chassert(!(err && errno == EBADF));
    }
}

void WriteBufferFromFileDescriptor::setFD(int fd_)
{
    /// The private non-blocking descriptor belongs to the previous sink - drop it.
    if (nonblocking_write_fd >= 0)
    {
        [[maybe_unused]] int err = ::close(nonblocking_write_fd);
        chassert(!(err && errno == EBADF));
        nonblocking_write_fd = -1;
    }
    fd = fd_;
}

void WriteBufferFromFileDescriptor::writeBestEffort(std::string_view data, UInt64 timeout_ms)
{
    /// Prefer the private non-blocking descriptor (present when the sink is a terminal and a
    /// cancellation hook has ever been installed) so this cannot sleep in write() at all.
    const int write_fd = nonblocking_write_fd >= 0 ? nonblocking_write_fd : fd;

    Stopwatch watch;
    size_t bytes_written = 0;
    while (bytes_written < data.size())
    {
        const UInt64 elapsed_ms = watch.elapsedMilliseconds();
        if (elapsed_ms >= timeout_ms)
            return;

        pollfd poll_fd{.fd = write_fd, .events = POLLOUT, .revents = 0};
        const int poll_res = ::poll(&poll_fd, 1, static_cast<int>(std::min(timeout_ms - elapsed_ms, UInt64(100))));
        if (poll_res < 0 && errno != EINTR)
            return;
        /// Timed out or interrupted by a signal - the descriptor is not writable yet.
        if (poll_res <= 0)
            continue;

        size_t bytes_to_write = data.size() - bytes_written;
        /// Without a non-blocking descriptor, cap the chunk at PIPE_BUF: after POLLOUT such a
        /// write cannot block on a pipe (and for other sinks it at least bounds the wait).
        if (write_fd == fd && !cancellation_fd_is_socket)
            bytes_to_write = std::min(bytes_to_write, static_cast<size_t>(PIPE_BUF));

        ssize_t res = 0;
        if (cancellation_fd_is_socket)
            res = ::send(fd, data.data() + bytes_written, bytes_to_write, MSG_DONTWAIT);
        else
            res = ::write(write_fd, data.data() + bytes_written, bytes_to_write);

        if (res > 0)
            bytes_written += res;
        else if (-1 == res && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
            continue;
        else
            /// Best effort: this is called on cancellation/teardown paths, so never throw here.
            return;
    }
}

void WriteBufferFromFileDescriptor::finalizeImpl()
{
    if (fd < 0)
    {
        chassert(!offset(), "attempt to write after close");
        return;
    }

    use_adaptive_buffer_size = false;
    WriteBufferFromFileBase::finalizeImpl();
}

void WriteBufferFromFileDescriptor::sync()
{
    /// If buffer has pending data - write it.
    next();

    ProfileEvents::increment(ProfileEvents::FileSync);

    Stopwatch watch;

    /// Request OS to sync data with storage medium.
#if defined(OS_DARWIN)
    int res = ::fsync(fd);
#else
    int res = ::fdatasync(fd);
#endif
    ProfileEvents::increment(ProfileEvents::FileSyncElapsedMicroseconds, watch.elapsedMicroseconds());

    if (-1 == res)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, getFileName(), "Cannot fsync {}", getFileName());
}


off_t WriteBufferFromFileDescriptor::seek(off_t offset, int whence) // NOLINT
{
    off_t res = lseek(fd, offset, whence);
    if (-1 == res)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, getFileName(), "Cannot seek through {}", getFileName());
    return res;
}

void WriteBufferFromFileDescriptor::truncate(off_t length) // NOLINT
{
    int res = ftruncate(fd, length);
    if (-1 == res)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_TRUNCATE_FILE, getFileName(), "Cannot truncate file {}", getFileName());
}


off_t WriteBufferFromFileDescriptor::size() const
{
    struct stat buf{};
    int res = fstat(fd, &buf);
    if (-1 == res)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSTAT, getFileName(), "Cannot execute fstat {}", getFileName());
    return buf.st_size;
}

std::string WriteBufferFromFileDescriptor::getFileName() const
{
    if (file_name.empty())
        return "(fd = " + toString(fd) + ")";

    return file_name;
}


}
