#include <Processors/Sources/TimeoutPipeBuffers.h>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/UDFProcessSubtreeSampler.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>
#ifdef __sun
#include <sys/filio.h>  // illumos defines FIONREAD in sys/filio.h, not sys/ioctl.h
#endif

#include <ranges>
#include <span>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_POLL;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int TIMEOUT_EXCEEDED;
}

bool tryMakeFdNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;
    if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK))
        return false;

    return true;
}

void makeFdNonBlocking(int fd)
{
    bool result = tryMakeFdNonBlocking(fd);
    if (!result)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set non-blocking mode of pipe");
}

bool tryMakeFdBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;

    if (-1 == fcntl(fd, F_SETFL, flags & (~O_NONBLOCK)))
        return false;

    return true;
}

void makeFdBlocking(int fd)
{
    bool result = tryMakeFdBlocking(fd);
    if (!result)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set blocking mode of pipe");
}

int pollWithTimeout(pollfd * pfds, size_t num, size_t timeout_milliseconds)
{
    auto logger = getLogger("TimeoutReadBufferFromFileDescriptor");
    auto describe_fd = [](const auto & pollfd) { return fmt::format("(fd={}, flags={})", pollfd.fd, fcntl(pollfd.fd, F_GETFL)); };

    int res = 0;

    /// Account against one anchor in microseconds: the per-iteration millisecond stopwatch this
    /// replaces truncated a sub-millisecond interruption to 0, so a signal arriving faster than once
    /// per millisecond - the query profiler under load - left the budget untouched and the poll never
    /// expired. Same accounting as `ReadBufferFromFileDescriptor::poll` and `Epoll::getManyReady`.
    /// Clamp before scaling: `timeout_milliseconds` comes from the unrestricted `command_read_timeout` /
    /// `command_write_timeout` settings, so a huge value would wrap in the multiplication and could then
    /// round a non-zero remainder down to zero.
    const UInt64 timeout_microseconds
        = std::min<UInt64>(timeout_milliseconds, std::numeric_limits<UInt64>::max() / 1000) * 1000;
    UInt64 remaining_microseconds = timeout_microseconds;
    Stopwatch watch;

    while (true)
    {
        LOG_TEST(logger, "Polling descriptors: {}", fmt::join(std::span(pfds, pfds + num) | std::views::transform(describe_fd), ", "));

        res = poll(
            pfds,
            static_cast<nfds_t>(num),
            static_cast<int>(std::min<UInt64>(
                (remaining_microseconds + 999) / 1000, static_cast<UInt64>(std::numeric_limits<int>::max()))));

        if (res < 0)
        {
            if (errno != EINTR)
                throw ErrnoException(ErrorCodes::CANNOT_POLL, "Cannot poll");

            /// A zero timeout is a non-blocking readiness probe, so there is no deadline to exhaust:
            /// retry it rather than letting a signal report the descriptor as not ready.
            if (timeout_microseconds == 0)
                continue;

            const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();
            if (elapsed_microseconds >= timeout_microseconds)
            {
                LOG_TEST(logger, "Timeout exceeded: elapsed={}us, timeout={}us", elapsed_microseconds, timeout_microseconds);
                res = 0;
                break;
            }
            remaining_microseconds = timeout_microseconds - elapsed_microseconds;
        }
        else
        {
            break;
        }
    }

    LOG_TEST(
        logger,
        "Poll for descriptors: {} returned {}",
        fmt::join(std::span(pfds, pfds + num) | std::views::transform(describe_fd), ", "),
        res);

    return res;
}

String readWhatThePipeHolds(int fd, size_t max_size)
{
    String result;
    if (fd < 0)
        return result;

    int available = 0;
    if (0 != ::ioctl(fd, FIONREAD, &available))
        throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot query the pipe {} for buffered bytes", fd);

    result.resize(std::min(static_cast<size_t>(std::max(available, 0)), max_size));
    size_t done = 0;
    while (done < result.size())
    {
        const ssize_t res = ::read(fd, result.data() + done, result.size() - done);
        if (res > 0)
            done += static_cast<size_t>(res);
        else if (res == -1 && errno == EINTR)
            continue;
        else
            break;
    }
    result.resize(done);
    return result;
}

bool pollFd(int fd, size_t timeout_milliseconds, int events)
{
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = static_cast<int16_t>(events);
    pfd.revents = 0;

    return pollWithTimeout(&pfd, 1, timeout_milliseconds) > 0;
}

TimeoutReadBufferFromFileDescriptor::TimeoutReadBufferFromFileDescriptor(
    int stdout_fd_,
    int stderr_fd_,
    size_t timeout_milliseconds_,
    ExternalCommandStderrReaction stderr_reaction_,
    UDFProcessSubtreeSampler * sampler_,
    size_t buffer_size)
    : BufferWithOwnMemory<ReadBuffer>(buffer_size)
    , stdout_fd(stdout_fd_)
    , stderr_fd(stderr_fd_)
    , timeout_milliseconds(timeout_milliseconds_)
    , stderr_reaction(stderr_reaction_)
    , sampler(sampler_)
    /// Allocated up front rather than on first use: the first use may be from a `noexcept`
    /// probe on a cleanup path (`controlChannelIsClean`, `pipeWorkerIsAtACleanBoundary`),
    /// where an allocation refused by the memory limit would terminate the server.
    , stderr_read_buf(new char[BUFFER_SIZE])
{
    makeFdNonBlocking(stdout_fd);
    makeFdNonBlocking(stderr_fd);

    pfds[0].fd = stdout_fd;
    pfds[0].events = POLLIN;
    pfds[1].fd = stderr_fd;
    pfds[1].events = POLLIN;

    /// Both descriptors are polled even under `ExternalCommandStderrReaction::NONE`. "None"
    /// says what to do with the command's stderr - nothing - not that the pipe may be left
    /// unread: a pipe nobody reads fills up, and the command then blocks in `write` with its
    /// answer unfinished. That is a hang for a single-shot command and worse for a pooled one,
    /// where the bytes accumulate across borrows until some later query is the one that stalls.
    /// So the bytes are read and thrown away here (`nextImpl` matches no reaction for `NONE`),
    /// which is what "ignore this output" has to mean for a pipe.
}

bool TimeoutReadBufferFromFileDescriptor::nextImpl()
{
    if (stdout_is_done)
        return false;

    size_t bytes_read = 0;

    /// One budget for the whole call rather than one per wake-up. `command_read_timeout` says
    /// how long this read may wait for the command to say something on its stdout, and this
    /// loop is woken by things that are not that: stderr the command drips out, a stderr pipe
    /// that has hung up, an interrupted read. Restarting the timeout at each of them would let
    /// a command that never answers hold the query for as long as it keeps making noise on the
    /// other pipe - which is the same unbounded wait, only reached the long way round.
    ///
    /// And not even one per call while a frame deadline is armed (`armFrameDeadline`): each
    /// call returns as soon as anything arrives, so a command that drips its answer out a
    /// byte at a time would otherwise get a fresh budget for every byte.
    const UInt64 deadline_ns = frame_deadline_ns ? frame_deadline_ns : readDeadlineNs();

    while (!bytes_read)
    {
        pfds[0].revents = 0;
        pfds[1].revents = 0;
        int num_events = pollWithTimeout(pfds, num_pfds, remainingMs(deadline_ns));
        if (num_events <= 0)
            throwReadTimeout();

        bool has_stdout = pfds[0].revents > 0;
        bool has_stderr = pfds[1].revents > 0;

        if (has_stderr)
            readStderrOnce();

        if (has_stdout)
        {
            ssize_t res = ::read(stdout_fd, internal_buffer.begin(), internal_buffer.size());

            if (-1 == res && errno != EINTR)
                throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from pipe");

            if (res == 0)
            {
                /// Late diagnostics are handled by `waitDrainingOutput` with the command's
                /// termination budget. Repeated EOF probes must not wait for future stderr.
                stdout_is_done = true;
                break;
            }

            if (res > 0)
            {
                bytes_read += res;
                if (sampler)
                {
                    sampler->recordOutputBytes(static_cast<size_t>(res));
                    /// The child produced this output, so it was running; sample its subtree VmHWM.
                    /// It may have already exited (short-lived UDF) — then the read finds no VmHWM
                    /// and this is a harmless no-op. Also a no-op on the pool path (executable_root_pid <= 0).
                    sampler->sampleExecutablePeak();
                }
            }
        }

        /// Checked after the wake-up, not before it, so that a `command_read_timeout` of zero
        /// keeps meaning "probe once" rather than "fail without looking".
        ///
        /// It has to be checked at all because an exhausted budget turns the poll above into a
        /// readiness probe rather than a wait, and a probe is satisfied by anything pending -
        /// including stderr the command is flooding out while never answering on stdout. Left
        /// to the poll alone, that loop would spin at full speed for as long as the command
        /// kept writing, and `command_read_timeout` would never be reached.
        if (!bytes_read && remainingMs(deadline_ns) == 0)
            throwReadTimeout();
    }

    if (bytes_read > 0)
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
    }
    else
    {
        /// Concluding best-effort tail sample. The function has closed stdout, so
        /// this is the last point it is typically still alive; take one final
        /// subtree sample (bypassing the throttle) to catch a peak reached after
        /// the last IO sample but before EOF. Fired once; a no-op on the pool path
        /// and harmless if the child has already exited. This is a single tail
        /// attempt, not continuous sampling during the post-output reap.
        /// This concluding sample is best-effort and is intentionally NOT covered
        /// by a deterministic test — whether the child is still resident when EOF
        /// is detected is timing-dependent, so any assertion on it would be
        /// flaky; the deterministic guarantees (output-phase capture, max-not-sum,
        /// parent-independence) are covered by the integration tests.
        if (sampler && !final_sample_taken)
        {
            final_sample_taken = true;
            sampler->sampleExecutablePeak(/*is_final=*/true);
        }
        return false;
    }

    return true;
}

TimeoutReadBufferFromFileDescriptor::~TimeoutReadBufferFromFileDescriptor()
{
    /// Do not touch stdout_fd/stderr_fd here: they are owned by the ShellCommand, which may
    /// already have closed them (`ShellCommand::wait` closes the streams), and the numbers may
    /// be recycled by another thread. An fcntl on them would corrupt an unrelated descriptor.

    // Handle LOG_FIRST and LOG_LAST cases with circular buffer
    if (!stderr_result_buf.empty())
    {
        String stderr_result;
        stderr_result.reserve(stderr_result_buf.size());
        stderr_result.append(stderr_result_buf.begin(), stderr_result_buf.end());

        if (stderr_reaction == ExternalCommandStderrReaction::LOG_FIRST || stderr_reaction == ExternalCommandStderrReaction::LOG_LAST)
        {
            LOG_WARNING(
                getLogger("ShellCommandSource"),
                "Executable generates stderr at the {}: {}",
                stderr_reaction == ExternalCommandStderrReaction::LOG_FIRST ? "beginning" : "end",
                stderr_result);
        }
    }
}

String TimeoutReadBufferFromFileDescriptor::consumeBufferedStderr()
{
    if (stderr_result_buf.empty())
        return {};
    String result;
    result.reserve(stderr_result_buf.size());
    result.append(stderr_result_buf.begin(), stderr_result_buf.end());
    stderr_result_buf.clear();
    return result;
}

TimeoutReadBufferFromFileDescriptor::ChannelState
TimeoutReadBufferFromFileDescriptor::channelState(bool consider_buffered_output) const noexcept
{
    const Int16 stdout_events = ShellCommand::pendingEvents(stdout_fd);

    ChannelState state;
    state.stdout_has_unread_output
        = (consider_buffered_output && available() > 0) || (stdout_events & POLLIN) != 0;
    state.stdout_hung_up = (stdout_events & (POLLHUP | POLLERR | POLLNVAL)) != 0;

    /// A hangup on stderr, unlike on stdout, is not asked about: a command may close its own
    /// stderr, and it stays hung up for the rest of its life, so reading that as leftover output
    /// would discard a healthy worker on every borrow and turn the pool into a process per call.
    /// A child that has actually exited hangs up stdout too, which is where that is caught.
    ///
    /// Asked only under `ExternalCommandStderrReaction::THROW`, the one reaction under which
    /// stderr that reaches the wrong query is a wrong verdict rather than a log line in the
    /// wrong place. Under every other one the next borrow takes what is left off the pipe
    /// without the reaction and logs it against the worker (`quarantineReusedWorker`,
    /// `discardStderrLeftByAPreviousBorrow`), and a command that keeps logging is not worth
    /// a process per call.
    if (stderr_reaction == ExternalCommandStderrReaction::THROW)
        state.stderr_has_unread_output = (ShellCommand::pendingEvents(stderr_fd) & (POLLIN | POLLERR | POLLNVAL)) != 0;

    return state;
}

String TimeoutReadBufferFromFileDescriptor::consumePendingStderr()
{
    String result = readPendingStderr();

    if (stderr_reaction == ExternalCommandStderrReaction::THROW)
        accumulateStderrForThrow(result);

    return result;
}

void TimeoutReadBufferFromFileDescriptor::drainStderrFully(size_t budget_milliseconds, bool with_reaction, size_t bytes_already_read)
{
    const UInt64 deadline_ns = monotonicDeadlineNs(budget_milliseconds);

    /// An empty pipe is answered at once: nothing pending, nothing to wait for. But a pipe
    /// that was full may belong to a command in the middle of a burst - blocked in `write`,
    /// and only now, with room made, being woken to write the rest. That wake-up takes a
    /// moment, longer on a loaded machine, and a poll with no timeout would find the pipe
    /// momentarily empty and declare the burst over, leaving its tail to land in the middle of
    /// the next request. So once enough has been read that the writer may have been blocked,
    /// each further poll waits a little for it, up to the budget.
    ///
    /// Only then: a command that wrote a log line and went back to waiting for its next
    /// request is not blocked on anything, and waiting for it would put an idle window on
    /// every pooled call that logs, which is the hot path of `executable_pool`. A writer can
    /// be blocked only on a pipe without room for its write - less than `PIPE_BUF` free - so
    /// that is what is looked for before every read, until it is seen once. The bytes consumed
    /// for the borrow's diagnostic count as well: the pipe held them too.
    static constexpr size_t wait_for_writer_ms = 20;
    bool writer_may_be_blocked = false;
    size_t read_before = bytes_already_read;

    while (!stderr_is_done)
    {
        const size_t remaining_ms = remainingMs(deadline_ns);
        if (remaining_ms == 0)
            return;

        pfds[1].revents = 0;
        const size_t wait_ms = writer_may_be_blocked ? std::min(remaining_ms, wait_for_writer_ms) : 0;
        if (pollWithTimeout(&pfds[1], 1, wait_ms) <= 0 || pfds[1].revents == 0)
            return;

        if (!writer_may_be_blocked)
            writer_may_be_blocked = stderrPipeIsNearlyFull(std::exchange(read_before, 0));
        readStderrOnce(with_reaction);
    }
}

bool TimeoutReadBufferFromFileDescriptor::stderrPipeIsNearlyFull(size_t bytes_already_read) const
{
#if defined(OS_LINUX)
    int queued = 0;
    const int capacity = ::fcntl(stderr_fd, F_GETPIPE_SZ);
    if (capacity <= 0 || 0 != ::ioctl(stderr_fd, FIONREAD, &queued) || queued < 0)
        return true;
    return static_cast<size_t>(queued) + bytes_already_read + PIPE_BUF > static_cast<size_t>(capacity);
#else
    UNUSED(bytes_already_read);
    return true;
#endif
}

void TimeoutReadBufferFromFileDescriptor::clearStderrOfAnEarlierBorrow(const char * log_name)
{
    /// The usual case, a quiet pipe, answered with one `poll`.
    if (!stderrHasPendingOutput())
        return;

    static constexpr size_t stderr_drain_budget_ms = 100;
    const String leftover = consumePendingStderrWithoutReaction();
    drainStderrFully(stderr_drain_budget_ms, /*with_reaction=*/ false, leftover.size());
    if (!leftover.empty())
        LOG_WARNING(
            getLogger(log_name),
            "The process of a pooled command had unread output on its stderr when it was borrowed, so it was "
            "written after the response of an earlier invocation. It is reported here rather than against "
            "this query, which did not cause it. Stderr: {}",
            leftover);
}

void TimeoutReadBufferFromFileDescriptor::consumeStderrChunk(std::string_view str)
{
    switch (stderr_reaction)
    {
        case ExternalCommandStderrReaction::NONE:
            break;
        case ExternalCommandStderrReaction::THROW:
            accumulateStderrForThrow(str);
            break;
        case ExternalCommandStderrReaction::LOG:
            LOG_WARNING(getLogger("TimeoutReadBufferFromFileDescriptor"), "Executable generates stderr: {}", str);
            break;
        case ExternalCommandStderrReaction::LOG_FIRST:
        {
            const size_t to_insert = std::min(stderr_result_buf.reserve(), str.size());
            if (to_insert > 0)
                stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.begin() + to_insert);
            break;
        }
        case ExternalCommandStderrReaction::LOG_LAST:
            stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.end());
            break;
    }
}

void TimeoutReadBufferFromFileDescriptor::accumulateStderrForThrow(std::string_view str)
{
    /// Nothing read is not something written. `hasStderr` is "the command has produced output
    /// on its stderr", and under `throw` that is a verdict on the query - so a read that came
    /// back empty must leave it alone. The probes call this with whatever the pipe held, and
    /// for a worker discarded over its *stdout* the pipe usually holds nothing at all: a
    /// value emplaced here would fail a query whose rows were already correct, with an empty
    /// message to explain it.
    if (str.empty())
        return;

    const size_t current_size = stderr_full_output ? stderr_full_output->size() : 0;
    if (current_size >= MAX_STDERR_SIZE)
        return;

    if (!stderr_full_output)
        stderr_full_output.emplace();

    stderr_full_output->append(str.substr(0, MAX_STDERR_SIZE - current_size));
}

size_t TimeoutReadBufferFromFileDescriptor::readStderrOnce(bool with_reaction)
{
    const ssize_t res = ::read(stderr_fd, stderr_read_buf.get(), BUFFER_SIZE);
    if (res > 0)
    {
        if (with_reaction)
            consumeStderrChunk(std::string_view(stderr_read_buf.get(), static_cast<size_t>(res)));
        return static_cast<size_t>(res);
    }

    if (res < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
        return 0;

    stopPollingStderr();
    return 0;
}

void TimeoutReadBufferFromFileDescriptor::stopPollingStderr() noexcept
{
    stderr_is_done = true;
    /// `poll` ignores a negative descriptor and leaves its `revents` zero.
    pfds[1].fd = -1;
    pfds[1].revents = 0;
}

void TimeoutReadBufferFromFileDescriptor::throwReadTimeout() const
{
    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe read timeout exceeded {} milliseconds", timeout_milliseconds);
}

TimeoutWriteBufferFromFileDescriptor::TimeoutWriteBufferFromFileDescriptor(
    int fd_, size_t timeout_milliseconds_, UDFProcessSubtreeSampler * sampler_, size_t buffer_size)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size), fd(fd_), timeout_milliseconds(timeout_milliseconds_), sampler(sampler_)
{
    makeFdNonBlocking(fd);
}

void TimeoutWriteBufferFromFileDescriptor::nextImpl()
{
    if (!offset())
        return;

    size_t bytes_written = 0;

    while (bytes_written != offset())
    {
        if (!pollFd(fd, timeout_milliseconds, POLLOUT))
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe write timeout exceeded {} milliseconds", timeout_milliseconds);

        ssize_t res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);

        if ((-1 == res || 0 == res) && errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write into pipe");

        if (res > 0)
        {
            bytes_written += res;
            if (sampler)
            {
                sampler->recordInputBytes(static_cast<size_t>(res));
                /// The child's stdin is still open (this write succeeded), so it was
                /// running; sample its subtree VmHWM. It may exit before we sample — a
                /// harmless no-op. Also a no-op on the pool path (executable_root_pid <= 0).
                sampler->sampleExecutablePeak();
            }
        }
    }
}

void TimeoutWriteBufferFromFileDescriptor::reset() const
{
    makeFdBlocking(fd);
}

}
