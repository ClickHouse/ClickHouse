#include <Processors/Sources/ShellCommandSource.h>

#include <poll.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/UDFProcessSubtreeSampler.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ErrnoException.h>
#include <Common/scope_guard_safe.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>

#include <Common/SharedMemoryRegion.h>
#include <Common/DoubleBufferedProducer.h>
#include <Formats/formatBlock.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Core/Block.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Field.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>

#include <boost/circular_buffer.hpp>
#include <fmt/ranges.h>

#include <csignal>
#include <ranges>


namespace CurrentMetrics
{
    extern const Metric ExecutableUDFSharedMemoryPooledBytes;
}

namespace ProfileEvents
{
    extern const Event ExecutableUDFSharedMemoryCalls;
    extern const Event ExecutableUDFSharedMemoryInputBytes;
    extern const Event ExecutableUDFSharedMemoryOutputBytes;
    extern const Event ExecutableUDFSharedMemoryRegionGrowths;
    extern const Event ExecutableUDFSharedMemoryAllocatedBytes;
    extern const Event ExecutableUDFSharedMemoryDirtyChannelDiscards;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_POLL;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int UDF_EXECUTION_FAILED;
}

/// How much a shared-memory region is enlarged at once while the query's memory limit cannot be
/// enforced (see `ensureRegionFits`). Small enough that going over the limit stays bounded, large
/// enough that flushing a buffer's worth of trailing bytes does not remap the region per byte.
static constexpr size_t UNENFORCED_GROWTH_STEP = 1024 * 1024;

/// Version of the shared-memory control protocol between the server and an executable UDF.
/// Sent as the first varint of every request so the child can detect an incompatible protocol
/// version and answer with an error status.
static constexpr UInt64 SHARED_MEMORY_PROTOCOL_VERSION = 1;

/// The descriptor number under which the command's process inherits shared-memory region 0; region
/// `i` is at this plus `i`. Chosen above the three standard streams, and the shared-memory transport
/// admits no extra pipes (it takes exactly one input), so nothing else in the child is numbered here.
static constexpr int SHARED_MEMORY_FIRST_CHILD_FD = 3;

/// Identifies one request, and has to be unguessable rather than merely unique.
///
/// A counter would be enough to catch a command that writes junk: the junk lands where the id
/// belongs and does not match. It is not enough to catch a command that is wrong in a more
/// systematic way - one that, having answered request N, writes a whole plausible frame for N+1
/// before it is asked. That frame would match, and the next query would be handed an answer
/// computed for somebody else's input. Drawing the id at random takes that away: a command cannot
/// write the answer to a question it has not been asked yet.
static UInt64 generateRequestId()
{
    static thread_local pcg64_fast rng(randomSeed());
    return rng();
}

/// Status codes returned by the child in the first varint of every response.
static constexpr UInt64 SHARED_MEMORY_STATUS_OK = 0;

/// The child cannot fit its result into the region and asks for a bigger one. The status is
/// followed by a varint with the total region size (in bytes) the child needs. The server enlarges
/// the region (up to `shared_memory_max_size`) and re-sends the same request; enlarging preserves
/// the file contents, so the serialized input is still in place. Any other non-zero status is an
/// error followed by a message.
static constexpr UInt64 SHARED_MEMORY_STATUS_NEED_MORE_SPACE = 2;

/// Upper bound on the length of an error message read from the child on the failure path.
static constexpr size_t SHARED_MEMORY_MAX_ERROR_MESSAGE_SIZE = 64 * 1024;

static bool tryMakeFdNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;
    if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK))
        return false;

    return true;
}

static void makeFdNonBlocking(int fd)
{
    bool result = tryMakeFdNonBlocking(fd);
    if (!result)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set non-blocking mode of pipe");
}

static bool tryMakeFdBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;

    if (-1 == fcntl(fd, F_SETFL, flags & (~O_NONBLOCK)))
        return false;

    return true;
}

static void makeFdBlocking(int fd)
{
    bool result = tryMakeFdBlocking(fd);
    if (!result)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set blocking mode of pipe");
}

static int pollWithTimeout(pollfd * pfds, size_t num, size_t timeout_milliseconds)
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

static bool pollFd(int fd, size_t timeout_milliseconds, int events)
{
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = static_cast<int16_t>(events);
    pfd.revents = 0;

    return pollWithTimeout(&pfd, 1, timeout_milliseconds) > 0;
}

class TimeoutReadBufferFromFileDescriptor : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit TimeoutReadBufferFromFileDescriptor(
        int stdout_fd_,
        int stderr_fd_,
        size_t timeout_milliseconds_,
        ExternalCommandStderrReaction stderr_reaction_,
        UDFProcessSubtreeSampler * sampler_)
        : stdout_fd(stdout_fd_)
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

    bool nextImpl() override
    {
        size_t bytes_read = 0;

        /// One budget for the whole call rather than one per wake-up. `command_read_timeout` says
        /// how long this read may wait for the command to say something on its stdout, and this
        /// loop is woken by things that are not that: stderr the command drips out, a stderr pipe
        /// that has hung up, an interrupted read. Restarting the timeout at each of them would let
        /// a command that never answers hold the query for as long as it keeps making noise on the
        /// other pipe - which is the same unbounded wait, only reached the long way round.
        const UInt64 deadline_ns = readDeadlineNs();

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
                    /// EOF on stdout, so the command is done answering - but not necessarily done
                    /// writing. Take the rest of its stderr off the pipe before returning, whatever
                    /// the reaction is: a command that writes more than a pipeful after closing its
                    /// stdout is otherwise left blocked in `write` for good, and the wait that reaps
                    /// it never finishes. `NONE` drops what it reads, which is all "ignore this
                    /// output" can mean for a pipe.
                    drainRemainingStderr();
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

    ~TimeoutReadBufferFromFileDescriptor() override
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

    /// Check if stderr was accumulated (for THROW mode)
    bool hasStderr() const { return stderr_full_output.has_value(); }

    /// Get accumulated stderr content (for THROW mode)
    const String & getStderr() const { return *stderr_full_output; }

    /// Get buffered stderr content from circular buffer (for LOG_FIRST/LOG_LAST modes)
    /// Clears the buffer to prevent duplicate logging in destructor
    String consumeBufferedStderr()
    {
        if (stderr_result_buf.empty())
            return {};
        String result;
        result.reserve(stderr_result_buf.size());
        result.append(stderr_result_buf.begin(), stderr_result_buf.end());
        stderr_result_buf.clear();
        return result;
    }

    /// What the command's pipes say about it right now. Three separate answers rather than one
    /// verdict, because they are three different things and the caller reports them differently:
    /// output left on a pipe is a protocol violation by the command, a hangup on stdout is a child
    /// that is simply gone, and neither should be described as the other.
    struct ChannelState
    {
        /// The command wrote past its response frame. `exchange` reads exactly that frame, so the
        /// leftover is read by whichever query borrows this worker next, as the status varint of a
        /// response to its own request.
        bool stdout_has_unread_output = false;

        /// The write end of stdout is gone: the child exited, or closed its stdout, which ends the
        /// protocol either way. Not a violation and not something to blame the command for - but
        /// still a worker that must not be handed on.
        bool stdout_hung_up = false;

        /// Same hazard as unread stdout, only quieter: `nextImpl` drains stderr on every read, so
        /// what is left here is picked up by the next query and reported as its output - and under
        /// `stderr_reaction = throw`, fails it.
        bool stderr_has_unread_output = false;

        bool isClean() const { return !stdout_has_unread_output && !stdout_hung_up && !stderr_has_unread_output; }
    };

    /// `consider_buffered_output` says whether bytes this buffer has read but not handed on count
    /// as unread output. They do for the shared-memory transport, where every byte of the response
    /// frame is accounted for and a leftover is a protocol violation. They do not for the pipe
    /// transport, where a format reader may legitimately hold buffered bytes it did not parse, and
    /// only what is still in the kernel pipe is evidence that the command spoke out of turn.
    ChannelState channelState(bool consider_buffered_output = true) const noexcept
    {
        const Int16 stdout_events = pipePendingEvents(stdout_fd);

        ChannelState state;
        state.stdout_has_unread_output
            = (consider_buffered_output && available() > 0) || (stdout_events & POLLIN) != 0;
        state.stdout_hung_up = (stdout_events & (POLLHUP | POLLERR | POLLNVAL)) != 0;

        /// A hangup on stderr, unlike on stdout, is not asked about: a command may close its own
        /// stderr, and it stays hung up for the rest of its life, so reading that as leftover output
        /// would discard a healthy worker on every borrow and turn the pool into a process per call.
        /// A child that has actually exited hangs up stdout too, which is where that is caught.
        ///
        /// Nothing is asked at all under `ExternalCommandStderrReaction::NONE`: those bytes are read
        /// and dropped on the floor, so there is no query for them to be misattributed to and no
        /// reason to spend a worker over them.
        if (stderr_reaction != ExternalCommandStderrReaction::NONE)
            state.stderr_has_unread_output = (pipePendingEvents(stderr_fd) & (POLLIN | POLLERR | POLLNVAL)) != 0;

        return state;
    }

    /// Reads whatever is sitting unread on stderr right now, so a caller that is about to throw the
    /// command away can report it. Best-effort and non-blocking - the descriptor is in non-blocking
    /// mode and nothing here waits for more - and capped, because the amount a broken command can
    /// have left there is not bounded by anything else.
    ///
    /// What it reads also goes through the configured reaction, which matters for
    /// `ExternalCommandStderrReaction::THROW`: output the command produced after its response is
    /// still output it produced, and `throw` promises the query fails for it. Reported only as a
    /// log line, it would leave the query succeeding against the contract the setting states. The
    /// other reactions are served by the discard report the caller writes from the returned string.
    ///
    /// Deliberately not `noexcept`: it builds a string, and an allocation on this teardown path can
    /// be refused by the memory tracker. That has to reach the caller's handler, which gives up on
    /// the diagnostic, rather than terminate the server over it.
    /// The same read, without putting what it finds through the reaction. For a caller that has to
    /// clear the pipe of somebody else's output - see `discardStderrLeftByAPreviousBorrow`.
    String consumePendingStderrWithoutReaction() const
    {
        return readPendingStderr();
    }

    String consumePendingStderr()
    {
        String result = readPendingStderr();

        if (stderr_reaction == ExternalCommandStderrReaction::THROW)
            accumulateStderrForThrow(result);

        return result;
    }

    String readPendingStderr() const
    {
        String result;

        char buffer[BUFFER_SIZE];
        while (result.size() < MAX_PENDING_STDERR_SIZE)
        {
            const size_t to_read = std::min(sizeof(buffer), MAX_PENDING_STDERR_SIZE - result.size());
            const ssize_t res = ::read(stderr_fd, buffer, to_read);
            if (res > 0)
                result.append(buffer, static_cast<size_t>(res));
            else if (res == -1 && errno == EINTR)
                continue;
            else
                break;
        }

        return result;
    }

    /// Whether anything is done with the command's stderr beyond taking it off the pipe. `NONE`
    /// drops what it reads, so a caller that would only read in order to drop has nothing to do.
    bool stderrIsObserved() const { return stderr_reaction != ExternalCommandStderrReaction::NONE; }

    /// Reads stderr until the pipe is empty or `budget_milliseconds` runs out.
    ///
    /// For the moment a pooled worker is handed on under `ExternalCommandStderrReaction::NONE`.
    /// Nothing is done with those bytes - that is what `none` means - but they cannot be left on
    /// the pipe either: they accumulate across borrows, and the command blocks in `write` once the
    /// pipe fills, so the borrow after that finds a worker that never reads its request. `none`
    /// promises a chatty command does not block, and this is where that promise is kept for a
    /// worker that goes back into the pool.
    ///
    /// With `with_reaction` false the bytes are dropped whatever the reaction is. That is for a
    /// borrow that clears the pipe of a *previous* borrow's output before sending its own request
    /// (`quarantineReusedWorker`, `discardStderrLeftByAPreviousBorrow`): those bytes are the
    /// earlier query's, and putting them through this query's `stderr_reaction` would fail this
    /// query, under `throw`, for a diagnostic it did not cause. The caller has already read and
    /// reported what it could of them (`consumePendingStderrWithoutReaction`, which is capped);
    /// this takes the rest, and whatever the command keeps writing during the drain, the same way.
    void drainStderrFully(size_t budget_milliseconds, bool with_reaction = true)
    {
        const UInt64 deadline_ns = clock_gettime_ns() + static_cast<UInt64>(budget_milliseconds) * 1000000ULL;

        while (!stderr_is_done && remainingMs(deadline_ns) != 0)
        {
            pfds[1].revents = 0;
            if (pollWithTimeout(&pfds[1], 1, 0) <= 0 || pfds[1].revents == 0)
                return;

            readStderrOnce(with_reaction);
        }
    }

    /// For a caller that reads the command's stderr itself and needs those bytes to go through the
    /// configured reaction all the same - the bounded wait that reaps a command drains both pipes,
    /// and that is the last stretch in which a command can still write.
    void consumeStderrBytes(std::string_view str) { consumeStderrChunk(str); }

private:
    /// One chunk of the command's stderr, put through the configured reaction. `NONE` matches
    /// nothing and the bytes are dropped - which is exactly what it is for: they still have to be
    /// taken off the pipe, or the command blocks in `write` once the pipe fills up.
    void consumeStderrChunk(std::string_view str)
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

    /// Accumulating stops at `MAX_STDERR_SIZE`, but reading never does: the point of reading is to
    /// keep the command from blocking, and that is true whether or not the bytes are still wanted.
    void accumulateStderrForThrow(std::string_view str)
    {
        const size_t current_size = stderr_full_output ? stderr_full_output->size() : 0;
        if (current_size >= MAX_STDERR_SIZE)
            return;

        if (!stderr_full_output)
            stderr_full_output.emplace();

        stderr_full_output->append(str.substr(0, MAX_STDERR_SIZE - current_size));
    }

    /// Reads what is pending on stderr once and puts it through the reaction. A pipe that is done -
    /// EOF, or an error that reading again will not fix - is dropped out of the poll set, because
    /// `poll` reports a hung-up descriptor immediately and forever: left in, it would turn the wait
    /// for the command's next answer into a busy loop that spins a core and never reaches
    /// `command_read_timeout`. A command closing its own stderr is ordinary (see
    /// `shm_udf_quiet_stderr.py`), so this is not an error path.
    void readStderrOnce(bool with_reaction = true)
    {
        const ssize_t res = ::read(stderr_fd, stderr_read_buf.get(), BUFFER_SIZE);
        if (res > 0)
        {
            if (with_reaction)
                consumeStderrChunk(std::string_view(stderr_read_buf.get(), static_cast<size_t>(res)));
            return;
        }

        if (res < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
            return;

        stopPollingStderr();
    }

    void stopPollingStderr() noexcept
    {
        stderr_is_done = true;
        /// `poll` ignores a negative descriptor and leaves its `revents` zero.
        pfds[1].fd = -1;
        pfds[1].revents = 0;
    }

    /// Takes the rest of the command's stderr off the pipe once its stdout has ended.
    ///
    /// Bounded twice over: it stops as soon as the pipe goes quiet for a moment, so an ordinary
    /// command is not held up, and it stops altogether after `command_read_timeout`, so a command
    /// that writes to stderr forever after closing its stdout cannot hold the query here for
    /// longer than one that never answers at all. Whatever is left after that belongs to the
    /// bounded wait that reaps the child.
    void drainRemainingStderr()
    {
        static constexpr size_t STDERR_DRAIN_POLL_MS = 100;

        /// A budget of its own rather than what is left of the read's: this runs after the command
        /// has closed its stdout, so the read it belongs to is over, and taking the remainder would
        /// mean that a read which used up its time leaves the command blocked in `write` - which is
        /// the one thing this is here to prevent.
        const UInt64 deadline_ns = readDeadlineNs();

        while (!stderr_is_done)
        {
            const size_t remaining_ms = remainingMs(deadline_ns);
            if (remaining_ms == 0)
                break;

            pfds[1].revents = 0;
            const int stderr_events = pollWithTimeout(&pfds[1], 1, std::min(STDERR_DRAIN_POLL_MS, remaining_ms));
            if (stderr_events <= 0 || pfds[1].revents == 0)
                break;

            readStderrOnce();
        }
    }

    [[noreturn]] void throwReadTimeout() const
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe read timeout exceeded {} milliseconds", timeout_milliseconds);
    }

    /// A monotonic deadline `timeout_milliseconds` from now. Clamped, because
    /// `command_read_timeout` is not bounded anywhere: a huge value would wrap the multiplication
    /// or the addition and put the deadline in the past, turning every wait into a probe.
    UInt64 readDeadlineNs() const noexcept
    {
        const UInt64 now_ns = clock_gettime_ns();
        const UInt64 max_ms = (std::numeric_limits<UInt64>::max() - now_ns) / 1000000ULL;
        return now_ns + std::min<UInt64>(timeout_milliseconds, max_ms) * 1000000ULL;
    }

    /// Milliseconds left until `deadline_ns`, rounded up so that the last fraction of a millisecond
    /// is still spent waiting instead of turning the poll into a non-blocking probe.
    static size_t remainingMs(UInt64 deadline_ns) noexcept
    {
        const UInt64 now_ns = clock_gettime_ns();
        if (now_ns >= deadline_ns)
            return 0;

        return static_cast<size_t>((deadline_ns - now_ns + 999999ULL) / 1000000ULL);
    }

    /// What is pending on `fd` right now, as `poll` revents. A zero timeout makes this a plain
    /// readiness probe, so it never waits - and it never throws either. Polled directly rather than
    /// through `pollFd`, which reports a failed `poll` by throwing and takes a logger to do it: the
    /// answer is wanted on the teardown path that hands a pooled worker back, where an exception
    /// would skip the hand-back entirely and lose the pool slot for the lifetime of the server. A
    /// probe that cannot be taken reports `POLLERR`, which every caller reads as a reason not to
    /// reuse the worker - the fail-closed side.
    ///
    /// The distinction the callers draw from this is between unread data (`POLLIN`) and a write end
    /// that is simply gone (`POLLHUP`). They are not the same thing and do not mean the same thing
    /// on the two pipes, so the raw events are returned rather than a verdict.
    static Int16 pipePendingEvents(int fd) noexcept
    {
        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLIN;

        int res = 0;
        do
        {
            pfd.revents = 0;
            res = ::poll(&pfd, 1, 0);
        }
        while (res < 0 && errno == EINTR);

        if (res < 0)
            return POLLERR;

        return static_cast<Int16>(pfd.revents);
    }

    int stdout_fd;
    int stderr_fd;
    size_t timeout_milliseconds;
    ExternalCommandStderrReaction stderr_reaction;
    UDFProcessSubtreeSampler * sampler;
    bool final_sample_taken = false;

    static constexpr size_t BUFFER_SIZE = 4_KiB;
    static constexpr size_t MAX_STDERR_SIZE = 1_MiB;  /// Safety limit for stderr accumulation
    /// A discarded worker's leftover stderr goes into a log line, so keep it to a readable size.
    static constexpr size_t MAX_PENDING_STDERR_SIZE = 4_KiB;
    pollfd pfds[2]{};
    static constexpr size_t num_pfds = 2;
    /// Set once stderr has reached EOF or failed for good; `pfds[1]` is then out of the poll set.
    bool stderr_is_done = false;
    std::unique_ptr<char[]> stderr_read_buf;
    boost::circular_buffer_space_optimized<char> stderr_result_buf{BUFFER_SIZE};
    std::optional<String> stderr_full_output;  /// For THROW mode: accumulate stderr up to MAX_STDERR_SIZE
};

class TimeoutWriteBufferFromFileDescriptor : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit TimeoutWriteBufferFromFileDescriptor(int fd_, size_t timeout_milliseconds_, UDFProcessSubtreeSampler * sampler_)
        : fd(fd_), timeout_milliseconds(timeout_milliseconds_), sampler(sampler_)
    {
        makeFdNonBlocking(fd);
    }

    void nextImpl() override
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

    /// Restore blocking mode before the command is returned to the process pool.
    /// Safe only while the fd is provably open (the send-data task calls this right
    /// before closing/returning); the destructor must not do it, see
    /// ~TimeoutReadBufferFromFileDescriptor.
    void reset() const
    {
        makeFdBlocking(fd);
    }

private:
    int fd;
    size_t timeout_milliseconds;
    UDFProcessSubtreeSampler * sampler;
};

/// Whether a pooled process is gone and left nothing behind on its stdout. `POLLHUP` without
/// `POLLIN` is a write end that is closed and a pipe that is empty; a live worker waiting for its
/// next request reports neither.
static bool pooledProcessHasExitedCleanly(const ShellCommand & process)
{
    pollfd pfd{};
    pfd.fd = process.out.getFD();
    pfd.events = POLLIN;

    int res = 0;
    do
    {
        pfd.revents = 0;
        res = ::poll(&pfd, 1, 0);
    }
    while (res < 0 && errno == EINTR);

    if (res <= 0)
        return false;

    return (pfd.revents & POLLHUP) != 0 && (pfd.revents & POLLIN) == 0;
}

class ShellCommandHolder
{
public:
    /// Builds the process. It is given the descriptors the child has to inherit - the shared-memory
    /// regions, as `{child_fd, parent_fd}` - which is why the regions have to exist before the
    /// process does: a `memfd` has no name a process could open later, so the only way for the
    /// command to reach it is to have been started with it.
    using ShellCommandBuilderFunc = std::function<std::unique_ptr<ShellCommand>(const std::vector<std::pair<int, int>> & inherited_fds)>;

    explicit ShellCommandHolder(ShellCommandBuilderFunc && func_)
        : func(std::move(func_))
    {}

    ~ShellCommandHolder()
    {
        shared_memory = {};

        if (persistent_memory_charge)
            unchargePersistentMemory(persistent_memory_charge);
    }

    /// Whether the next `buildCommand` hands back a process that has already served a borrow. A
    /// caller that has to distinguish "this worker may have left something on its pipes" from "this
    /// process was started a moment ago" needs to ask before building.
    bool hasReturnedCommand() const { return returned_command != nullptr; }

    /// Hands back the process that served the previous borrow, or starts a new one. A new one
    /// inherits the regions this holder owns at that moment, so they have to have been created
    /// already (see `ensureSharedMemory`); a returned one inherited them when it was started.
    std::unique_ptr<ShellCommand> buildCommand()
    {
        if (returned_command)
            return std::move(returned_command);

        return func(inheritedRegionFds());
    }

    /// The descriptors a process started now has to inherit: region `i` under child descriptor
    /// `SHARED_MEMORY_FIRST_CHILD_FD + i`, which is also the number its request names it by.
    std::vector<std::pair<int, int>> inheritedRegionFds() const
    {
        std::vector<std::pair<int, int>> fds;
        for (size_t i = 0; i < shared_memory.size(); ++i)
            if (shared_memory[i])
                fds.emplace_back(SHARED_MEMORY_FIRST_CHILD_FD + static_cast<int>(i), shared_memory[i]->fd());
        return fds;
    }

    void returnCommand(std::unique_ptr<ShellCommand> command)
    {
        returned_command = std::move(command);
    }

    /// The id for the next request to this process. It lives on the holder rather than on the
    /// borrower because what it is for is telling this request's response apart from anything the
    /// process wrote for an earlier one - and an earlier one can belong to an earlier borrow.
    UInt64 nextRequestId() const { return generateRequestId(); }

    /// Shared-memory region(s) for this process, created once and reused across pool borrows.
    /// `index` selects the buffer: index 0 is used by the plain (synchronous) transport; the
    /// pipelined transport additionally uses index 1 for double buffering.
    ///
    /// Creating and growing regions does not charge any memory tracker here: while the holder is
    /// borrowed, the borrowing query owns the charge (see `releaseChargeToBorrower`).
    SharedMemoryRegionPtr getOrCreateSharedMemory(size_t size, size_t index, bool & created)
    {
        if (!shared_memory[index])
        {
            shared_memory[index] = std::make_shared<SharedMemoryRegion>(size);
            created = true;
        }
        else
            created = false;

        return shared_memory[index];
    }

    /// What the region at `index` costs - its committed size - or zero if it has not been created
    /// yet. Lets the borrower charge its query memory tracker for the right number of bytes BEFORE
    /// the region is created or reused, because creating one commits its pages. The committed size
    /// rather than the mapped one: a growth that reserved its pages but could not map them leaves
    /// the file larger than the mapping, and those pages cost what any others do.
    size_t getSharedMemorySize(size_t index) const
    {
        return shared_memory[index] ? shared_memory[index]->backingSize() : 0;
    }

    void growSharedMemory(size_t index, size_t new_size)
    {
        shared_memory[index]->grow(new_size);
    }

    void resetSharedMemory(size_t index)
    {
        shared_memory[index].reset();
    }

    /// A region is charged to exactly one memory tracker at a time, chosen by who can observe it:
    /// while the holder is borrowed, the borrowing query's tracker owns the charge, so the memory
    /// limit of that query still covers the region; while the holder sits idle in the process pool
    /// the region stays mapped with no query to charge, so the global tracker owns it instead.
    /// The charge is handed over in both directions rather than taken twice, because a query
    /// charge already propagates up into `total_memory_tracker` — charging both would count the
    /// same bytes twice there and let a handful of pooled workers exhaust
    /// `max_server_memory_usage` on paper.
    ///
    /// Called by the borrower right after it takes the holder from the pool, before it creates,
    /// grows or charges anything.
    void releaseChargeToBorrower()
    {
        if (persistent_memory_charge)
            unchargePersistentMemory(persistent_memory_charge);
    }

    /// Called by the borrower once it has finished with the regions and is about to drop its own
    /// (query-level) charge, so that whatever survives the borrow is accounted again. Charges the
    /// regions the holder actually still owns, which may be fewer than at the start of the borrow
    /// (a discarded worker drops them) or larger (they may have grown).
    ///
    /// A region is sealed against shrinking and only the server grows it, so its `backingSize` is
    /// the truth about what the region holds - the command cannot change it. A pooled region keeps
    /// whatever size its largest chunk grew it to (or its largest growth committed, even one that
    /// then failed to map), for the life of the worker, and that is what the server is charged
    /// for while the worker sits idle.
    ///
    /// Never throws: this runs on a cleanup path, and it is an accounting hand-back rather than an
    /// allocation — the memory is already mapped, refusing the charge would not free anything.
    void acquireChargeFromBorrower() noexcept
    {
        size_t bytes = 0;
        for (const auto & region : shared_memory)
            if (region)
                bytes += region->backingSize();

        if (!bytes)
            return;

        LockMemoryExceptionInThread block_exceptions(VariableContext::Global);
        chargePersistentMemory(bytes);
    }

private:
    void chargePersistentMemory(size_t bytes)
    {
        MemoryTrackerBlockerInThread blocker(VariableContext::User);
        MemoryTrackerSwitcher switcher(&total_memory_tracker);
        [[maybe_unused]] auto trace = CurrentMemoryTracker::alloc(static_cast<Int64>(bytes));
        CurrentThread::flushUntrackedMemory();
        persistent_memory_charge += bytes;

        /// The same bytes, reported on their own. The server-wide tracker they were just added to
        /// carries everything else the server allocates as well, so it cannot answer how much of it
        /// is regions held by idle pooled workers - which is the part an administrator sizing
        /// `max_server_memory_usage` against a pool has to know, and the only part a test of this
        /// hand-over can assert exactly.
        CurrentMetrics::add(CurrentMetrics::ExecutableUDFSharedMemoryPooledBytes, static_cast<Int64>(bytes));
    }

    void unchargePersistentMemory(size_t bytes)
    {
        MemoryTrackerBlockerInThread blocker(VariableContext::User);
        MemoryTrackerSwitcher switcher(&total_memory_tracker);
        [[maybe_unused]] auto trace = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
        CurrentThread::flushUntrackedMemory();
        persistent_memory_charge -= bytes;

        CurrentMetrics::sub(CurrentMetrics::ExecutableUDFSharedMemoryPooledBytes, static_cast<Int64>(bytes));
    }

    std::unique_ptr<ShellCommand> returned_command;
    ShellCommandBuilderFunc func;
    std::array<SharedMemoryRegionPtr, 2> shared_memory;
    size_t persistent_memory_charge = 0;
};

namespace
{
    /** A stream, that get child process and sends data using tasks in background threads.
    * For each send data task background thread is created. Send data task must send data to process input pipes.
    * ShellCommandPoolSource receives data from process stdout.
    *
    * If process_pool is passed in constructor then after source is destroyed process is returned to pool.
    */
    class ShellCommandSource final : public ISource
    {
    public:

        using SendDataTask = std::function<void(void)>;

        ShellCommandSource(
            ContextPtr context_,
            const std::string & format_,
            size_t command_read_timeout_milliseconds,
            ExternalCommandStderrReaction stderr_reaction,
            bool check_exit_code_,
            SharedHeader sample_block_,
            std::unique_ptr<ShellCommand> && command_,
            std::vector<SendDataTask> && send_data_tasks = {},
            const ShellCommandSourceConfiguration & configuration_ = {},
            std::unique_ptr<ShellCommandHolder> && command_holder_ = nullptr,
            std::shared_ptr<ProcessPool> process_pool_ = nullptr,
            bool worker_is_reused_ = false)
            : ISource(std::make_shared<const Block>(sample_block_->cloneEmpty()))
            , context(context_)
            , format(format_)
            , sample_block(sample_block_)
            , configuration(configuration_)
            /// Reads the descriptors out of the command without taking it yet - see the declaration
            /// of `command` for why this object takes ownership as late as it can.
            , timeout_command_out(command_->out.getFD(), command_->err.getFD(), command_read_timeout_milliseconds, stderr_reaction, configuration_.sampler.get())
            , process_pool(process_pool_)
            , check_exit_code(check_exit_code_)
            , worker_is_reused(worker_is_reused_)
            , command(std::move(command_))
            , command_holder(std::move(command_holder_))
        {
            /// Everything the constructor does lives in this try: a borrowed process holder is
            /// already owned by this object (the caller's local was moved from in the member
            /// initializer list above), so an exception that escapes here would destroy it without
            /// handing it back, and `BorrowedObjectPool` never gives that slot out again. Copying
            /// the context and changing its settings can throw - MEMORY_LIMIT_EXCEEDED, say.
            try
            {
                auto context_for_reading = Context::createCopy(context);
                /// Currently parallel parsing input format cannot read exactly max_block_size rows from input,
                /// so it will be blocked on ReadBufferFromFileDescriptor because this file descriptor represent pipe that does not have eof.
                if (configuration.read_fixed_number_of_rows)
                    context_for_reading->setSetting("input_format_parallel_parsing", false);
                /// Here header auto detection can only cause troubles, since if it
                /// will find "header" the number of input and output rows will not
                /// match.
                context_for_reading->setSetting("input_format_csv_detect_header", false);
                context_for_reading->setSetting("input_format_tsv_detect_header", false);
                context_for_reading->setSetting("input_format_custom_detect_header", false);
                context = context_for_reading;

                /// Before anything is sent to a worker that has served somebody else.
                quarantineReusedWorker();

                auto thread_group = CurrentThread::getGroup();

                for (auto && send_data_task : send_data_tasks)
                {
                    send_data_threads.emplace_back([thread_group, task = std::move(send_data_task), this]() mutable
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::SEND_TO_SHELL_CMD);

                        try
                        {
                            task();
                        }
                        catch (...)
                        {
                            std::lock_guard lock(send_data_lock);
                            exception_during_send_data = std::current_exception();
                        }

                        // In case of exception, the task should be reset in thread
                        // worker function or else it breaks d'tor invariants such
                        // as in ~WriteBuffer.
                        //
                        // For completed execution, the task reset allows to account
                        // memory deallocation in sending data thread group.
                        task = {};
                    });
                }
                size_t max_block_size = configuration.max_block_size;

                if (configuration.read_fixed_number_of_rows)
                {
                    if (configuration.read_number_of_rows_from_process_output)
                    {
                        /// Initialize executor in generate
                        return;
                    }

                    max_block_size = configuration.number_of_rows_to_read;
                }

                pipeline = QueryPipeline(Pipe(context->getInputFormat(format, timeout_command_out, *sample_block, max_block_size)));
                pipeline.disableProfileEventUpdate();
                executor = std::make_unique<PullingPipelineExecutor>(pipeline);
            }
            catch (...)
            {
                /// A failure of the teardown itself must not replace the failure that got us here,
                /// and must not skip handing the borrowed worker back to the pool.
                try
                {
                    cleanup();
                }
                catch (...)
                {
                    tryLogCurrentException("ShellCommandSource");
                }
                throw;
            }
        }

        ~ShellCommandSource() override
        {
            /// Destructors are noexcept and `cleanup` allocates (an empty `QueryPipeline` allocates
            /// its processor list, returning the holder to the pool grows a vector), so under a
            /// memory limit it can throw - which would terminate the server.
            try
            {
                cleanup();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource");
            }
        }

    protected:
        void cleanup()
        {
            for (auto & thread : send_data_threads)
                if (thread.joinable())
                    thread.join();

            /// Stop reading the child's stdout before anything can take the descriptors away. The
            /// input format reads them through `timeout_command_out` - with
            /// `input_format_parallel_parsing` from its own segmentator thread - while `command`,
            /// which owns those descriptors and reaps the child, is destroyed before this pipeline
            /// (it has to be constructed last, see its declaration). Destroying the executor here
            /// joins those threads while the descriptors are still open. `prepare` does the same
            /// before its own wait, for the paths that reach it; this covers the rest.
            stopReadingCommandOutput();

            /// Record this borrow's resource usage before the child is gone. The two
            /// executable UDF types measure it differently.
            if (configuration.sampler)
            {
                if (process_pool)
                {
                    /// Resource accounting must observe the borrow's resident set before
                    /// the worker is torn down or the slot is handed back to the pool —
                    /// either path destroys `/proc/<pid>/{stat,status}` and the sampler
                    /// would then read zero CPU and zero `VmHWM`. For a worker that
                    /// `prepare` already reaped this has happened there; the call is
                    /// idempotent, so this one covers every path that does not go through
                    /// `prepare` (cancellation, a failure downstream).
                    recordPooledResourceUsageNoThrow();
                }
                else if (command)
                {
                    /// Peak memory was sampled from /proc VmHWM during IO, while the child
                    /// was provably alive; by cleanup the child has closed stdout and is
                    /// exiting, so its `/proc` mm fields are gone — no useful sample here.
                    ///
                    /// Capture wait4 rusage for CPU. When `prepare` already waited the child
                    /// via its blocking `wait` (`check_exit_code=true`), `isWaitCalled()` is
                    /// true and this is skipped. A child lingering past the poll budget is left
                    /// to `~ShellCommand`'s bounded `command_termination_timeout` + SIGTERM, so
                    /// profiling cannot turn cleanup into a query hang. No status check: a
                    /// non-zero exit must not raise CHILD_WAS_NOT_EXITED_NORMALLY here.
                    if (!command->isWaitCalled())
                    {
                        try
                        {
                            command->tryWaitWithoutStatusCheck();
                        }
                        catch (...)
                        {
                            tryLogCurrentException("ShellCommandSource");
                        }
                    }

                    /// Peak memory is independent of the wait: it comes from /proc VmHWM
                    /// sampled during IO and stamped by recordExecutableElapsed. CPU requires
                    /// the wait4 rusage and is recorded only when the wait succeeded.
                    configuration.sampler->recordExecutableElapsed();

                    if (command->wasChildResourceUsageCaptured())
                        configuration.sampler->recordExecutableFinished(
                            command->getChildUserTimeMicroseconds(),
                            command->getChildSystemTimeMicroseconds());
                }
            }

            if (command_is_invalid)
                command = nullptr;

            if (command_holder && process_pool)
            {
                bool valid_command = configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read;

                if (command && valid_command)
                    valid_command = pipeWorkerIsAtACleanBoundary();

                if (command && valid_command)
                    command_holder->returnCommand(std::move(command));

                /// A worker that is not going back to the pool has to die before its slot does: the
                /// query waiting for that slot starts a replacement at once, so leaving this process
                /// to be destroyed later - with a `command_termination_timeout` wait in front of it -
                /// lets the pool run over `pool_size` for as long as that takes.
                command = nullptr;

                process_pool->returnObject(std::move(command_holder));
            }
        }

        Chunk generate() override
        {
            rethrowExceptionDuringSendDataIfNeeded();

            Chunk chunk;

            try
            {
                if (configuration.read_fixed_number_of_rows)
                {
                    if (!executor && configuration.read_number_of_rows_from_process_output)
                    {
                        readText(configuration.number_of_rows_to_read, timeout_command_out);
                        char dummy = 0;
                        readChar(dummy, timeout_command_out);

                        size_t max_block_size = configuration.number_of_rows_to_read;
                        pipeline = QueryPipeline(Pipe(context->getInputFormat(format, timeout_command_out, *sample_block, max_block_size)));
                        pipeline.disableProfileEventUpdate();
                        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
                    }

                    if (current_read_rows >= configuration.number_of_rows_to_read)
                        return {};
                }

                if (!executor->pull(chunk))
                    return {};

                current_read_rows += chunk.getNumRows();
            }
            catch (...)
            {
                command_is_invalid = true;
                throw;
            }

            return chunk;
        }

        Status prepare() override
        {
            auto status = ISource::prepare();

            if (status == Status::Finished)
            {
                for (auto & thread : send_data_threads)
                    if (thread.joinable())
                        thread.join();

                /// Check if stderr was accumulated before checking exit code
                /// This ensures stderr exceptions take priority over exit code exceptions
                if (timeout_command_out.hasStderr())
                {
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                                  "Executable generates stderr: {}", timeout_command_out.getStderr());
                }

                bool wait_for_command = command != nullptr;
                if (process_pool)
                {
                    bool valid_command
                        = configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read;

                    /// A worker that answered in full is checked here rather than waited for: it is
                    /// meant to stay alive for the next borrow, so waiting for its exit is the one
                    /// thing that must not happen.
                    if (valid_command)
                        checkPooledWorkerAfterAnswering();

                    // We can only wait for pooled commands when they are invalid.
                    wait_for_command = wait_for_command && !valid_command;
                }

                /// Two independent reasons to wait, and either one on its own is enough. One is
                /// `check_exit_code`: the command's exit status has to be read. The other is
                /// `stderr_reaction`: this wait is the last stretch in which the command can still
                /// write, and what it writes there has to go through the reaction like anything it
                /// wrote earlier. Tying the second to the first is what left a command with
                /// `stderr_reaction = throw` and `check_exit_code = 0` able to complain on its way
                /// out and still have the query succeed.
                /// A pooled worker that is going back to the pool is not waited for at all, and
                /// that is the one path on which nothing else ever looks at its stderr again: the
                /// probe in `cleanup` discards a worker that left something there, but by then this
                /// query has already succeeded. Under `stderr_reaction` `throw` that is the setting
                /// failing to do the only thing it promises, and it fails for the query that
                /// actually caused the output.
                ///
                /// So what has already arrived is taken now and put through the reaction. Only what
                /// has arrived: this is an instant drain, not a wait. Waiting here for output that
                /// may never come would put an idle window on every successful pooled call, which
                /// is the hot path of `executable_pool`. Output that lands after this point is
                /// beyond reach without such a wait, and is answered the only other way there is -
                /// the worker is discarded rather than passed on.
                ///
                /// Note what this is and is not for. A command that writes its diagnostic together
                /// with its last row is already caught long before here: the read loop polls stderr
                /// alongside stdout, so those bytes are through the reaction by the time the rows
                /// are. What is left for this is the sliver between the server's final read and
                /// this point - small, but the only part of the path where the query could
                /// otherwise succeed while its own command was complaining.
                if (!wait_for_command && command && timeout_command_out.stderrIsObserved())
                {
                    static constexpr size_t late_stderr_drain_ms = 10;
                    timeout_command_out.drainStderrFully(late_stderr_drain_ms);

                    if (timeout_command_out.hasStderr())
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Executable generates stderr: {}", timeout_command_out.getStderr());
                }

                if (wait_for_command && (check_exit_code || timeout_command_out.stderrIsObserved()))
                {
                    /// Stop reading the child's stdout before this wait touches the same descriptor.
                    /// The source can be finished from above - a `LIMIT` downstream closes the
                    /// output port - while `ParallelParsingInputFormat` still has a segmentator
                    /// thread reading that pipe on its own; draining it here would then be a second
                    /// reader on one descriptor, and closing it afterwards would pull it out from
                    /// under that thread. Destroying the executor joins it first. `cleanup` does
                    /// the same for the paths that never reach here, and both are idempotent.
                    stopReadingCommandOutput();

                    /// The wait below reaps the worker, and `/proc/<pid>` goes with it. The
                    /// borrow's CPU and peak resident set have to be read before that, or this - a
                    /// discarded worker, which is the case the accounting is most wanted for -
                    /// reports zeros. A no-op off the pool path, and idempotent, so `cleanup` can
                    /// call the same thing for every path that does not come through here.
                    recordPooledResourceUsageNoThrow();

                    try
                    {
                        /// `waitDrainingOutput` rather than `wait`: the command may still be
                        /// writing. Reading its stdout stops at the row count this source asked
                        /// for, and stderr is only drained until it goes quiet, so a command that
                        /// carries on writing past either is blocked in `write` on a full pipe -
                        /// and `wait` reaps before it closes anything, so it would never return.
                        /// Draining lets the command reach its own exit, bounded by
                        /// `command_termination_timeout`.
                        ///
                        /// A command that does not exit within that budget fails the query rather
                        /// than being waved through: `check_exit_code` says the exit status is
                        /// checked, and a status that cannot be read is not a passing one. The
                        /// alternative - warn and succeed - would make the setting mean "checked,
                        /// unless the command avoids being checked", which is the one command it
                        /// most needs to hold for. `check_exit_code = 0` is how a command that is
                        /// not expected to exit promptly is configured.
                        const bool reaped = command->waitDrainingOutput(
                            [this](std::string_view str) { timeout_command_out.consumeStderrBytes(str); }, check_exit_code);

                        /// A status that could not be read is not a passing status, and that holds
                        /// however little time the command was given. A `command_termination_timeout`
                        /// of zero is the sharpest case: it says the command gets no grace period at
                        /// all, so one that has not exited by the time this looks is out of time by
                        /// the configuration's own definition, and is about to be signalled. Waving
                        /// that through with a warning would make `check_exit_code` mean "checked,
                        /// unless the timeout is short", which is not a contract anyone can rely on.
                        if (!reaped && check_exit_code)
                            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                                "The command did not exit within command_termination_timeout ({} seconds) after "
                                "its stdin was closed, so its exit code could not be checked; it will be "
                                "signalled. Give it a longer command_termination_timeout, or set check_exit_code "
                                "to 0 for a command that is not expected to exit on its own",
                                command->terminationTimeoutSeconds());
                    }
                    catch (Exception & e)
                    {
                        /// Enrich exit code exception with buffered stderr content (LOG_FIRST/LOG_LAST modes)
                        String stderr_content = timeout_command_out.consumeBufferedStderr();
                        if (!stderr_content.empty())
                            e.addMessage("Stderr: {}", stderr_content);
                        throw;
                    }

                    /// Asked again, because the wait above is the last stretch in which the command
                    /// can still write and it put what it wrote through the reaction. Under `throw`
                    /// that output fails the query just like output produced any earlier would.
                    if (timeout_command_out.hasStderr())
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Executable generates stderr: {}", timeout_command_out.getStderr());
                }

                rethrowExceptionDuringSendDataIfNeeded();
            }

            return status;
        }

        String getName() const override { return "ShellCommandSource"; }

    private:

        /// Decides whether the answer this borrow just read can be trusted, and whether the worker
        /// that produced it kept the obligations of `check_exit_code` - both while this query can
        /// still be failed, rather than in `cleanup`, where the only thing left to do about either
        /// is to throw the worker away after the caller has been told the query succeeded.
        ///
        /// A hung-up stdout means the worker is gone - it answered and exited. Under
        /// `check_exit_code` its status is exactly what this query was promised, so it is reaped
        /// and checked now; a pooled worker is otherwise never waited for, which is what left
        /// `check_exit_code` unenforced for a command that answers correctly and then exits
        /// non-zero.
        void checkPooledWorkerAfterAnswering()
        {
            if (!command)
                return;

            const auto state = timeout_command_out.channelState(/*consider_buffered_output=*/ false);

            if (!state.stdout_hung_up)
                return;

            /// The worker is gone either way; it must not go back to the pool.
            command_is_invalid = true;

            if (!check_exit_code)
                return;

            stopReadingCommandOutput();

            try
            {
                command->waitDrainingOutput(
                    [this](std::string_view str) { timeout_command_out.consumeStderrBytes(str); }, /*check_exit_status=*/ true);
            }
            catch (Exception & e)
            {
                String stderr_content = timeout_command_out.consumeBufferedStderr();
                if (!stderr_content.empty())
                    e.addMessage("Stderr: {}", stderr_content);
                throw;
            }

            if (timeout_command_out.hasStderr())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable generates stderr: {}", timeout_command_out.getStderr());
        }

        /// Looks over a reused pooled worker's pipes before this borrow sends it anything.
        ///
        /// The probe when the worker was handed back proved only that it was quiet at that instant.
        /// A command that goes quiet, is pooled, and then writes leaves those bytes waiting for
        /// whoever borrows it next - and this transport has no framing that would let this query
        /// tell them apart from its own answer. So the two pipes are treated very differently.
        ///
        /// Late stdout is fatal. The shared-memory transport can afford to read a stale byte and
        /// then reject the frame, because every response carries the id of the request it answers;
        /// here the first thing this query parses would simply be somebody else's rows, silently
        /// and plausibly. A query failed loudly is worth a great deal more than a query answered
        /// wrongly, so that is what happens, and the worker does not go back to the pool.
        ///
        /// Late stderr is not fatal - nothing can mistake it for output - but it must not go
        /// through `stderr_reaction` either, or this query fails for a diagnostic it did not cause.
        /// It is reported against the worker instead, and the pipe is emptied: a worker that filled
        /// it is blocked in `write` and would not read this borrow's input at all.
        void quarantineReusedWorker()
        {
            if (!worker_is_reused || !command)
                return;

            static constexpr size_t stderr_drain_budget_ms = 100;

            try
            {
                const String leftover_stderr = timeout_command_out.consumePendingStderrWithoutReaction();
                if (!leftover_stderr.empty())
                    LOG_WARNING(
                        getLogger("ShellCommandSource"),
                        "A pooled command had unread output on its stderr when it was borrowed, so it was "
                        "written after the response of an earlier invocation. It is reported here rather than "
                        "against this query, which did not cause it. Stderr: {}",
                        leftover_stderr);

                /// The report above is capped; the pipe must not be. What is left beyond the cap,
                /// and what the command writes while this drains, is the same earlier query's and
                /// is dropped without the reaction for the same reason.
                timeout_command_out.drainStderrFully(stderr_drain_budget_ms, /*with_reaction=*/ false);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource");
            }

            if (!timeout_command_out.channelState().stdout_has_unread_output)
                return;

            command_is_invalid = true;
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "A pooled command had unread output on its stdout when it was borrowed, so it was written "
                "after the response to an earlier invocation. This transport has no way to tell those bytes "
                "from this query's own result, so the query fails rather than being answered with them. The "
                "command must write nothing past the rows it was asked for");
        }

        /// Whether a pooled pipe-mode worker may be handed on to the next query.
        ///
        /// The row count that got us here says this query received what it asked for. It says
        /// nothing about the state the command is in afterwards, and two states disqualify it -
        /// both of which are silent until they surface as somebody else's failure.
        ///
        /// A command that has exited leaves a hung-up stdout, and the next borrow discovers that
        /// only when its own write to a dead stdin fails. A command that wrote past its last row
        /// leaves those bytes on the pipe, where the next borrow reads them as the beginning of
        /// *its* result: the same hazard the shared-memory transport spells out for its response
        /// frame, except that here it corrupts rows rather than a frame, which is worse.
        ///
        /// Only what is still in the kernel pipe counts, not what this buffer has read and not
        /// parsed (`consider_buffered_output` is false). That is not caution, it is the actual
        /// distinction: a format reader routinely holds bytes it did not parse - it reads ahead in
        /// blocks and stops at the row it was asked for - and those bytes die with this source and
        /// reach nobody. Bytes left in the *pipe* are the ones the next borrower would read as its
        /// own. Counting the buffered ones instead makes every well-behaved pooled worker look
        /// dirty, and quietly turns `executable_pool` into a process per call.
        ///
        /// Under `stderr_reaction` `none` pending stderr is not a reason to discard - those bytes
        /// are nobody's - but it is a reason to take them off the pipe first, or they accumulate
        /// across borrows until the command blocks in `write` and the borrow after that waits for a
        /// worker that will never read its request.
        bool pipeWorkerIsAtACleanBoundary() noexcept
        {
            if (!command)
                return false;

            /// The drain polls and reads; either can fail, and this function may not throw. A
            /// worker whose pipes could not even be probed is not one to hand on: discard it.
            try
            {
                static constexpr size_t stderr_drain_budget_ms = 100;
                if (!timeout_command_out.stderrIsObserved())
                    timeout_command_out.drainStderrFully(stderr_drain_budget_ms);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource", "Cannot probe the pipes of a pooled command; discarding its process");
                return false;
            }

            const auto state = timeout_command_out.channelState(/*consider_buffered_output=*/ false);
            if (state.isClean())
                return true;

            try
            {
                const String leftover_stderr
                    = state.stderr_has_unread_output ? timeout_command_out.consumePendingStderr() : String{};

                if (state.stdout_hung_up && !state.stdout_has_unread_output)
                    LOG_DEBUG(
                        getLogger("ShellCommandSource"),
                        "The process of a pooled command exited after answering, so it was not returned to the "
                        "pool.");
                else
                    LOG_WARNING(
                        getLogger("ShellCommandSource"),
                        "A pooled command left unread output on its {} after answering, so its process was "
                        "discarded instead of reused. The command must write nothing past the rows it was asked "
                        "for, and must write diagnostics before them rather than after.{}{}",
                        state.stdout_has_unread_output && state.stderr_has_unread_output
                            ? "stdout and stderr"
                            : (state.stdout_has_unread_output ? "stdout" : "stderr"),
                        leftover_stderr.empty() ? "" : " Stderr: ",
                        leftover_stderr);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource");
            }

            return false;
        }

        /// Joins whatever is still reading the command's stdout and lets go of the pipeline over it.
        /// Moved into a temporary rather than assigned an empty pipeline: assignment would construct
        /// one, and that allocates, on a path that also runs from a destructor. Idempotent.
        void stopReadingCommandOutput()
        {
            executor.reset();
            {
                QueryPipeline discarded = std::move(pipeline);
            }
        }

        void rethrowExceptionDuringSendDataIfNeeded()
        {
            std::lock_guard lock(send_data_lock);
            if (exception_during_send_data)
            {
                command_is_invalid = true;
                std::rethrow_exception(exception_during_send_data);
            }
        }

        /// Reads the borrow's CPU and peak resident set out of `/proc` while the worker is still
        /// there to be read. Idempotent (see `UDFProcessSubtreeSampler::recordReleased`), so both
        /// the teardown that discards a worker and the ordinary end of a borrow can call it.
        ///
        /// Never throws: it reads procfs and builds containers, so a memory limit can refuse it,
        /// and it runs both from `cleanup` - which the destructor calls - and from `prepare`, where
        /// failing a query that has already produced its rows over a profiling read would be worse
        /// than losing the measurement.
        void recordPooledResourceUsageNoThrow() noexcept
        {
            if (!configuration.sampler || !process_pool)
                return;

            try
            {
                configuration.sampler->recordReleased();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource");
            }
        }

        ContextPtr context;
        std::string format;
        SharedHeader sample_block;

        ShellCommandSourceConfiguration configuration;

        TimeoutReadBufferFromFileDescriptor timeout_command_out;

        size_t current_read_rows = 0;

        std::shared_ptr<ProcessPool> process_pool;

        bool check_exit_code = false;

        QueryPipeline pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;

        std::vector<ThreadFromGlobalPool> send_data_threads;

        std::mutex send_data_lock;
        std::exception_ptr exception_during_send_data;

        std::atomic<bool> command_is_invalid {false};

        /// Whether `command` is a process that has already served a borrow, and may therefore have
        /// left something on its pipes. A freshly started one cannot have.
        bool worker_is_reused = false;

        /// Taken over after every other member, because every other member has to be able to throw
        /// without costing a healthy pooled worker: until this object owns these two they still
        /// belong to `createPipe`, whose scope guard hands them back to the pool. `timeout_command_out`
        /// allocates its buffer and `pipeline` allocates in its default constructor, so this is not
        /// a theoretical ordering. Destroyed first for the same reason they are constructed last,
        /// which is safe: `cleanup` has already joined the send-data threads and handed the command
        /// back, and ~TimeoutReadBufferFromFileDescriptor deliberately does not touch its descriptors.
        std::unique_ptr<ShellCommand> command;
        ShellCommandHolderPtr command_holder;
    };

    class SendingChunkHeaderTransform final : public ISimpleTransform
    {
    public:
        SendingChunkHeaderTransform(SharedHeader header, WriteBuffer & buffer_)
            : ISimpleTransform(header, header, false)
            , buffer(buffer_)
        {
        }

        String getName() const override { return "SendingChunkHeaderTransform"; }

    protected:

        void transform(Chunk & chunk) override
        {
            writeText(chunk.getNumRows(), buffer);
            writeChar('\n', buffer);
        }

    private:
        WriteBuffer & buffer;
    };

    /** Serializes straight into a shared-memory region, enlarging it when it fills up.
      *
      * Going through a heap buffer first would make the serialized chunk exist twice at the same
      * time - once on the heap and once in the region - doubling the peak memory of a call, and it
      * would copy every byte, which is what this transport exists to avoid. Writing through the
      * mapping is safe because the region is sealed against shrinking (see `SharedMemoryRegion`):
      * the command cannot take a page out from under this buffer.
      *
      * `grow` must make the region hold at least `required` bytes or throw. Enlarging a region
      * replaces its mapping, so `data()` is re-read after every growth; the bytes written so far
      * survive it, because the file behind the mapping keeps its contents.
      */
    class WriteBufferToSharedMemoryRegion : public WriteBuffer
    {
    public:
        using GrowFn = std::function<void(size_t required)>;

        WriteBufferToSharedMemoryRegion(SharedMemoryRegion & region_, GrowFn grow_)
            : WriteBuffer(region_.data(), region_.size())
            , region(region_)
            , grow(std::move(grow_))
        {
        }

    private:
        /// Called once the working buffer is used up - which also happens on the flush that ends the
        /// serialization, so a region filled to its last byte does not by itself mean that the region
        /// is too small. One spare byte outside the region tells the two cases apart: a writer that
        /// still has something to say lands in that byte and comes back here, and only then is the
        /// region really enlarged. Without it, input that ends exactly at the end of the region would
        /// demand one byte more than it needs - a needless growth, or a failed query when the region
        /// is not allowed to grow at all (`shared_memory_max_size` defaults to `shared_memory_size`).
        void nextImpl() override
        {
            /// `bytes` is only updated after this returns, so this is exactly what has been written,
            /// wherever it went.
            size_t written = count();

            if (written > region.size())
                moveOverflowIntoRegion(written);

            if (written == region.size())
            {
                set(overflow.data(), overflow.size());
                return;
            }

            set(region.data() + written, region.size() - written);
        }

        /// The serialization is over and everything is already in the region, so there is nothing
        /// to flush here. In particular the region must not grow from this method: `finalize`
        /// blocks memory-limit exceptions for its whole body, so a growth started here would commit
        /// its pages without `max_memory_usage` ever being enforced. Draining the spare byte is the
        /// writer's job (any `next` does it, under the memory limit) - see serializeInto, which is
        /// also why this buffer is never finalized implicitly from a destructor.
        ///
        /// This only covers growth this buffer starts itself. A format that wraps this one
        /// (`WriteBufferValidUTF8`, `PeekableWriteBuffer`) flushes into it from inside its own
        /// `finalize`, under the same blocked limit, and a growth from there cannot be checked
        /// against `max_memory_usage` either - the bytes are still charged to the query, only the
        /// limit is not enforced for them. `ensureRegionFits` keeps that bounded by growing in a
        /// small step instead of doubling whenever it sees the limit blocked.
        void finalizeImpl() override
        {
            if (offset() != 0 && working_buffer.begin() == overflow.data())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Shared-memory region buffer is finalized with a byte past the end of the region; "
                    "it has to be flushed with `next` first");

            bytes += offset();
            set(nullptr, 0);
        }

        /// Enlarges the region to hold everything written so far (this throws when it cannot) and
        /// copies the spare byte into the space that opened up.
        void moveOverflowIntoRegion(size_t written)
        {
            size_t old_size = region.size();
            grow(written);
            memcpy(region.data() + old_size, overflow.data(), written - old_size);
        }

        SharedMemoryRegion & region;
        GrowFn grow;
        /// Catches a writer stepping past the end of the region; see nextImpl.
        std::array<char, 1> overflow{};
    };

    /** Exchanges data with the child process through a shared-memory region instead of the pipes.
      *
      * The protocol is strictly lock-step and driven synchronously from generate() (see the full
      * specification in docs/reference/functions/regular-functions/udf.mdx, "Shared memory mode"):
      *   1. pull the next input chunk and serialize it into the shared-memory region;
      *   2. write a request to the child's stdin:
      *        varint version, varint request id, string path, varint input offset, varint input size;
      *   3. read the response from the child's stdout: the request id echoed back, a varint status,
      *        then, on success, varint output offset + varint output size; on failure, a
      *        length-prefixed error message;
      *   4. deserialize the output from the region.
      * Each request invalidates the previous contents of the region. The region is created once per
      * process (reused across pool borrows), so the loop keeps a single mapping for the whole session.
      */
    class ShellCommandSharedMemorySource final : public ISource
    {
    public:
        ShellCommandSharedMemorySource(
            ContextPtr context_,
            const std::string & format_,
            size_t command_read_timeout_milliseconds,
            size_t command_write_timeout_milliseconds,
            ExternalCommandStderrReaction stderr_reaction,
            bool check_exit_code_,
            SharedHeader sample_block_,
            ShellCommandHolder::ShellCommandBuilderFunc build_command_,
            Pipe input_pipe_,
            size_t shared_memory_size_,
            size_t shared_memory_max_size_,
            bool pipeline_mode_,
            bool is_pooled_,
            const ShellCommandSourceConfiguration & configuration_,
            std::unique_ptr<ShellCommandHolder> && command_holder_,
            std::shared_ptr<ProcessPool> process_pool_)
            : ISource(sample_block_)
            , context(context_)
            , format(format_)
            , sample_block(sample_block_)
            , configuration(configuration_)
            , is_pooled(is_pooled_)
            , shared_memory_max_size(shared_memory_max_size_)
            , pipeline_mode(pipeline_mode_)
            , process_pool(process_pool_)
            , check_exit_code(check_exit_code_)
            , command_holder(std::move(command_holder_))
        {
            try
            {
                /// Create the region(s) here (not in the caller) so that any failure — creating the
                /// `memfd`, reserving its storage, sealing or mapping it — is cleaned up by this
                /// constructor, which returns the borrowed process holder to the pool. On the
                /// pool path a region is created once and reused across borrows. The pipelined
                /// transport uses two regions for double buffering.
                ///
                /// The regions are charged to this query's memory tracker for the whole borrow, so
                /// they count against its memory limit; the charge is released on the same (query)
                /// thread in cleanup(), including when region creation below throws. A pooled region
                /// outlives the borrow, so the charge for it is handed over from the holder here and
                /// handed back in cleanup(); the holder accounts it globally while the worker sits
                /// idle in the pool. Exactly one tracker holds it at any moment — see
                /// `ShellCommandHolder::releaseChargeToBorrower`.
                if (command_holder)
                    command_holder->releaseChargeToBorrower();

                /// May throw MEMORY_LIMIT_EXCEEDED.
                size_t region_count = pipeline_mode ? 2 : 1;
                for (size_t i = 0; i < region_count; ++i)
                {
                    bool region_created = false;
                    if (command_holder)
                    {
                        /// Charge before the region is created: creating it commits its pages, so
                        /// a query that is already at its memory limit has to be rejected first
                        /// (the non-pooled branch below does the same). A region that survived a
                        /// previous borrow may have grown, so charge what it actually holds - its
                        /// committed size; a missing one is created at exactly shared_memory_size_.
                        size_t existing_size = command_holder->getSharedMemorySize(i);
                        chargeQueryMemory(existing_size ? existing_size : shared_memory_size_);
                        regions[i] = command_holder->getOrCreateSharedMemory(shared_memory_size_, i, region_created);
                        regions_created_by_this_borrow[i] = region_created;
                    }
                    else
                    {
                        chargeQueryMemory(shared_memory_size_);
                        regions[i] = std::make_shared<SharedMemoryRegion>(shared_memory_size_);
                        region_created = true;
                        regions_created_by_this_borrow[i] = region_created;
                    }

                    if (region_created)
                        ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, shared_memory_size_);
                }

                /// Only now the process. A `memfd` has no name a process could open later, so the
                /// command reaches its regions by having inherited their descriptors at `exec` -
                /// which is why the regions above had to come first. A pooled worker that served an
                /// earlier borrow inherited them when it was started; `buildCommand` hands it back
                /// as it is. `command` is the last thing this constructor takes on for the reason
                /// given at its declaration.
                if (command_holder)
                {
                    const bool worker_is_reused = command_holder->hasReturnedCommand();
                    command = command_holder->buildCommand();

                    /// A worker that exited while it sat in the pool is replaced before anything
                    /// is built on it - see the same step on the pipe path in `createPipe`. Its
                    /// regions are unaffected and the replacement inherits the very same ones.
                    if (worker_is_reused && pooledProcessHasExitedCleanly(*command))
                    {
                        LOG_DEBUG(
                            getLogger("ShellCommandSharedMemorySource"),
                            "The process of an executable UDF (pid {}) exited while it was idle in the pool; "
                            "starting a replacement for this borrow.",
                            command->getPid());

                        command.reset();
                        command = command_holder->buildCommand();
                    }

                    /// Borrow acquired: capture the pid for procfs sampling. Best-effort, and it
                    /// allocates, so a failure must not fail a query that is otherwise ready.
                    if (configuration.sampler)
                    {
                        try
                        {
                            configuration.sampler->recordPidAcquired(command->getPid());
                        }
                        catch (...)
                        {
                            tryLogCurrentException("ShellCommandSharedMemorySource");
                        }
                    }
                }
                else
                {
                    std::vector<std::pair<int, int>> inherited_fds;
                    for (size_t i = 0; i < region_count; ++i)
                        inherited_fds.emplace_back(SHARED_MEMORY_FIRST_CHILD_FD + static_cast<int>(i), regions[i]->fd());
                    command = build_command_(inherited_fds);

                    if (configuration.sampler)
                        configuration.sampler->recordExecutablePid(command->getPid());
                }

                timeout_command_out = std::make_unique<TimeoutReadBufferFromFileDescriptor>(
                    command->out.getFD(), command->err.getFD(), command_read_timeout_milliseconds, stderr_reaction, configuration_.sampler.get());

                /// Match the pipe-mode reader: disable header auto-detection, otherwise the first row
                /// of the result could be consumed as a header and the row count would not match.
                auto context_for_reading = Context::createCopy(context);
                if (configuration.read_fixed_number_of_rows)
                    context_for_reading->setSetting("input_format_parallel_parsing", false);
                context_for_reading->setSetting("input_format_csv_detect_header", false);
                context_for_reading->setSetting("input_format_tsv_detect_header", false);
                context_for_reading->setSetting("input_format_custom_detect_header", false);
                context = context_for_reading;

                timeout_command_in = std::make_unique<TimeoutWriteBufferFromFileDescriptor>(
                    command->in.getFD(), command_write_timeout_milliseconds, configuration_.sampler.get());

                /// Before the first request, so that anything an earlier borrow's command wrote
                /// after its response is cleared off the pipe and reported against nobody, rather
                /// than read during this query and charged to it.
                discardStderrLeftByAPreviousBorrow();

                input_header = materializeBlock(input_pipe_.getHeader());
                input_pipe_.resize(1);
                input_pipeline = QueryPipeline(std::move(input_pipe_));
                input_executor = std::make_unique<PullingPipelineExecutor>(input_pipeline);

                /// In pipelined mode a background thread serializes the next input chunk into the
                /// other region while the current chunk is being processed by the child. It inherits
                /// this query's thread group for correct CPU/memory accounting and is joined in
                /// cleanup() before any shared state is torn down.
                ///
                /// Note that the prefetch only pays off when input_pipe_ yields more than one block.
                /// The executable-UDF caller builds it from a single SourceFromSingleChunk, so today
                /// the producer serializes one chunk and then reports exhaustion: the machinery is
                /// exercised but nothing overlaps. See docs/reference/functions/regular-functions/udf.mdx.
                if (pipeline_mode)
                    producer.start(
                        CurrentThread::getGroup(),
                        ThreadName::SEND_TO_SHELL_CMD,
                        [this](size_t index) { return serializeInto(index); });

                constructor_finished = true;
            }
            catch (...)
            {
                /// Construction cannot send a protocol request, so a pooled command is still at a
                /// clean boundary and can be returned together with the holder - provided its
                /// pipes were wrapped, which `controlChannelIsClean` checks; a worker whose reader
                /// never came to be is discarded. A failure of the teardown itself must not replace
                /// the failure that got us here.
                command_can_be_reused = true;
                try
                {
                    cleanup();
                }
                catch (...)
                {
                    tryLogCurrentException("ShellCommandSharedMemorySource");
                }
                throw;
            }
        }

        ~ShellCommandSharedMemorySource() override
        {
            /// Destructors are noexcept, so nothing may escape - and `cleanup` is not allocation
            /// free (assigning an empty `QueryPipeline` allocates its processor list, handing the
            /// holder back to the pool grows a vector), which under a memory limit is exactly where
            /// an exception comes from.
            try
            {
                cleanup();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        String getName() const override { return "ShellCommandSharedMemorySource"; }

    protected:
        Chunk generate() override
        {
            try
            {
                while (true)
                {
                    if (output_executor)
                    {
                        Chunk chunk;
                        if (output_executor->pull(chunk))
                        {
                            /// `pull` can report success and still return an empty chunk (it makes
                            /// one execution step, which does not necessarily produce data). Such a
                            /// chunk would finish the source in `ISource::tryGenerate` and skip the
                            /// row-count checks below, so keep pulling instead of returning it.
                            if (!chunk.hasRows())
                                continue;

                            /// A command that produces more rows than requested violates the UDF
                            /// protocol. Detect it here — before the oversized chunk leaves the
                            /// source — so the exception below marks the command invalid and a
                            /// pooled worker is discarded instead of being reused as valid.
                            if (configuration.read_fixed_number_of_rows
                                && current_read_rows + chunk.getNumRows() > configuration.number_of_rows_to_read)
                                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                                    "Executable UDF wrong result, expected {} row(s), but the command produced more (at least {})",
                                    configuration.number_of_rows_to_read,
                                    current_read_rows + chunk.getNumRows());

                            current_read_rows += chunk.getNumRows();
                            return chunk;
                        }

                        output_executor.reset();
                        {
                            /// Moved into a temporary rather than assigned an empty pipeline:
                            /// assignment would construct one, and that allocates - here, right
                            /// after a perfectly good answer was read, that would fail the query and
                            /// throw away a healthy worker over cleaning up the result.
                            QueryPipeline discarded = std::move(output_pipeline);
                        }
                        output_read_buffer.reset();

                        /// The current buffer is fully drained; hand it back to the producer so it
                        /// can serialize the next-but-one chunk into it.
                        if (pipeline_mode && holding_buffer)
                        {
                            producer.release(active_index);
                            holding_buffer = false;
                        }
                    }

                    /// On the pool path we cannot rely on stdin EOF; stop once enough rows were produced.
                    /// On the non-pooled path the child exits on stdin EOF, so close it before wait().
                    if (configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read)
                    {
                        closeStdinIfNeeded(is_pooled);
                        return {};
                    }

                    bool sent = pipeline_mode ? sendNextRequestPipelined() : sendNextRequest();
                    if (!sent)
                    {
                        assertEnoughRowsRead();
                        return {};
                    }
                }
            }
            catch (...)
            {
                /// A failure while the next input is being prepared - the input pipeline itself, a
                /// region that cannot grow to hold the serialized block, an exceeded memory limit -
                /// never reached the child: no request was sent, so a pooled worker is still at a
                /// clean protocol boundary and it, together with its regions, can be reused by the
                /// next borrow. Any other failure either leaves the child's state unknown (a
                /// partially written request, an unread response) or proves that it misbehaves, so
                /// the worker has to be discarded.
                if (preparing_input)
                    command_can_be_reused = true;
                else if (!command_can_be_reused)
                    command_is_invalid = true;
                throw;
            }
        }

        Status prepare() override
        {
            auto status = ISource::prepare();

            if (status == Status::Finished)
            {
                /// Join the background producer: generate() stops taking items from it as soon as
                /// enough rows were read, so a failure of the producer after that point is still
                /// unobserved here and is rethrown below.
                producer.stop();

                /// Decided once and used twice below: the answer includes a probe of the child's
                /// stdout, so asking again could give a different one, and closing stdin for a
                /// worker that is then not reaped - or reaping one whose stdin was left open, which
                /// makes the blocking wait sit out the whole command_termination_timeout - is
                /// exactly the mismatch the two uses have to avoid.
                const bool keep_command = commandIsReused();

                /// Reported here, while the command's pipes are still open. The wait below reaps a
                /// discarded child, and reaping closes `out` and `err` - after which their descriptor
                /// numbers may already belong to something else, so there is nothing left to read the
                /// leftover output from. `cleanup` reports instead on the paths that never reach this
                /// one (cancellation, an exception downstream); whichever runs first clears the flags,
                /// so a discard is reported once.
                reportDirtyChannelDiscard();

                /// Everything below this line takes the worker's `/proc` entries away: closing its
                /// stdin makes a discarded child exit, and a zombie has no `mm`, so its `VmHWM` is
                /// gone the moment it does; the wait after that reaps the pid altogether. So the
                /// borrow's CPU and peak resident set are read here, while there is still something
                /// to read - otherwise a discarded worker, which is exactly the case this teardown
                /// exists for, would report zeros. `cleanup` calls the same thing for the paths
                /// that never reach `prepare`; it is idempotent.
                if (!keep_command)
                    recordPooledResourceUsageNoThrow();

                /// The source can finish without generate() reaching the end of the input — most
                /// notably on cancellation. A child that is not going back to the pool only exits
                /// once it sees EOF on its stdin, so close it before the blocking wait below, which
                /// reaps the child before closing any pipe itself.
                closeStdinNoThrow(keep_command);

                if (timeout_command_out->hasStderr())
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Executable generates stderr: {}", timeout_command_out->getStderr());

                /// Two independent reasons to wait, and either one on its own is enough. One is
                /// `check_exit_code`: the worker's exit status has to be read. The other is
                /// `stderr_reaction`: this wait is the last stretch in which the command can still
                /// write, and what it writes there has to go through the reaction like anything it
                /// wrote earlier. Tying the second to the first is what left a command with
                /// `stderr_reaction = throw` and `check_exit_code = 0` able to complain on its way
                /// out and still have the query succeed.
                const bool wait_for_command = command != nullptr && !keep_command;
                if (wait_for_command && (check_exit_code || timeout_command_out->stderrIsObserved()))
                {
                    try
                    {
                        /// The same decision as the stdin close above: a worker that goes back to
                        /// the pool must not be reaped here, and one that does not had its stdin
                        /// closed, so this wait can actually finish. `commandIsReused` reads a
                        /// missing command as "not reused", so check it before the wait.
                        ///
                        /// `waitDrainingOutput` rather than `wait`, because the very reason a
                        /// worker is discarded here can be that it wrote past its response frame:
                        /// once that output fills the pipe the child sits in `write`, and a plain
                        /// `wait` - which reaps before it closes anything - would never return.
                        /// Nothing reads those pipes any more, so this wait takes the bytes off
                        /// them and lets the child reach its own exit, bounded by
                        /// `command_termination_timeout`. What it finds on stderr still goes
                        /// through `stderr_reaction`: this is the last stretch in which a command
                        /// can write, and under `throw` that output fails the query like any other.
                        ///
                        /// A worker that does not exit within that budget fails the query rather
                        /// than being waved through: `check_exit_code` says the exit status is
                        /// checked, and a status that cannot be read is not a passing one. The
                        /// query's rows are already correct, but so are the rows of any command
                        /// whose exit code turns out to be non-zero - which is exactly what the
                        /// setting exists to reject. `check_exit_code = 0` is how a command that is
                        /// not expected to exit promptly is configured.
                        const bool reaped = command->waitDrainingOutput(
                            [this](std::string_view str) { timeout_command_out->consumeStderrBytes(str); }, check_exit_code);

                        /// As on the pipe path: a status that could not be read is not a passing
                        /// status, whatever the budget was. See the note there.
                        if (!reaped && check_exit_code)
                            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                                "The process of an executable UDF did not exit within "
                                "command_termination_timeout ({} seconds) after its stdin was closed, so its exit "
                                "code could not be checked; it will be signalled. Give it a longer "
                                "command_termination_timeout, or set check_exit_code to 0 for a command that is "
                                "not expected to exit on its own",
                                command->terminationTimeoutSeconds());
                    }
                    catch (Exception & e)
                    {
                        String stderr_content = timeout_command_out->consumeBufferedStderr();
                        if (!stderr_content.empty())
                            e.addMessage("Stderr: {}", stderr_content);
                        throw;
                    }

                    /// Asked again, because the wait above is the last stretch in which the command
                    /// can still write and it put what it wrote through the reaction. Under `throw`
                    /// that output fails the query just like output produced any earlier would.
                    if (timeout_command_out->hasStderr())
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Executable generates stderr: {}", timeout_command_out->getStderr());
                }

                /// A producer error (a failing input pipeline, an exceeded memory limit) must not be
                /// swallowed just because the consumer no longer needed the chunk it was preparing.
                producer.rethrowIfFailed();
            }

            return status;
        }

    private:
        void assertEnoughRowsRead()
        {
            if (configuration.read_fixed_number_of_rows && current_read_rows < configuration.number_of_rows_to_read)
            {
                command_is_invalid = true;
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable UDF wrong result, expected {} row(s), actual {}",
                    configuration.number_of_rows_to_read,
                    current_read_rows);
            }
        }

        /// Pulls the next non-empty input block and serializes it into regions[index] (growing the
        /// region on demand). Returns the serialized size, or std::nullopt when input is exhausted.
        /// In pipelined mode this runs on the background producer thread; otherwise inline.
        std::optional<size_t> serializeInto(size_t index)
        {
            Block input_block;
            bool have_input = false;
            /// A teardown (cancellation, or the consumer having read all the rows it needs) waits
            /// for this callback to return, so give up between blocks instead of pulling the input
            /// pipeline dry first. In synchronous mode the producer was never started and this is
            /// always false.
            while (!producer.isStopRequested() && input_executor->pull(input_block))
            {
                if (input_block.rows() != 0)
                {
                    have_input = true;
                    break;
                }
            }

            if (!have_input)
                return std::nullopt;

            /// Serialize into the region itself, growing it on demand (up to shared_memory_max_size)
            /// whenever it fills up. The command asks for more room later if its result does not fit
            /// next to the input.
            /// Deliberately not auto-finalized: on the exception path the buffer is destroyed while
            /// the stack unwinds, which holds nothing back (everything written is already in the
            /// region), and a `finalize` from a destructor could not report a problem anyway.
            /// Serialize into the region itself, growing it on demand (up to shared_memory_max_size)
            /// whenever it fills up. The command asks for more room later if its result does not fit
            /// next to the input.
            /// Deliberately not auto-finalized: on the exception path the buffer is destroyed while
            /// the stack unwinds, which holds nothing back (everything written is already in the
            /// region), and a `finalize` from a destructor could not report a problem anyway.
            WriteBufferToSharedMemoryRegion write_buffer(
                *regions[index],
                /// The input is serialized straight into the region, so its total size is known only
                /// once it is over: every request for room is a lower bound on what it needs.
                [this, index](size_t required)
                { ensureRegionFits(index, required, "The serialized input", /*required_is_lower_bound=*/ true); });

            auto output_format = context->getOutputFormat(format, write_buffer, input_header);
            formatBlock(output_format, input_block);

            /// `formatBlock` flushes the format into the buffer, which also moves a byte that ended
            /// up in its spare slot into the region. Do it explicitly all the same: it is what grows
            /// the region under this query's memory limit, and `finalize` may not do it (see
            /// WriteBufferToSharedMemoryRegion::finalizeImpl).
            write_buffer.next();
            write_buffer.finalize();

            return write_buffer.count();
        }

        /// Request to the child: protocol version, file path, input offset, input size.
        void sendRequest(size_t index, size_t input_size, UInt64 request_id)
        {
            writeVarUInt(SHARED_MEMORY_PROTOCOL_VERSION, *timeout_command_in);
            writeVarUInt(request_id, *timeout_command_in);
            writeStringBinary(SharedMemoryRegion::pathForChildFd(SHARED_MEMORY_FIRST_CHILD_FD + static_cast<int>(index)), *timeout_command_in);
            writeVarUInt(static_cast<UInt64>(0), *timeout_command_in);
            writeVarUInt(static_cast<UInt64>(input_size), *timeout_command_in);
            timeout_command_in->next();
        }

        /// The id for the next request to this process. Routed through the holder when there is
        /// one only so that the two paths read the same; the value is drawn at random either way.
        UInt64 nextRequestId() const
        {
            if (command_holder)
                return command_holder->nextRequestId();

            return generateRequestId();
        }

        /// Sends the request for regions[index] to the child and sets up output_executor over the
        /// response. The region must already hold `input_size` bytes of serialized input at offset 0.
        void exchange(size_t index, size_t input_size)
        {
            /// Counted here rather than in serializeInto: the producer thread runs ahead of the
            /// consumer, so a prefetched chunk that is never sent must not count as a call.
            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryCalls);
            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryInputBytes, input_size);

            UInt64 output_offset = 0;
            UInt64 output_size = 0;

            while (true)
            {
                const UInt64 request_id = nextRequestId();
                sendRequest(index, input_size, request_id);

                /// Response from the child: the id of the request it is answering, a status varint,
                /// and then either the output location (on success), the size it needs (when the
                /// region is too small) or an error message.
                ///
                /// The id is what makes the answer provably an answer to *this* request. Without
                /// it, anything the process left on its stdout - a stray byte after an earlier
                /// response, a line of debug output - is read as the status of this one, and the
                /// rest of the frame shifts along with it: the real status becomes the offset and
                /// the real offset becomes the size. That is not a failure, it is a plausible
                /// frame, and with a matching row count the query returns the region's *input*
                /// bytes as its result. Silently wrong answers are the one outcome the checks
                /// around here exist to prevent, and only an echoed id rules them out.
                UInt64 answered_request_id = 0;
                readVarUInt(answered_request_id, *timeout_command_out);

                if (answered_request_id != request_id)
                {
                    command_is_invalid = true;
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Executable UDF answered request {} with a frame for request {}. The command must echo "
                        "the request id and must write nothing but the response frame; a process that has written "
                        "anything else is out of step with the protocol and its answers cannot be trusted",
                        request_id, answered_request_id);
                }

                UInt64 status = 0;
                readVarUInt(status, *timeout_command_out);

                if (status == SHARED_MEMORY_STATUS_NEED_MORE_SPACE)
                {
                    /// The result does not fit next to the input; the server is the only side that
                    /// can enlarge the region, so it does that and re-sends the same request. The
                    /// serialized input survives the growth (`ftruncate` keeps the file contents),
                    /// so it does not have to be written again. This terminates: every iteration
                    /// strictly increases the region size, which is capped by shared_memory_max_size.
                    UInt64 requested_size = 0;
                    readVarUInt(requested_size, *timeout_command_out);

                    if (requested_size <= regions[index]->size())
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Executable UDF asked for a shared-memory region of {} bytes, which is not larger "
                            "than the current one ({} bytes)",
                            requested_size, regions[index]->size());

                    try
                    {
                        ensureRegionFits(index, requested_size, "The region size requested by the command");
                    }
                    catch (...)
                    {
                        /// The child answered this request in full and is waiting for the next one,
                        /// so a region that cannot grow that far (a memory limit, the configured
                        /// cap) leaves it at a clean protocol boundary, like a failure while the
                        /// input was being prepared: the pooled worker stays reusable.
                        command_can_be_reused = true;
                        throw;
                    }
                    continue;
                }

                if (status != SHARED_MEMORY_STATUS_OK)
                {
                    String message;
                    /// Cap the error message so a buggy or malicious command cannot force a huge
                    /// allocation on the failure path.
                    readStringBinary(message, *timeout_command_out, SHARED_MEMORY_MAX_ERROR_MESSAGE_SIZE);

                    /// The response was read in full, so the command is back to waiting for the next
                    /// request: this is the command reporting that it cannot process this input, not
                    /// the command misbehaving, and a pooled worker survives it. (A truncated
                    /// message would have thrown out of the read above and discarded the worker.)
                    command_can_be_reused = true;
                    throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
                        "Executable UDF reported an error (status {}): {}", status, message);
                }

                readVarUInt(output_offset, *timeout_command_out);
                readVarUInt(output_size, *timeout_command_out);
                break;
            }

            auto & region = *regions[index];

            if (output_offset > region.size() || output_size > region.size() - output_offset)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable UDF returned an out-of-bounds region: offset {}, size {}, region size {}",
                    output_offset, output_size, region.size());

            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryOutputBytes, output_size);

            /// Parsed where it lies. The region is sealed against shrinking, so the bytes the
            /// command just reported are backed by pages that cannot go away under the parser -
            /// there is no copy to make and nothing to check before reading. `output_read_buffer`
            /// points into the mapping, and a growth replaces the mapping, so every caller drops
            /// the buffer before asking for the next chunk.
            output_read_buffer = std::make_unique<ReadBufferFromMemory>(region.data() + output_offset, output_size);
            output_pipeline = QueryPipeline(Pipe(context->getInputFormat(format, *output_read_buffer, *sample_block, configuration.max_block_size)));
            /// Like in pipe mode: the rows the command returns are not rows read by this query, so
            /// they must not be added to SelectedRows/SelectedBytes by the read progress callback.
            output_pipeline.disableProfileEventUpdate();
            output_executor = std::make_unique<PullingPipelineExecutor>(output_pipeline);
        }

        /// Synchronous transport: serialize the next chunk into region 0 and exchange it.
        bool sendNextRequest()
        {
            preparing_input = true;
            auto input_size = serializeInto(0);
            preparing_input = false;

            if (!input_size)
            {
                closeStdinIfNeeded(is_pooled);
                return false;
            }
            exchange(0, *input_size);
            return true;
        }

        /// Pipelined transport: take the next chunk that the background producer has already
        /// serialized into one of the two regions, and exchange it. The buffer stays held until its
        /// output is fully drained in generate(), which then releases it back to the producer.
        bool sendNextRequestPipelined()
        {
            preparing_input = true;
            auto item = producer.next(); /// blocks for the prefetched chunk; rethrows producer errors
            preparing_input = false;

            if (!item)
            {
                closeStdinIfNeeded(is_pooled);
                return false;
            }

            active_index = item->index;
            holding_buffer = true;
            exchange(active_index, item->size);
            return true;
        }

        /// Charge/uncharge the query memory tracker for the mmap'd shared-memory region(s).
        ///
        /// These are synthetic charges: the region is a mapped `memfd`, not a heap allocation at a known
        /// address. We therefore intentionally do NOT emit allocation-profiler samples
        /// (AllocationTrace::onAlloc / onFree) for them — a sample carrying a fake pointer would only
        /// pollute allocation profiles, and in pipelined mode a single free could not honestly name
        /// two separate regions. The memory-tracker counter (used for the memory limit) is still
        /// updated by alloc() / free() regardless. chargeQueryMemory may throw MEMORY_LIMIT_EXCEEDED
        /// before it records the charge, leaving query_memory_charge unchanged.
        ///
        /// In pipelined mode both the query thread (growing the active region in `exchange`) and the
        /// background producer thread (growing the other region in `serializeInto`) charge memory,
        /// so the running total is atomic: a lost update would make the final uncharge in cleanup()
        /// release the wrong amount and permanently skew the memory trackers.
        void chargeQueryMemory(size_t bytes)
        {
            [[maybe_unused]] auto trace = CurrentMemoryTracker::alloc(static_cast<Int64>(bytes));
            try
            {
                /// These synthetic charges are large enough to matter on their own. Do not let them
                /// sit below `max_untracked_memory`, because the region's pages are committed right
                /// after this method returns.
                CurrentThread::flushUntrackedMemory();
                CurrentMemoryTracker::check();
            }
            catch (...)
            {
                [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
                CurrentThread::flushUntrackedMemory();
                throw;
            }

            query_memory_charge.fetch_add(bytes, std::memory_order_relaxed);
        }

        void unchargeQueryMemory(size_t bytes)
        {
            [[maybe_unused]] auto trace = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
            CurrentThread::flushUntrackedMemory();
            query_memory_charge.fetch_sub(bytes, std::memory_order_relaxed);
        }

        /// Ensures the region can hold `required` bytes, growing it (up to shared_memory_max_size)
        /// if needed. Growth doubles the size to amortize repeated growths. `what` names whose
        /// requirement this is, for the exception raised when the region cannot grow that far. The
        /// added bytes are charged to the query memory tracker like the rest of the region; for a
        /// pooled region cleanup() hands that charge over to the holder together with the region.
        void ensureRegionFits(size_t index, size_t required, std::string_view what, bool required_is_lower_bound = false)
        {
            auto & region = *regions[index];
            if (required <= region.size())
                return;

            if (required > shared_memory_max_size)
                throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER,
                    "{} ({}{} bytes) does not fit into the shared-memory region "
                    "({} bytes, maximum {} bytes): increase shared_memory_max_size",
                    what, required_is_lower_bound ? "at least " : "", required, region.size(), shared_memory_max_size);

            /// Double, so that repeated growth stays amortized - the input is serialized straight
            /// into the region and asks for one byte at a time - but never past the cap, and never
            /// less than a caller that knows its exact requirement asked for. Taking the cap
            /// whenever doubling overshoots it would commit (and `posix_fallocate`) the whole
            /// configured maximum for a chunk that needs a little more room.
            size_t new_size = std::max(required, std::min(region.size() * 2, shared_memory_max_size));

            /// Unless memory-limit exceptions are blocked right now. That happens when the growth is
            /// driven from inside someone else's `finalize` - a format's wrapping buffer
            /// (`WriteBufferValidUTF8`, `PeekableWriteBuffer`) flushing what it still holds into this
            /// region - because `WriteBuffer::finalize` blocks them for its whole body. The charge
            /// below then cannot fail, so `max_memory_usage` is not enforced for these bytes, and a
            /// doubling would put a whole region size past the limit. Take a modest step instead:
            /// what escapes the limit is then bounded by what the writer is actually flushing.
            if (LockMemoryExceptionInThread::isBlocked(VariableContext::Process, /*fault_injection=*/ false))
                new_size = std::max(required, std::min(region.size() + UNENFORCED_GROWTH_STEP, shared_memory_max_size));

            /// What this growth will commit. Measured against the committed size, not the mapped
            /// one: an earlier growth may have committed its pages and then failed to map them, in
            /// which case the query is already charged for them and this growth only maps.
            const size_t backing_before = region.backingSize();
            const size_t added = new_size > backing_before ? new_size - backing_before : 0;

            /// Charge first (may throw MEMORY_LIMIT_EXCEEDED), then grow. If the growth fails, roll
            /// back exactly the part that was not committed: `posix_fallocate` undoes itself, but
            /// a remap that fails after it leaves the pages committed and the file - sealed against
            /// shrinking - permanently larger. Those pages stay charged, here and on every later
            /// borrow, because they are what the region costs from now on.
            chargeQueryMemory(added);

            try
            {
                if (command_holder)
                    command_holder->growSharedMemory(index, new_size);
                else
                    region.grow(new_size);
            }
            catch (...)
            {
                const size_t committed = region.backingSize() - backing_before;
                if (added > committed)
                    unchargeQueryMemory(added - committed);
                if (committed)
                    ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, committed);
                throw;
            }

            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryRegionGrowths);
            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, added);
        }

        /// Takes anything a previous borrow's command left on its stderr off the pipe, without
        /// putting it through `stderr_reaction`.
        ///
        /// The probe that decides whether a worker may be pooled is one instant, so a command that
        /// writes a moment after answering slips past it and its bytes are sitting there when the
        /// next query borrows the process. Those bytes belong to the query that caused them, and
        /// that query is over; running them through the reaction here would fail *this* query for
        /// something it did not do, which under `stderr_reaction` `throw` is the difference between
        /// a confusing failure and a wrong accusation. They are logged instead, so they are not
        /// lost, and the query that borrows the worker is left alone.
        void discardStderrLeftByAPreviousBorrow()
        {
            if (!is_pooled)
                return;

            static constexpr size_t stderr_drain_budget_ms = 100;

            try
            {
                const String leftover = timeout_command_out->consumePendingStderrWithoutReaction();

                /// The report above is capped; the pipe must not be. A worker that filled it is
                /// blocked in `write` and will not read this borrow's request until there is room.
                /// What is left beyond the cap, and what the command writes while this drains, is
                /// the same earlier query's and is dropped without the reaction for the same reason.
                timeout_command_out->drainStderrFully(stderr_drain_budget_ms, /*with_reaction=*/ false);
                if (!leftover.empty())
                    LOG_WARNING(
                        getLogger("ShellCommandSharedMemorySource"),
                        "The process of an executable UDF had unread output on its stderr when it was borrowed, "
                        "so it was written after the response of an earlier invocation. It is reported here "
                        "rather than against this query, which did not cause it. Stderr: {}",
                        leftover);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        /// Whether the pooled worker process survives this borrow and goes back to the pool together
        /// with its holder. Only a process left at a known protocol boundary does: one that never
        /// saw a request from this borrow, or one that answered every request in full. A protocol
        /// failure, a dead child and an invocation cut short (query cancellation, an exception
        /// downstream) all leave its state unknown, so it is discarded instead - which also means
        /// its stdin must be closed and its shared-memory regions released.
        ///
        /// Only meaningful once the source is being torn down: while it is still running, a pooled
        /// worker that has not produced all its rows yet is not being discarded.
        /// A pooled worker may go back only while its control channel is at a protocol boundary.
        /// `exchange` reads exactly the control frame of a response - the status, and then either
        /// the output location, the size the command wants, or an error message - and nothing makes
        /// the command stop writing to stdout afterwards. A stray byte left there costs this query
        /// nothing, because its answer has already been read out of the shared-memory region; the
        /// next borrow is the one that reads that byte as the status varint of the response to its
        /// own first request, and fails in a way that looks nothing like the command that caused it.
        ///
        /// Stderr carries the same hazard, quietly rather than fatally, and a hangup on stdout says
        /// the child is gone; `TimeoutReadBufferFromFileDescriptor::ChannelState` spells all three
        /// out. Any of them means the worker is not at a boundary and has to be reaped instead.
        ///
        /// The probe cannot be exhaustive - a command that writes its stray byte later still slips
        /// through - but it catches the case that actually happens, a command that emits it together
        /// with its response.
        /// Not `const`: under `stderr_reaction` `none` it also empties the worker's stderr pipe (see
        /// below), which is a change to the worker's state, not just a look at it.
        bool controlChannelIsClean() noexcept
        {
            /// Latched, because the answer is asked for more than once - `prepare` decides with it
            /// and `cleanup` decides again - and the evidence does not survive being looked at.
            /// Reporting the discard drains the leftover stderr, and closing the discarded child's
            /// stdin makes it exit; a second, unlatched probe would then find two clean pipes and
            /// hand back a worker whose stdin is already closed, and the next query to borrow it
            /// would fail writing its first request. Deciding twice is exactly what the two callers
            /// avoid internally - this is what makes them agree with each other as well.
            if (channel_was_dirty)
                return false;

            /// Once the child has been reaped its pipes are closed and the descriptor numbers may
            /// have been recycled, so there is nothing safe to poll here - and a reaped child is
            /// not going back to the pool anyway.
            if (!command || command->isWaitCalled())
                return false;

            /// The constructor builds the process before it wraps the process's pipes, and the
            /// wrapping can fail (an allocation past the memory limit, a descriptor that cannot be
            /// made non-blocking). The constructor's own cleanup then asks this question with no
            /// reader to ask it through. A worker whose pipes could not even be looked at is not
            /// handed on - the same rule as a probe that fails below.
            if (!timeout_command_out)
                return false;

            /// Nothing is done with what a `none` command writes, but it cannot be left on the pipe
            /// either: those bytes survive into the next borrow, accumulate, and once the pipe is
            /// full the command blocks in `write` without ever reading the next request. `none`
            /// promises a chatty command does not block; this is where that promise is kept for a
            /// worker that is about to be handed on. The other reactions need nothing here: for
            /// them pending stderr already means the worker is not at a boundary, and the discard
            /// path drains it.
            /// The drain polls and reads; either can fail, and this function may not throw. A
            /// worker whose pipes could not even be probed is not one to hand on: it is treated as
            /// dirty and discarded.
            try
            {
                if (!timeout_command_out->stderrIsObserved())
                {
                    static constexpr size_t stderr_drain_budget_ms = 100;
                    timeout_command_out->drainStderrFully(stderr_drain_budget_ms);
                }
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource", "Cannot probe the pipes of a pooled command; discarding its process");
                channel_was_dirty = true;
                return false;
            }

            const auto state = timeout_command_out->channelState();
            if (state.isClean())
                return true;

            /// Remember what was wrong, so that the discard can be reported for what it is. It is
            /// otherwise completely invisible: the query it happens in succeeds, and the next one
            /// simply gets a new process - so a command that writes to stderr after answering would
            /// quietly turn its `executable_pool` into a process per call.
            channel_was_dirty = true;
            dirty_stdout = state.stdout_has_unread_output;
            dirty_stderr = state.stderr_has_unread_output;
            child_is_gone = state.stdout_hung_up && !state.stdout_has_unread_output;
            return false;
        }

        /// Reports a discarded worker, once per borrow. Leftover output is the command's mistake and
        /// is reported as such, together with whatever it left on stderr - this is the only place
        /// that output is ever going to be seen, since nothing else reads a discarded worker's pipes
        /// before they are closed. A child that simply exited is not a mistake and must not be
        /// described as one: it gets its own line and is not counted as a protocol violation.
        void reportDirtyChannelDiscard() noexcept
        {
            /// Taken and cleared up front, so that nothing below can leave a discard to be reported
            /// a second time - and so that everything that follows is inside the handler. Every
            /// line here allocates, this runs on `prepare`'s path where the stack is not unwinding,
            /// and the function is `noexcept`: a `MEMORY_LIMIT_EXCEEDED` from formatting a log
            /// message would otherwise terminate the server over a diagnostic.
            const bool had_dirty_stdout = std::exchange(dirty_stdout, false);
            const bool had_dirty_stderr = std::exchange(dirty_stderr, false);
            const bool had_child_gone = std::exchange(child_is_gone, false);

            try
            {
                if (!had_dirty_stdout && !had_dirty_stderr)
                {
                    if (had_child_gone)
                        LOG_DEBUG(
                            getLogger("ShellCommandSharedMemorySource"),
                            "The process of an executable UDF exited after answering, so it was not returned "
                            "to the pool.");
                    return;
                }

                ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryDirtyChannelDiscards);

                const String leftover_stderr = timeout_command_out->consumePendingStderr();
                LOG_WARNING(
                    getLogger("ShellCommandSharedMemorySource"),
                    "Executable UDF left unread output on its {} after answering, so its pooled process "
                    "was discarded instead of reused. The command must not write anything past its "
                    "response frame, and must write diagnostics before the response rather than after "
                    "it.{}{}",
                    had_dirty_stdout && had_dirty_stderr ? "stdout and stderr" : (had_dirty_stdout ? "stdout" : "stderr"),
                    leftover_stderr.empty() ? "" : " Stderr: ",
                    leftover_stderr);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        /// Not `const` for the same reason as `controlChannelIsClean`, which it asks.
        bool commandIsReused()
        {
            return is_pooled
                && command != nullptr
                && !command_is_invalid
                && (command_can_be_reused
                    || (configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read))
                && controlChannelIsClean();
        }


        /// Closes the command's stdin, so that the child exits when it sees EOF. `command_is_reused`
        /// tells that the process is going back to the pool for another borrow: only then does the
        /// descriptor have to stay open. Every other process - a non-pooled one, or a pooled one
        /// that is being discarded - has to be closed here, because the waits that follow are the
        /// ones that do NOT close it themselves: the sampler's `tryWaitWithoutStatusCheck` polls
        /// for the whole command_termination_timeout, and `prepare` may call the blocking `wait`,
        /// which reaps the child before closing any pipe and would never return for a child that
        /// is waiting for its next request. (`~ShellCommand` does close the pipes before waiting,
        /// so it is not what this protects against.)
        void closeStdinIfNeeded(bool command_is_reused)
        {
            if (command_is_reused || stdin_closed || !command)
                return;

            stdin_closed = true;

            /// The constructor can fail before the write buffer exists - while creating a region or
            /// charging its memory. The child is already running and blocked in read by then, so
            /// its stdin still has to be closed: otherwise the wait in cleanup() and in
            /// ~ShellCommand blocks for the whole command_termination_timeout before the child is
            /// signalled.
            if (!timeout_command_in)
            {
                command->in.close();
                return;
            }

            try
            {
                timeout_command_in->finalize();
                (*timeout_command_in).reset();
            }
            catch (...)
            {
                /// The child exits when it sees EOF on its stdin, so the descriptor has to be closed
                /// even when finalizing the buffer failed: otherwise the child stays blocked in
                /// read and the wait for it never returns.
                timeout_command_in->cancel();
                command->in.close();
                throw;
            }

            command->in.close();
        }

        /// Reads the borrow's CPU and peak resident set out of `/proc` while the worker is still
        /// there to be read. Idempotent (see `UDFProcessSubtreeSampler::recordReleased`), so both
        /// the teardown that discards a worker and the ordinary end of a borrow call it.
        ///
        /// Never throws: it reads procfs and builds containers, so a memory limit can refuse it,
        /// and it runs both from `cleanup` - which the destructor calls - and from `prepare`, where
        /// failing a query that has already produced its rows over a profiling read would be worse
        /// than losing the measurement.
        void recordPooledResourceUsageNoThrow() noexcept
        {
            if (!configuration.sampler || !process_pool)
                return;

            try
            {
                configuration.sampler->recordReleased();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        /// Same, for the teardown paths (cancellation, cleanup) where an exception must not escape.
        void closeStdinNoThrow(bool command_is_reused)
        {
            try
            {
                closeStdinIfNeeded(command_is_reused);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        void cleanup()
        {
            /// Stop and join the background producer before touching any state it shares (the input
            /// pipeline, the serialize buffer and the regions). Idempotent and a no-op if it was
            /// never started (synchronous mode or a failure early in the constructor).
            producer.stop();

            /// Tear down the output pipeline first. Its parsing threads (input_format_parallel_parsing)
            /// read straight out of the shared-memory region through output_read_buffer, so they must be
            /// joined before the child is reaped and before the regions are unmapped below. generate()
            /// does this in order on the normal path; here it also covers the destructor path (query
            /// cancellation, an exception downstream) where the pipeline is still alive.
            output_executor.reset();
            {
                /// Moved into a temporary rather than assigned an empty pipeline: assignment would
                /// construct one, and that allocates, on a path that runs from a destructor.
                QueryPipeline discarded = std::move(output_pipeline);
            }
            output_read_buffer.reset();

            /// The reuse decision, made once and used for everything below.
            bool keep_command = commandIsReused();

            /// A process and its regions live and die together: the process reached them by
            /// inheriting their descriptors at `exec`, and a region created for it cannot be
            /// swapped for another one behind its back - it would go on opening the descriptor it
            /// was given. So a borrow that failed after creating regions does not keep the process
            /// either. It is a fresh one in that case (a returned process comes with its regions
            /// already there), so nothing of value is lost.
            bool regions_created_by_this_borrow_exist = false;
            for (bool created : regions_created_by_this_borrow)
                regions_created_by_this_borrow_exist |= created;

            if (!constructor_finished && regions_created_by_this_borrow_exist)
                keep_command = false;

            /// The converse of the same rule. A borrow that failed before it took the worker out of
            /// the holder - the constructor charging the worker's regions to a query that is at its
            /// memory limit, say - touched neither the process nor its regions: no request reached
            /// it, and it is still sitting in the holder with the very descriptors it was started
            /// with. `keep_command` is false here only because there is no `command` in this
            /// object to keep. Its regions have to stay with it all the same: dropping them and
            /// keeping the process would have the next borrow create new regions that the worker
            /// has never heard of, and read its answers out of memory the worker never writes to.
            const bool worker_untouched = command == nullptr
                && command_holder && command_holder->hasReturnedCommand()
                && !regions_created_by_this_borrow_exist;

            reportDirtyChannelDiscard();


            /// Before the stdin close below, for the same reason `prepare` samples before its own:
            /// a discarded child exits on that EOF, and a zombie has no `/proc` mm fields left to
            /// read. Idempotent, so a borrow that already went through `prepare` is unaffected.
            recordPooledResourceUsageNoThrow();

            /// A child that is not going back to the pool exits on stdin EOF, so its stdin must be
            /// closed here as well: generate() closes it on the normal path, but not when the source
            /// is torn down before that (query cancellation, an exception downstream), and never for
            /// a pooled worker, which only turns out to be discarded at this point. A child left
            /// blocked in read(stdin) would make the sampler's wait below spin for the whole
            /// command_termination_timeout.
            closeStdinNoThrow(keep_command);

            /// The write buffer must be finalized (or canceled) before it is destroyed. On the pool
            /// path stdin stays open for reuse, so it was not finalized while sending requests.
            if (timeout_command_in && !timeout_command_in->isFinalized() && !timeout_command_in->isCanceled())
            {
                try
                {
                    timeout_command_in->finalize();
                    (*timeout_command_in).reset();
                }
                catch (...)
                {
                    timeout_command_in->cancel();
                    tryLogCurrentException("ShellCommandSharedMemorySource");
                }
            }

            /// Mirrors ShellCommandSource::cleanup: record resource usage for this borrow before the
            /// child is torn down, then hand the process back to the pool.
            if (configuration.sampler)
            {
                /// The pool path was measured above, before the stdin close.
                if (!process_pool && command)
                {
                    if (!command->isWaitCalled())
                    {
                        try
                        {
                            command->tryWaitWithoutStatusCheck();
                        }
                        catch (...)
                        {
                            tryLogCurrentException("ShellCommandSharedMemorySource");
                        }
                    }

                    configuration.sampler->recordExecutableElapsed();

                    if (command->wasChildResourceUsageCaptured())
                        configuration.sampler->recordExecutableFinished(
                            command->getChildUserTimeMicroseconds(),
                            command->getChildSystemTimeMicroseconds());
                }
            }

            if (command_is_invalid)
                command = nullptr;

            if (command_holder)
            {
                if (!keep_command && !worker_untouched)
                {
                    /// The worker process is being discarded (protocol failure, child death,
                    /// overproduction, cancellation, etc.). Its pooled shared-memory regions belong
                    /// to that process, so release all of them, including any created by an earlier
                    /// borrow, instead of leaving the regions and their persistent memory charge
                    /// pinned on the reused holder for a replacement process - which could not use
                    /// them anyway, since it is the process that inherits a region's descriptor at
                    /// `exec`, and a replacement gets its own. resetSharedMemory drops the holder's
                    /// reference (the region dies with the last one, below) and uncharges memory.
                    for (size_t i = 0; i < regions.size(); ++i)
                    {
                        regions[i].reset();
                        command_holder->resetSharedMemory(i);
                        regions_created_by_this_borrow[i] = false;
                    }
                }
                else
                {
                    /// The regions stay with the worker (kept, or never taken out), at whatever size
                    /// this borrow grew them to. They are sealed against shrinking, so there is no
                    /// trimming them back to `shared_memory_size` for the idle time;
                    /// `shared_memory_max_size` is what a pooled worker may hold, and the holder
                    /// charges the server for exactly that.
                }
            }

            /// Release the per-borrow memory charge on the query thread. The producer thread is
            /// joined above, so this total is final.
            if (size_t charge = query_memory_charge.load(std::memory_order_relaxed))
                unchargeQueryMemory(charge);

            /// Whatever regions the holder still owns outlive this borrow, so they are charged
            /// again - globally this time - now that the borrow's charge is gone. There is no way
            /// to move a charge between trackers atomically, so one of the two orders has to be
            /// picked: this one leaves the bytes uncounted for the moment in between, the other
            /// would count them twice. Undercounting for a moment can at most let a concurrent
            /// allocation through (memory limits are approximate anyway - see
            /// `max_untracked_memory`), while double counting could fail a query that fits and
            /// would inflate the peak the server reports. The borrow side of the hand-over
            /// (`releaseChargeToBorrower`) errs the same way, for the same reason.
            if (command_holder)
                command_holder->acquireChargeFromBorrower();

            if (command_holder && process_pool)
            {
                if (keep_command)
                    command_holder->returnCommand(std::move(command));

                /// A worker that is not going back to the pool has to die before its slot does: the
                /// query waiting for that slot starts a replacement at once, so leaving this process
                /// to be destroyed later - with a `command_termination_timeout` wait in front of it -
                /// lets the pool run over `pool_size` for as long as that takes. Its stdin was closed
                /// above, so the child is already on its way out.
                command = nullptr;

                process_pool->returnObject(std::move(command_holder));
            }
        }

        ContextPtr context;
        std::string format;
        SharedHeader sample_block;
        Block input_header;

        ShellCommandSourceConfiguration configuration;

        /// regions[0] is used by both transports; regions[1] is the second double-buffer used only
        /// in pipelined mode. active_index is the buffer the consumer currently exchanges/reads.
        std::array<SharedMemoryRegionPtr, 2> regions;
        std::array<bool, 2> regions_created_by_this_borrow{};
        size_t active_index = 0;
        bool holding_buffer = false;
        bool constructor_finished = false;

        /// Set while the input for the next request is being serialized, before that request is
        /// sent to the child - see the catch-all in generate(). Only ever read on the query thread.
        bool preparing_input = false;

        /// Set when input preparation fails before a request reaches a pooled child. Unlike an
        /// incomplete or cancelled invocation, this leaves the child at a known protocol boundary.
        bool command_can_be_reused = false;

        bool is_pooled;
        size_t shared_memory_max_size;
        bool pipeline_mode;
        std::atomic<size_t> query_memory_charge = 0;

        std::unique_ptr<TimeoutReadBufferFromFileDescriptor> timeout_command_out;
        std::unique_ptr<TimeoutWriteBufferFromFileDescriptor> timeout_command_in;

        size_t current_read_rows = 0;
        bool stdin_closed = false;

        /// Set by `controlChannelIsClean`, which is const because it only answers a question;
        /// reported and cleared by `reportDirtyChannelDiscard`. `channel_was_dirty` is the latch and
        /// is never cleared - see `controlChannelIsClean` for why the answer must not be re-derived.
        mutable bool channel_was_dirty = false;
        mutable bool dirty_stdout = false;
        mutable bool dirty_stderr = false;
        mutable bool child_is_gone = false;

        std::shared_ptr<ProcessPool> process_pool;

        bool check_exit_code = false;

        QueryPipeline input_pipeline;
        std::unique_ptr<PullingPipelineExecutor> input_executor;


        /// output_read_buffer points into the region's mapping and is read by the output pipeline
        /// (including its parallel-parsing threads), so it must outlive the pipeline:
        /// declared first, therefore destroyed last. cleanup() tears all three down in order.
        std::unique_ptr<ReadBufferFromMemory> output_read_buffer;
        QueryPipeline output_pipeline;
        std::unique_ptr<PullingPipelineExecutor> output_executor;

        std::atomic<bool> command_is_invalid {false};


        /// Background prefetcher for pipelined mode. Its thread reads the input pipeline and
        /// serializes into the regions, so it is declared after both and is therefore destroyed
        /// before them; its destructor stops and joins the thread. The two members below are
        /// destroyed before this one, but they are not what the thread touches, and `cleanup` has
        /// joined it long before any destructor runs anyway.
        DoubleBufferedProducer producer;

        /// The worker process and its pool holder are taken over after EVERY other member, because
        /// every other member has to be able to throw without costing a healthy pooled worker: until
        /// this object owns these two they still belong to the caller, whose scope guard hands them
        /// back to the pool. `timeout_command_out` allocates its buffer, and both `QueryPipeline`
        /// members allocate in their default constructor, so this is not a theoretical ordering.
        ///
        /// Being last also makes them the first members destroyed, which is safe: `cleanup` runs
        /// before any of that and has already stopped the producer, torn the pipelines down and
        /// handed the command back, and ~TimeoutReadBufferFromFileDescriptor deliberately does not
        /// touch the descriptors the command owns.
        std::unique_ptr<ShellCommand> command;
        ShellCommandHolderPtr command_holder;
    };

}

void checkSharedMemoryIsNotConfigured(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & surface)
{
    for (const auto & shared_memory_key : SHARED_MEMORY_CONFIGURATION_KEYS)
    {
        if (config.has(config_prefix + "." + std::string(shared_memory_key)))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "{}: `{}` is not supported here - the shared-memory transport is available for executable "
                "user defined functions only",
                surface, shared_memory_key);
    }
}

ShellCommandSourceCoordinator::ShellCommandSourceCoordinator(const Configuration & configuration_)
    : configuration(configuration_)
{
    if (configuration.is_executable_pool)
        process_pool = std::make_shared<ProcessPool>(configuration.pool_size ? configuration.pool_size : std::numeric_limits<size_t>::max());
}

Pipe ShellCommandSourceCoordinator::createPipe(
    const std::string & command,
    const VectorWithMemoryTracking<std::string> & arguments,
    std::vector<Pipe> && input_pipes,
    Block sample_block,
    ContextPtr context,
    const ShellCommandSourceConfiguration & source_configuration)
{
    if (configuration.use_shared_memory && input_pipes.size() != 1)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Shared-memory mode supports exactly one input pipe, got {}", input_pipes.size());

    ShellCommand::Config command_config(command);
    command_config.arguments = arguments;
    command_config.pipe_capacity = configuration.command_pipe_capacity;
    for (size_t i = 1; i < input_pipes.size(); ++i)
        command_config.write_fds.emplace_back(i + 2);

    std::unique_ptr<ShellCommand> process;
    std::unique_ptr<ShellCommandHolder> process_holder;

    /// Whether the process below has already served a borrow, and may therefore have left something
    /// on its pipes - see `ShellCommandSource::quarantineReusedWorker`.
    bool worker_is_reused = false;

    /// A borrowed holder is handed over to the source below, which returns it to the pool when the
    /// query is done. Until that hand-over happens the holder is only this local, and anything that
    /// throws in between - building the command, or a member initializer of the source, which runs
    /// before the source's own constructor cleanup can take over - would destroy it without
    /// returning it. `BorrowedObjectPool` never decrements what it has allocated, so each such loss
    /// permanently costs the pool one slot, and after `pool_size` of them every call fails with
    /// "Could not get process from pool". The guard fires only while the local still owns the
    /// holder: on the normal path the source has taken it and this is a no-op.
    SCOPE_EXIT_SAFE({
        if (process_holder)
        {
            /// Hand the worker back to its holder as well when the source never took it: nothing
            /// was sent to it, so it is still at a clean protocol boundary, and killing it would
            /// cost the next query a process spawn over a failure that never reached this one.
            if (process)
                process_holder->returnCommand(std::move(process));
            process_pool->returnObject(std::move(process_holder));
        }
    });

    auto destructor_strategy = ShellCommand::DestructorStrategy{true /*terminate_in_destructor*/, SIGTERM, configuration.command_termination_timeout_seconds};
    command_config.terminate_in_destructor_strategy = destructor_strategy;

    command_config.register_in_udf_process_registry = configuration.is_user_defined_function;

    bool is_executable_pool = (process_pool != nullptr);

    /// How a process is started. It is given the descriptors the child has to inherit - the
    /// shared-memory regions, if any - so that the same builder serves both transports: the pipe
    /// transport passes none.
    const bool execute_direct = configuration.execute_direct;
    command_config.collect_resource_usage = !is_executable_pool && source_configuration.sampler != nullptr;
    ShellCommandHolder::ShellCommandBuilderFunc build_process
        = [command_config, execute_direct](const std::vector<std::pair<int, int>> & inherited_fds) mutable
    {
        command_config.inherited_fds = inherited_fds;
        if (execute_direct)
            return ShellCommand::executeDirect(command_config);
        return ShellCommand::execute(command_config);
    };

    if (is_executable_pool)
    {
        bool result = process_pool->tryBorrowObject(
            process_holder,
            [build_process]() { return std::make_unique<ShellCommandHolder>(ShellCommandHolder::ShellCommandBuilderFunc(build_process)); },
            configuration.max_command_execution_time_seconds * 1000);

        /// Pool wait is frozen here on both the success and the timeout-failure
        /// paths so that `PoolWaitMicroseconds` always records contention for a
        /// slot. Any time spent below in `buildCommand` (cold spawn) lands in
        /// `ElapsedMicroseconds` instead.
        if (source_configuration.sampler)
            source_configuration.sampler->recordPoolWaitDone();

        if (!result)
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Could not get process from pool, max command execution timeout exceeded {} seconds",
                configuration.max_command_execution_time_seconds);
    }

    if (configuration.use_shared_memory)
    {
        /// The regions and the process are both created inside the source, in that order: the
        /// process inherits the regions' descriptors at `exec`, so they have to exist first. Doing
        /// it there also means a failure anywhere along the way - reserving a region, charging its
        /// memory, starting the process - is handled by the source's constructor cleanup, which
        /// returns the borrowed holder to the pool instead of permanently shrinking its capacity.
        auto source = std::make_unique<ShellCommandSharedMemorySource>(
            context,
            configuration.format,
            configuration.command_read_timeout_milliseconds,
            configuration.command_write_timeout_milliseconds,
            configuration.stderr_reaction,
            configuration.check_exit_code,
            std::make_shared<const Block>(std::move(sample_block)),
            std::move(build_process),
            std::move(input_pipes[0]),
            configuration.shared_memory_size,
            configuration.shared_memory_max_size,
            configuration.shared_memory_pipeline,
            is_executable_pool,
            source_configuration,
            std::move(process_holder),
            process_pool);

        return Pipe(std::move(source));
    }

    if (is_executable_pool)
    {
        /// Asked before building, because building is what consumes the stored process.
        worker_is_reused = process_holder->hasReturnedCommand();
        process = process_holder->buildCommand();

        /// A worker that exited while it sat in the pool is replaced here, before anything is built
        /// on it. This is the last point at which a replacement is still possible: once the source
        /// owns the process, the only thing it can do about a dead one is fail the query that
        /// happened to borrow it - and that query would fail obscurely, on its first write to a
        /// closed stdin, for something that happened before it started. Only a hung-up stdout with
        /// nothing left on it counts: a dead worker that also left bytes behind is the stale-output
        /// case, and that one has to fail loudly rather than be quietly replaced, because the
        /// bytes are what matters (see `ShellCommandSource::quarantineReusedWorker`).
        if (worker_is_reused && pooledProcessHasExitedCleanly(*process))
        {
            LOG_DEBUG(
                getLogger("ShellCommandSource"),
                "The process of a pooled command (pid {}) exited while it was idle in the pool; starting a "
                "replacement for this borrow.",
                process->getPid());

            process.reset();
            process = process_holder->buildCommand();
            worker_is_reused = false;
        }

        /// Borrow acquired: capture pid for procfs sampling. The pre-snapshot
        /// runs here so `clear_refs` and the utime/stime baseline cover only
        /// the work attributable to this borrow.
        ///
        /// `recordPidAcquired` allocates (vector return from `walkSubtree`,
        /// `unordered_set` and `unordered_map` inserts) and is not noexcept.
        /// Sampling is best-effort, so a failure here must not fail the query
        /// that is otherwise ready to run: swallow it and drop one pre baseline.
        /// (The borrowed holder itself is safe either way - see the scope guard
        /// above, which returns it to the pool on any throw from here on.)
        if (source_configuration.sampler)
        {
            try
            {
                source_configuration.sampler->recordPidAcquired(process->getPid());
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource");
            }
        }
    }
    else
    {
        process = build_process({});

        /// Record the child pid so sampleExecutablePeak can walk the subtree
        /// during IO. No-op when sampler is null.
        if (source_configuration.sampler)
            source_configuration.sampler->recordExecutablePid(process->getPid());
    }

    std::vector<ShellCommandSource::SendDataTask> tasks;
    tasks.reserve(input_pipes.size());

    for (size_t i = 0; i < input_pipes.size(); ++i)
    {
        WriteBufferFromFile * write_buffer = nullptr;

        if (i == 0)
        {
            write_buffer = &process->in;
        }
        else
        {
            int descriptor = static_cast<int>(i) + 2;
            auto it = process->write_fds.find(descriptor);
            if (it == process->write_fds.end())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Process does not contain descriptor to write {}", descriptor);

            write_buffer = &it->second;
        }

        int write_buffer_fd = write_buffer->getFD();
        /// Only the primary stdin pipe (i == 0) contributes to InputBytes.
        /// Additional write descriptors carry side-channel data that isn't
        /// part of the UDF's observable input.
        UDFProcessSubtreeSampler * write_sampler = (i == 0) ? source_configuration.sampler.get() : nullptr;
        auto timeout_write_buffer
            = std::make_shared<TimeoutWriteBufferFromFileDescriptor>(write_buffer_fd, configuration.command_write_timeout_milliseconds, write_sampler);

        input_pipes[i].resize(1);

        auto out = context->getOutputFormat(configuration.format, *timeout_write_buffer, materializeBlock(input_pipes[i].getHeader()));
        out->setAutoFlush();

        if (configuration.send_chunk_header)
        {
            /// We cannot use timeout_write_buffer directly since the output format may wrap the buffer, so we need to use a wrapper
            auto transform = std::make_shared<SendingChunkHeaderTransform>(input_pipes[i].getSharedHeader(), *out->getWriteBufferPtr());
            input_pipes[i].addTransform(std::move(transform));
        }

        auto num_streams = input_pipes[i].maxParallelStreams();
        auto pipeline = std::make_shared<QueryPipeline>(std::move(input_pipes[i]));
        pipeline->setNumThreads(num_streams);
        pipeline->complete(std::move(out));

        ShellCommandSource::SendDataTask task = [pipeline, timeout_write_buffer, write_buffer, is_executable_pool]()
        {
            CompletedPipelineExecutor executor(*pipeline);
            executor.execute();

            timeout_write_buffer->finalize();
            (*timeout_write_buffer).reset();

            if (!is_executable_pool)
            {
                write_buffer->close();
            }
        };

        tasks.emplace_back(std::move(task));
    }

    auto source = std::make_unique<ShellCommandSource>(
        context,
        configuration.format,
        configuration.command_read_timeout_milliseconds,
        configuration.stderr_reaction,
        configuration.check_exit_code,
        std::make_shared<const Block>(std::move(sample_block)),
        std::move(process),
        std::move(tasks),
        source_configuration,
        std::move(process_holder),
        process_pool,
        worker_is_reused);

    return Pipe(std::move(source));
}

}
