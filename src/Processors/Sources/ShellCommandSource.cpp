#include <Processors/Sources/ShellCommandSource.h>

#include <poll.h>

#include <Common/CurrentMemoryTracker.h>
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
#include <Core/Field.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>

#include <boost/circular_buffer.hpp>
#include <fmt/ranges.h>

#include <csignal>
#include <ranges>


namespace ProfileEvents
{
    extern const Event ExecutableUDFSharedMemoryCalls;
    extern const Event ExecutableUDFSharedMemoryInputBytes;
    extern const Event ExecutableUDFSharedMemoryOutputBytes;
    extern const Event ExecutableUDFSharedMemoryRegionGrowths;
    extern const Event ExecutableUDFSharedMemoryAllocatedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_POLL;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int UDF_EXECUTION_FAILED;
    extern const int LOGICAL_ERROR;
}

/// Version of the shared-memory control protocol between the server and an executable UDF.
/// Sent as the first varint of every request so the child can detect an incompatible protocol
/// version and answer with an error status.
static constexpr UInt64 SHARED_MEMORY_PROTOCOL_VERSION = 1;

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
    {
        makeFdNonBlocking(stdout_fd);
        makeFdNonBlocking(stderr_fd);

        pfds[0].fd = stdout_fd;
        pfds[0].events = POLLIN;
        pfds[1].fd = stderr_fd;
        pfds[1].events = POLLIN;

        if (stderr_reaction == ExternalCommandStderrReaction::NONE)
            num_pfds = 1;
        else
            num_pfds = 2;
    }

    bool nextImpl() override
    {
        size_t bytes_read = 0;

        while (!bytes_read)
        {
            pfds[0].revents = 0;
            pfds[1].revents = 0;
            int num_events = pollWithTimeout(pfds, num_pfds, timeout_milliseconds);
            if (num_events <= 0)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe read timeout exceeded {} milliseconds", timeout_milliseconds);

            bool has_stdout = pfds[0].revents > 0;
            bool has_stderr = pfds[1].revents > 0;

            if (has_stderr)
            {
                if (stderr_read_buf == nullptr)
                    stderr_read_buf.reset(new char[BUFFER_SIZE]);
                ssize_t res = ::read(stderr_fd, stderr_read_buf.get(), BUFFER_SIZE);
                if (res > 0)
                {
                    std::string_view str(stderr_read_buf.get(), res);
                    if (stderr_reaction == ExternalCommandStderrReaction::THROW)
                    {
                        /// Accumulate stderr up to safety limit
                        size_t current_size = stderr_full_output ? stderr_full_output->size() : 0;
                        if (current_size < MAX_STDERR_SIZE)
                        {
                            if (!stderr_full_output)
                                stderr_full_output.emplace();
                            size_t bytes_to_append = std::min(static_cast<size_t>(res), MAX_STDERR_SIZE - current_size);
                            stderr_full_output->append(str.begin(), str.begin() + bytes_to_append);
                        }
                    }
                    else if (stderr_reaction == ExternalCommandStderrReaction::LOG)
                    {
                        LOG_WARNING(getLogger("TimeoutReadBufferFromFileDescriptor"), "Executable generates stderr: {}", str);
                    }
                    else if (stderr_reaction == ExternalCommandStderrReaction::LOG_FIRST)
                    {
                        res = std::min(ssize_t(stderr_result_buf.reserve()), res);
                        if (res > 0)
                            stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.begin() + res);
                    }
                    else if (stderr_reaction == ExternalCommandStderrReaction::LOG_LAST)
                    {
                        stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.begin() + res);
                    }
                }
            }

            if (has_stdout)
            {
                ssize_t res = ::read(stdout_fd, internal_buffer.begin(), internal_buffer.size());

                if (-1 == res && errno != EINTR)
                    throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from pipe");

                if (res == 0)
                {
                    /// EOF on stdout - drain remaining stderr before returning
                    if (stderr_reaction != ExternalCommandStderrReaction::NONE
                        && stderr_reaction != ExternalCommandStderrReaction::LOG)
                    {
                        static constexpr int STDERR_DRAIN_TIMEOUT_MS = 100;  /// Short timeout for remaining stderr after stdout EOF

                        while (true)
                        {
                            pfds[1].revents = 0;
                            int stderr_events = pollWithTimeout(&pfds[1], 1, STDERR_DRAIN_TIMEOUT_MS);
                            if (stderr_events <= 0)
                                break;

                            if (pfds[1].revents <= 0)
                                break;

                            if (stderr_read_buf == nullptr)
                                stderr_read_buf.reset(new char[BUFFER_SIZE]);
                            ssize_t stderr_res = ::read(stderr_fd, stderr_read_buf.get(), BUFFER_SIZE);
                            if (stderr_res <= 0)
                                break;

                            std::string_view str(stderr_read_buf.get(), stderr_res);
                            if (stderr_reaction == ExternalCommandStderrReaction::THROW)
                            {
                                size_t current_size = stderr_full_output ? stderr_full_output->size() : 0;
                                if (current_size >= MAX_STDERR_SIZE)
                                    break;
                                if (!stderr_full_output)
                                    stderr_full_output.emplace();
                                size_t bytes_to_append = std::min(static_cast<size_t>(stderr_res), MAX_STDERR_SIZE - current_size);
                                stderr_full_output->append(str.begin(), str.begin() + bytes_to_append);
                            }
                            else if (stderr_reaction == ExternalCommandStderrReaction::LOG_FIRST)
                            {
                                ssize_t to_insert = std::min(ssize_t(stderr_result_buf.reserve()), stderr_res);
                                if (to_insert > 0)
                                    stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.begin() + to_insert);
                            }
                            else if (stderr_reaction == ExternalCommandStderrReaction::LOG_LAST)
                            {
                                stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.begin() + stderr_res);
                            }
                        }
                    }
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

private:
    int stdout_fd;
    int stderr_fd;
    size_t timeout_milliseconds;
    ExternalCommandStderrReaction stderr_reaction;
    UDFProcessSubtreeSampler * sampler;
    bool final_sample_taken = false;

    static constexpr size_t BUFFER_SIZE = 4_KiB;
    static constexpr size_t MAX_STDERR_SIZE = 1_MiB;  /// Safety limit for stderr accumulation
    pollfd pfds[2]{};
    size_t num_pfds;
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

class ShellCommandHolder
{
public:
    using ShellCommandBuilderFunc = std::function<std::unique_ptr<ShellCommand>()>;

    explicit ShellCommandHolder(ShellCommandBuilderFunc && func_)
        : func(std::move(func_))
    {}

    ~ShellCommandHolder()
    {
        shared_memory = {};

        if (persistent_memory_charge)
            unchargePersistentMemory(persistent_memory_charge);
    }

    std::unique_ptr<ShellCommand> buildCommand()
    {
        if (returned_command)
            return std::move(returned_command);

        return func();
    }

    void returnCommand(std::unique_ptr<ShellCommand> command)
    {
        returned_command = std::move(command);
    }

    /// Shared-memory region(s) for this process, created once and reused across pool borrows.
    /// `index` selects the buffer: index 0 is used by the plain (synchronous) transport; the
    /// pipelined transport additionally uses index 1 for double buffering.
    ///
    /// Creating and growing regions does not charge any memory tracker here: while the holder is
    /// borrowed, the borrowing query owns the charge (see `releaseChargeToBorrower`).
    SharedMemoryRegionPtr getOrCreateSharedMemory(const std::string & directory, size_t size, size_t index, bool & created)
    {
        if (!shared_memory[index])
        {
            shared_memory[index] = std::make_shared<SharedMemoryRegion>(directory, size);
            created = true;
        }
        else
            created = false;

        return shared_memory[index];
    }

    /// Size of the region at `index`, or zero if it has not been created yet. Lets the borrower
    /// charge its query memory tracker for the right number of bytes BEFORE the region is created
    /// or reused, because creating one commits its backing `tmpfs` pages.
    size_t getSharedMemorySize(size_t index) const
    {
        return shared_memory[index] ? shared_memory[index]->size() : 0;
    }

    void growSharedMemory(size_t index, size_t new_size)
    {
        shared_memory[index]->grow(new_size);
    }

    void resetSharedMemory(size_t index)
    {
        shared_memory[index].reset();
    }

    /// Gives back the space a borrow grew the region at `index` beyond `base_size`. A region that
    /// one outsized chunk enlarged would otherwise stay mapped at that size for the lifetime of
    /// the worker: nothing else ever makes it smaller, and while the worker waits in the pool the
    /// memory is charged globally, where no query is blamed for it. Growing it again is a cheap
    /// `ftruncate` + remap, and the contents of a region never have to survive a borrow.
    void shrinkSharedMemory(size_t index, size_t base_size) noexcept
    {
        if (shared_memory[index] && shared_memory[index]->size() > base_size)
            shared_memory[index]->shrink(base_size);
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
    /// Never throws: this runs on a cleanup path, and it is an accounting hand-back rather than an
    /// allocation — the memory is already mapped, refusing the charge would not free anything.
    void acquireChargeFromBorrower() noexcept
    {
        size_t bytes = 0;
        for (const auto & region : shared_memory)
            if (region)
                bytes += region->size();

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
    }

    void unchargePersistentMemory(size_t bytes)
    {
        MemoryTrackerBlockerInThread blocker(VariableContext::User);
        MemoryTrackerSwitcher switcher(&total_memory_tracker);
        [[maybe_unused]] auto trace = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
        CurrentThread::flushUntrackedMemory();
        persistent_memory_charge -= bytes;
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
            std::shared_ptr<ProcessPool> process_pool_ = nullptr)
            : ISource(std::make_shared<const Block>(sample_block_->cloneEmpty()))
            , context(context_)
            , format(format_)
            , sample_block(sample_block_)
            , command(std::move(command_))
            , configuration(configuration_)
            , timeout_command_out(command->out.getFD(), command->err.getFD(), command_read_timeout_milliseconds, stderr_reaction, configuration_.sampler.get())
            , command_holder(std::move(command_holder_))
            , process_pool(process_pool_)
            , check_exit_code(check_exit_code_)
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
                cleanup();
                throw;
            }
        }

        ~ShellCommandSource() override
        {
            cleanup();
        }

    protected:
        void cleanup()
        {
            for (auto & thread : send_data_threads)
                if (thread.joinable())
                    thread.join();

            /// Record this borrow's resource usage before the child is gone. The two
            /// executable UDF types measure it differently.
            if (configuration.sampler)
            {
                if (process_pool)
                {
                    /// Resource accounting must observe the borrow's resident set before
                    /// the worker is torn down or the slot is handed back to the pool —
                    /// either path destroys `/proc/<pid>/{stat,status}` and the sampler
                    /// would then read zero CPU and zero `VmHWM`.
                    /// `recordReleased` reads procfs and may throw, but `cleanup` is
                    /// called from the destructor — swallow any exception so the
                    /// destructor stays noexcept.
                    try
                    {
                        configuration.sampler->recordReleased();
                    }
                    catch (...)
                    {
                        tryLogCurrentException("ShellCommandSource");
                    }
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
                    command_holder->returnCommand(std::move(command));

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

                if (check_exit_code)
                {
                    try
                    {
                        if (process_pool)
                        {
                            bool valid_command
                                = configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read;

                            // We can only wait for pooled commands when they are invalid.
                            if (!valid_command)
                                command->wait();
                        }
                        else
                            command->wait();
                    }
                    catch (Exception & e)
                    {
                        /// Enrich exit code exception with buffered stderr content (LOG_FIRST/LOG_LAST modes)
                        String stderr_content = timeout_command_out.consumeBufferedStderr();
                        if (!stderr_content.empty())
                            e.addMessage("Stderr: {}", stderr_content);
                        throw;
                    }
                }

                rethrowExceptionDuringSendDataIfNeeded();
            }

            return status;
        }

        String getName() const override { return "ShellCommandSource"; }

    private:

        void rethrowExceptionDuringSendDataIfNeeded()
        {
            std::lock_guard lock(send_data_lock);
            if (exception_during_send_data)
            {
                command_is_invalid = true;
                std::rethrow_exception(exception_during_send_data);
            }
        }

        ContextPtr context;
        std::string format;
        SharedHeader sample_block;

        std::unique_ptr<ShellCommand> command;
        ShellCommandSourceConfiguration configuration;

        TimeoutReadBufferFromFileDescriptor timeout_command_out;

        size_t current_read_rows = 0;

        ShellCommandHolderPtr command_holder;
        std::shared_ptr<ProcessPool> process_pool;

        bool check_exit_code = false;

        QueryPipeline pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;

        std::vector<ThreadFromGlobalPool> send_data_threads;

        std::mutex send_data_lock;
        std::exception_ptr exception_during_send_data;

        std::atomic<bool> command_is_invalid {false};
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
      * would copy every byte, which is what this transport exists to avoid.
      *
      * `grow` must make the region hold at least `required` bytes or throw. Enlarging a region
      * replaces its mapping, so `data()` is re-read after every growth; the bytes written so far
      * survive it, because the backing file keeps its contents.
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
      *        varint version, string path, varint input offset, varint input size;
      *   3. read the response from the child's stdout: a varint status, then, on success,
      *        varint output offset + varint output size; on failure, a length-prefixed error message;
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
            std::unique_ptr<ShellCommand> && command_,
            Pipe input_pipe_,
            const std::string & shared_memory_path_,
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
            , command(std::move(command_))
            , configuration(configuration_)
            , is_pooled(is_pooled_)
            , shared_memory_size(shared_memory_size_)
            , shared_memory_max_size(shared_memory_max_size_)
            , pipeline_mode(pipeline_mode_)
            , timeout_command_out(command->out.getFD(), command->err.getFD(), command_read_timeout_milliseconds, stderr_reaction, configuration_.sampler.get())
            , command_holder(std::move(command_holder_))
            , process_pool(process_pool_)
            , check_exit_code(check_exit_code_)
        {
            try
            {
                /// Create the region(s) here (not in the caller) so that any failure — creating or
                /// linking the backing file, reserving its storage, or mapping it — is cleaned up by
                /// this constructor, which returns the borrowed process holder to the pool. On the
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
                        /// Charge before the region is created: creating it commits the backing
                        /// tmpfs pages, so a query that is already at its memory limit has to be
                        /// rejected first (the non-pooled branch below does the same). A region
                        /// that survived a previous borrow may have grown, so charge what it
                        /// actually holds; a missing one is created at exactly shared_memory_size_.
                        size_t existing_size = command_holder->getSharedMemorySize(i);
                        chargeQueryMemory(existing_size ? existing_size : shared_memory_size_);
                        regions[i] = command_holder->getOrCreateSharedMemory(shared_memory_path_, shared_memory_size_, i, region_created);
                        regions_created_by_this_borrow[i] = region_created;
                    }
                    else
                    {
                        chargeQueryMemory(shared_memory_size_);
                        regions[i] = std::make_shared<SharedMemoryRegion>(shared_memory_path_, shared_memory_size_);
                        region_created = true;
                        regions_created_by_this_borrow[i] = region_created;
                    }

                    if (region_created)
                        ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, shared_memory_size_);
                }

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
                /// clean boundary and can be returned together with the holder.
                command_can_be_reused = true;
                cleanup();
                throw;
            }
        }

        ~ShellCommandSharedMemorySource() override
        {
            cleanup();
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
                        output_pipeline = QueryPipeline();
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

                /// The source can finish without generate() reaching the end of the input — most
                /// notably on cancellation. A child that is not going back to the pool only exits
                /// once it sees EOF on its stdin, so close it before the blocking wait below, which
                /// reaps the child before closing any pipe itself.
                closeStdinNoThrow(commandIsReused());

                if (timeout_command_out.hasStderr())
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Executable generates stderr: {}", timeout_command_out.getStderr());

                if (check_exit_code)
                {
                    try
                    {
                        /// The same decision as the stdin close above: a worker that goes back to
                        /// the pool must not be reaped here, and one that does not had its stdin
                        /// closed, so this blocking wait can actually finish.
                        if (!commandIsReused())
                            command->wait();
                    }
                    catch (Exception & e)
                    {
                        String stderr_content = timeout_command_out.consumeBufferedStderr();
                        if (!stderr_content.empty())
                            e.addMessage("Stderr: {}", stderr_content);
                        throw;
                    }
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
        void sendRequest(size_t index, size_t input_size)
        {
            writeVarUInt(SHARED_MEMORY_PROTOCOL_VERSION, *timeout_command_in);
            writeStringBinary(regions[index]->path(), *timeout_command_in);
            writeVarUInt(static_cast<UInt64>(0), *timeout_command_in);
            writeVarUInt(static_cast<UInt64>(input_size), *timeout_command_in);
            timeout_command_in->next();
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
                sendRequest(index, input_size);

                /// Response from the child: a status varint, then either the output location (on
                /// success), the size it needs (when the region is too small) or an error message.
                UInt64 status = 0;
                readVarUInt(status, timeout_command_out);

                if (status == SHARED_MEMORY_STATUS_NEED_MORE_SPACE)
                {
                    /// The result does not fit next to the input; the server is the only side that
                    /// can enlarge the region, so it does that and re-sends the same request. The
                    /// serialized input survives the growth (`ftruncate` keeps the file contents),
                    /// so it does not have to be written again. This terminates: every iteration
                    /// strictly increases the region size, which is capped by shared_memory_max_size.
                    UInt64 requested_size = 0;
                    readVarUInt(requested_size, timeout_command_out);

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
                    readStringBinary(message, timeout_command_out, SHARED_MEMORY_MAX_ERROR_MESSAGE_SIZE);
                    throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
                        "Executable UDF reported an error (status {}): {}", status, message);
                }

                readVarUInt(output_offset, timeout_command_out);
                readVarUInt(output_size, timeout_command_out);
                break;
            }

            auto & region = *regions[index];

            if (output_offset > region.size() || output_size > region.size() - output_offset)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable UDF returned an out-of-bounds region: offset {}, size {}, region size {}",
                    output_offset, output_size, region.size());

            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryOutputBytes, output_size);

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
        /// These are synthetic charges: the region is a tmpfs mmap, not a heap allocation at a known
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
                /// sit below `max_untracked_memory`, because the mmap/tmpfs allocation happens right
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

            size_t added = new_size - region.size();

            /// Charge first (may throw MEMORY_LIMIT_EXCEEDED), then grow; roll the charge back if
            /// the grow itself fails so accounting always matches the actual region size.
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
                unchargeQueryMemory(added);
                throw;
            }

            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryRegionGrowths);
            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, added);
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
        bool commandIsReused() const
        {
            return is_pooled
                && command != nullptr
                && !command_is_invalid
                && (command_can_be_reused
                    || (configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read));
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
                timeout_command_in->reset();
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
            output_pipeline = QueryPipeline();
            output_read_buffer.reset();

            /// Decide once, here: everything below - the stdin close, the regions, the hand-back to
            /// the pool - has to agree on whether this worker survives the borrow, and the decision
            /// stops being readable as soon as a discarded command is dropped further down.
            const bool keep_command = commandIsReused();

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
                    timeout_command_in->reset();
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
                if (process_pool)
                {
                    try
                    {
                        configuration.sampler->recordReleased();
                    }
                    catch (...)
                    {
                        tryLogCurrentException("ShellCommandSharedMemorySource");
                    }
                }
                else if (command)
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
                if (!keep_command)
                {
                    /// The worker process is being discarded (protocol failure, child death,
                    /// overproduction, cancellation, etc.). Its pooled shared-memory regions belong
                    /// to that process, so release all of them, including any created by an earlier
                    /// borrow, instead of leaving the tmpfs files and their persistent memory charge
                    /// pinned on the reused holder for a replacement process. resetSharedMemory
                    /// unmaps+unlinks the file (once the source's own reference below is dropped)
                    /// and uncharges memory.
                    for (size_t i = 0; i < regions.size(); ++i)
                    {
                        regions[i].reset();
                        command_holder->resetSharedMemory(i);
                        regions_created_by_this_borrow[i] = false;
                    }
                }
                else
                {
                    if (!constructor_finished)
                    {
                        /// Construction failed but the command stays valid and is reused: undo only
                        /// the regions this borrow created, preserving any created by an earlier one.
                        for (size_t i = 0; i < regions_created_by_this_borrow.size(); ++i)
                        {
                            if (regions_created_by_this_borrow[i])
                            {
                                regions[i].reset();
                                command_holder->resetSharedMemory(i);
                                regions_created_by_this_borrow[i] = false;
                            }
                        }
                    }

                    /// Regions that this borrow had to grow for an unusually large chunk are trimmed
                    /// back to the configured shared_memory_size before the worker goes back to the
                    /// pool. Growth is amortized within a borrow, but nothing ever shrinks a region
                    /// again, so without this a single outsized chunk would pin up to
                    /// shared_memory_max_size (twice that in pipelined mode) per pooled worker
                    /// against max_server_memory_usage for as long as the server runs.
                    for (size_t i = 0; i < regions.size(); ++i)
                        command_holder->shrinkSharedMemory(i, shared_memory_size);
                }

                /// Whatever regions the holder still owns outlive this borrow, so hand their charge
                /// back to it before the per-borrow charge below goes away. Doing it in this order
                /// means the bytes are never uncharged everywhere at once, and never charged twice.
                command_holder->acquireChargeFromBorrower();
            }

            /// Release the per-borrow memory charge on the query thread. The producer thread is
            /// joined above, so this total is final.
            if (size_t charge = query_memory_charge.load(std::memory_order_relaxed))
                unchargeQueryMemory(charge);

            if (command_holder && process_pool)
            {
                if (keep_command)
                    command_holder->returnCommand(std::move(command));

                process_pool->returnObject(std::move(command_holder));
            }
        }

        ContextPtr context;
        std::string format;
        SharedHeader sample_block;
        Block input_header;

        std::unique_ptr<ShellCommand> command;
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
        size_t shared_memory_size;
        size_t shared_memory_max_size;
        bool pipeline_mode;
        std::atomic<size_t> query_memory_charge = 0;

        TimeoutReadBufferFromFileDescriptor timeout_command_out;
        std::unique_ptr<TimeoutWriteBufferFromFileDescriptor> timeout_command_in;

        size_t current_read_rows = 0;
        bool stdin_closed = false;

        ShellCommandHolderPtr command_holder;
        std::shared_ptr<ProcessPool> process_pool;

        bool check_exit_code = false;

        QueryPipeline input_pipeline;
        std::unique_ptr<PullingPipelineExecutor> input_executor;

        /// output_read_buffer points into the shared-memory region and is read by the output
        /// pipeline (including its parallel-parsing threads), so it must outlive the pipeline:
        /// declared first, therefore destroyed last. cleanup() tears all three down in order.
        std::unique_ptr<ReadBufferFromMemory> output_read_buffer;
        QueryPipeline output_pipeline;
        std::unique_ptr<PullingPipelineExecutor> output_executor;

        std::atomic<bool> command_is_invalid {false};

        /// Background prefetcher for pipelined mode. Declared last so it is destroyed first; its
        /// destructor stops and joins the producer thread (cleanup() also does this explicitly).
        DoubleBufferedProducer producer;
    };

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
            process_pool->returnObject(std::move(process_holder));
    });

    auto destructor_strategy = ShellCommand::DestructorStrategy{true /*terminate_in_destructor*/, SIGTERM, configuration.command_termination_timeout_seconds};
    command_config.terminate_in_destructor_strategy = destructor_strategy;

    command_config.register_in_udf_process_registry = configuration.is_user_defined_function;

    bool is_executable_pool = (process_pool != nullptr);
    if (is_executable_pool)
    {
        bool execute_direct = configuration.execute_direct;

        bool result = process_pool->tryBorrowObject(
            process_holder,
            [command_config, execute_direct]()
            {
                ShellCommandHolder::ShellCommandBuilderFunc func = [command_config, execute_direct]() mutable
                {
                    if (execute_direct)
                        return ShellCommand::executeDirect(command_config);
                    return ShellCommand::execute(command_config);
                };

                return std::make_unique<ShellCommandHolder>(std::move(func));
            },
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

        process = process_holder->buildCommand();

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
        command_config.collect_resource_usage = (source_configuration.sampler != nullptr);
        if (configuration.execute_direct)
            process = ShellCommand::executeDirect(command_config);
        else
            process = ShellCommand::execute(command_config);

        /// Record the child pid so sampleExecutablePeak can walk the subtree
        /// during IO. No-op when sampler is null.
        if (source_configuration.sampler)
            source_configuration.sampler->recordExecutablePid(process->getPid());
    }

    if (configuration.use_shared_memory)
    {
        /// The shared-memory region is created inside the source (which now owns the borrowed
        /// process holder). Doing it there means a failure to create or link the backing file,
        /// reserve its storage, map it, or charge its memory is handled by the source's constructor
        /// cleanup, which returns the holder to the pool instead of permanently shrinking its capacity.
        auto source = std::make_unique<ShellCommandSharedMemorySource>(
            context,
            configuration.format,
            configuration.command_read_timeout_milliseconds,
            configuration.command_write_timeout_milliseconds,
            configuration.stderr_reaction,
            configuration.check_exit_code,
            std::make_shared<const Block>(std::move(sample_block)),
            std::move(process),
            std::move(input_pipes[0]),
            configuration.shared_memory_path,
            configuration.shared_memory_size,
            configuration.shared_memory_max_size,
            configuration.shared_memory_pipeline,
            is_executable_pool,
            source_configuration,
            std::move(process_holder),
            process_pool);

        return Pipe(std::move(source));
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
        process_pool);

    return Pipe(std::move(source));
}

}
