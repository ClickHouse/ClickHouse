#pragma once

#include <Common/ShellCommand.h>
#include <Common/ShellCommandSettings.h>
#include <Common/Stopwatch.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <base/unit.h>

#include <boost/circular_buffer.hpp>

#include <poll.h>

#include <memory>
#include <optional>
#include <string_view>

/// The pipes of an external command with timeouts: reading its stdout while keeping its stderr
/// drained and put through `stderr_reaction`, and writing its stdin. Shared by the pipe and the
/// shared-memory transports of `ShellCommandSourceCoordinator`.

namespace DB
{

class UDFProcessSubtreeSampler;

bool tryMakeFdNonBlocking(int fd);

void makeFdNonBlocking(int fd);

bool tryMakeFdBlocking(int fd);

void makeFdBlocking(int fd);

int pollWithTimeout(pollfd * pfds, size_t num, size_t timeout_milliseconds);

/// What the pipe `fd` holds at this moment, up to `max_size` bytes: exactly the bytes that are there
/// (`FIONREAD`), so the read never waits, whatever the mode of the descriptor, and a writer that
/// keeps the pipe busy - a descendant of a dead command that inherited its write end - cannot hold
/// the reader. Nothing for a closed descriptor (-1). Throws if the pipe cannot be measured.
String readWhatThePipeHolds(int fd, size_t max_size);

bool pollFd(int fd, size_t timeout_milliseconds, int events);

class TimeoutReadBufferFromFileDescriptor : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit TimeoutReadBufferFromFileDescriptor(
        int stdout_fd_,
        int stderr_fd_,
        size_t timeout_milliseconds_,
        ExternalCommandStderrReaction stderr_reaction_,
        UDFProcessSubtreeSampler * sampler_,
        size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;

    ~TimeoutReadBufferFromFileDescriptor() override;

    /// Check if stderr was accumulated (for THROW mode)
    bool hasStderr() const { return stderr_full_output.has_value(); }

    /// Get accumulated stderr content (for THROW mode)
    const String & getStderr() const { return *stderr_full_output; }

    /// Get buffered stderr content from circular buffer (for LOG_FIRST/LOG_LAST modes)
    /// Clears the buffer to prevent duplicate logging in destructor
    String consumeBufferedStderr();

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

    /// Whether a pipe holds bytes nobody has read: what a pooled process wrote after its last answer.
    static bool pipeHasPendingOutput(int fd) noexcept { return (ShellCommand::pendingEvents(fd) & POLLIN) != 0; }

    /// Whether the command's stderr holds bytes nobody has read, whatever the reaction.
    bool stderrHasPendingOutput() const noexcept { return pipeHasPendingOutput(stderr_fd); }

    /// `consider_buffered_output` says whether bytes this buffer has read but not handed on count
    /// as unread output. They do for the shared-memory transport, where every byte of the response
    /// frame is accounted for and a leftover is a protocol violation. They do not for the pipe
    /// transport, where a format reader may legitimately hold buffered bytes it did not parse, and
    /// only what is still in the kernel pipe is evidence that the command spoke out of turn.
    ChannelState channelState(bool consider_buffered_output = true) const noexcept;

    /// The same read as `consumePendingStderr`, without putting what it finds through the reaction.
    /// For a caller that has to clear the pipe of somebody else's output - see
    /// `discardStderrLeftByAPreviousBorrow`.
    String consumePendingStderrWithoutReaction() const { return readPendingStderr(); }

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
    String consumePendingStderr();

    String readPendingStderr() const { return readWhatThePipeHolds(stderr_fd, MAX_PENDING_STDERR_SIZE); }

    /// Whether anything is done with the command's stderr beyond taking it off the pipe. `NONE`
    /// drops what it reads, so a caller that would only read in order to drop has nothing to do.
    bool stderrIsObserved() const { return stderr_reaction != ExternalCommandStderrReaction::NONE; }

    /// Whether stderr the command writes fails the query. The one reaction under which a byte of
    /// stderr that was not attributed to the query that caused it is a wrong verdict, and not just
    /// a log line in the wrong place.
    bool stderrThrows() const { return stderr_reaction == ExternalCommandStderrReaction::THROW; }

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
    void drainStderrFully(size_t budget_milliseconds, bool with_reaction = true, size_t bytes_already_read = 0);

    /// Whether the stderr pipe has - counting `bytes_already_read` that were just taken off it - less
    /// than `PIPE_BUF` bytes free: the only state in which the command can be blocked writing to it.
    /// Answered "yes" where the pipe cannot be measured, which only costs the wait.
    bool stderrPipeIsNearlyFull(size_t bytes_already_read) const;

    /// Clears what a pooled worker left on its stderr after the response of an earlier borrow, before
    /// this borrow sends its first request, and logs it against nobody: those bytes are the earlier
    /// query's, and putting them through this query's reaction would fail it, under `throw`, for a
    /// diagnostic it did not cause. The report is capped; the pipe is not - a worker that filled it
    /// is blocked in `write` and will not read the request until there is room - so what is left
    /// beyond the cap, and what the command writes while this drains, is dropped the same way.
    void clearStderrOfAnEarlierBorrow(const char * log_name);

    /// For a caller that reads the command's stderr itself and needs those bytes to go through the
    /// configured reaction all the same - the bounded wait that reaps a command drains both pipes,
    /// and that is the last stretch in which a command can still write.
    void consumeStderrBytes(std::string_view str) { consumeStderrChunk(str); }

    /// Makes every read until `disarmFrameDeadline` share one deadline, `timeout_milliseconds`
    /// from now, rather than each read getting its own: what bounds the time a whole response
    /// may take to arrive, not just the gap between two of its pieces.
    void armFrameDeadline() noexcept { frame_deadline_ns = readDeadlineNs(); }
    void disarmFrameDeadline() noexcept { frame_deadline_ns = 0; }

private:
    /// One chunk of the command's stderr, put through the configured reaction. `NONE` matches
    /// nothing and the bytes are dropped - which is exactly what it is for: they still have to be
    /// taken off the pipe, or the command blocks in `write` once the pipe fills up.
    void consumeStderrChunk(std::string_view str);

    /// Accumulating stops at `MAX_STDERR_SIZE`, but reading never does: the point of reading is to
    /// keep the command from blocking, and that is true whether or not the bytes are still wanted.
    void accumulateStderrForThrow(std::string_view str);

    /// Reads what is pending on stderr once and puts it through the reaction. A pipe that is done -
    /// EOF, or an error that reading again will not fix - is dropped out of the poll set, because
    /// `poll` reports a hung-up descriptor immediately and forever: left in, it would turn the wait
    /// for the command's next answer into a busy loop that spins a core and never reaches
    /// `command_read_timeout`. A command closing its own stderr is ordinary (see
    /// `shm_udf_quiet_stderr.py`), so this is not an error path.
    ///
    /// Returns the number of bytes read.
    size_t readStderrOnce(bool with_reaction = true);

    void stopPollingStderr() noexcept;

    [[noreturn]] void throwReadTimeout() const;

    /// A monotonic deadline `timeout_milliseconds` from now. Clamped, because
    /// `command_read_timeout` is not bounded anywhere: a huge value would wrap the multiplication
    /// or the addition and put the deadline in the past, turning every wait into a probe.
    UInt64 readDeadlineNs() const noexcept { return monotonicDeadlineNs(timeout_milliseconds); }

    static size_t remainingMs(UInt64 deadline_ns) noexcept { return millisecondsUntil(deadline_ns); }

    int stdout_fd;
    int stderr_fd;
    size_t timeout_milliseconds;
    /// Zero when no frame deadline is armed.
    UInt64 frame_deadline_ns = 0;
    ExternalCommandStderrReaction stderr_reaction;
    UDFProcessSubtreeSampler * sampler;
    bool final_sample_taken = false;
    bool stdout_is_done = false;

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
    explicit TimeoutWriteBufferFromFileDescriptor(
        int fd_, size_t timeout_milliseconds_, UDFProcessSubtreeSampler * sampler_, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    /// Restore blocking mode before the command is returned to the process pool.
    /// Safe only while the fd is provably open (the send-data task calls this right
    /// before closing/returning); the destructor must not do it, see
    /// ~TimeoutReadBufferFromFileDescriptor.
    void reset() const;

private:
    int fd;
    size_t timeout_milliseconds;
    UDFProcessSubtreeSampler * sampler;
};

}
