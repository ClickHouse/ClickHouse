#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/Sources/TimeoutPipeBuffers.h>

#include <functional>
#include <string_view>

/// What the pipe and the shared-memory transports of `ShellCommandSourceCoordinator` share about the
/// commands they run: how a pooled worker is judged, reported and handed back, and how a command
/// that is done with is waited for.

namespace DB
{

class ShellCommand;
class UDFProcessSubtreeSampler;

/// Logs why a pooled worker found at the end of a borrow not to be at a clean boundary (`state`) is
/// discarded, for both transports, with what it left on its stderr: nothing else reads a discarded
/// worker's pipes before they are closed. Read under every reaction that observes stderr, not only
/// when it is what disqualified the worker (`channelState` asks about it under `throw` alone). A
/// worker that simply exited is not a mistake and is not described as one. `advice` says what the
/// command must not do, in the terms of its protocol.
void logDirtyChannelDiscard(
    const char * log_name,
    TimeoutReadBufferFromFileDescriptor & output,
    const TimeoutReadBufferFromFileDescriptor::ChannelState & state,
    std::string_view advice);

/// What a pooled process that exited in the pool left on its stderr. It wrote that after the
/// hand-back probe of the query it last served and before it died, so nobody has read it: the
/// query is over and the process is about to be replaced. It is reported against the process
/// (logged by the caller) rather than dropped - a diagnostic written on the way out is the one a
/// person debugging the command most wants to see. Capped for the log line; the pipe is read to
/// what it holds at this moment and no further (`readWhatThePipeHolds`).
String readLeftoverStderrOfExitedProcess(const ShellCommand & process);

/// The context a command's output is parsed with, for both transports. Header auto-detection could
/// only cause trouble: a first row taken for a header makes the number of input and output rows
/// differ. And parallel parsing cannot read exactly `max_block_size` rows from a pipe that has no EOF,
/// so it is off where a fixed number of rows is read.
ContextMutablePtr makeContextForReadingCommandOutput(const ContextPtr & context, bool read_fixed_number_of_rows);

/// Records the end of a pooled borrow in the sampler, for both transports: before the worker is torn
/// down or handed back, while its `/proc` entries still say what the borrow cost. Best-effort.
void recordPooledReleaseNoThrow(UDFProcessSubtreeSampler * sampler, bool is_pooled, const char * log_name) noexcept;

/// Records the resource usage of a non-pooled command in the sampler, for both transports.
///
/// Peak memory was sampled from /proc VmHWM during IO, while the child was provably alive; by cleanup
/// the child has closed stdout and is exiting, so its `/proc` mm fields are gone - no useful sample
/// here.
///
/// Capture wait4 rusage for CPU. When `prepare` already waited the child via its blocking `wait`
/// (`check_exit_code=true`), `isWaitCalled` is true and this is skipped. A child lingering past the
/// poll budget is left to `~ShellCommand`'s bounded `command_termination_timeout` + SIGTERM, so
/// profiling cannot turn cleanup into a query hang. No status check: a non-zero exit must not raise
/// CHILD_WAS_NOT_EXITED_NORMALLY here.
void recordNonPooledUsage(UDFProcessSubtreeSampler & sampler, ShellCommand & command, const char * log_name);

/// Puts every input descriptor of `command` back into blocking mode, as a pooled worker is kept
/// between borrows. False, logged, if one of them cannot be.
bool restoreBlockingInputs(ShellCommand & command) noexcept;

/// Waits for a command that is done with (`ShellCommand::waitDrainingOutput` with `options`), for
/// both transports, putting what it writes to stderr meanwhile through the reaction of `output`: the
/// wait is the last stretch in which the command can still write. Fails the query if the exit status
/// was wanted and could not be read - a status that could not be read is not a passing one, however
/// little time the command was given, or `check_exit_code` would mean "checked, unless the timeout is
/// short" - and, under `throw`, if the command wrote to stderr. A failure carries what the command
/// said on stderr. `close_inputs_first` closes every input of the command before the wait, so that a
/// command written to exit on their EOF does. `what_happened` describes the command for the message,
/// as in "The command did not exit ... after its stdin was closed".
struct CommandThatDidNotExit
{
    std::string_view subject;
    std::string_view after;
};

void waitForCommandExit(
    ShellCommand & command,
    TimeoutReadBufferFromFileDescriptor & output,
    bool close_inputs_first,
    ShellCommand::WaitDrainingOptions options,
    CommandThatDidNotExit what_happened);

/// For `ShellCommand::waitDrainingOutput`: interrupts the wait for a command once its query is killed.
std::function<void()> queryKilledCheck(const ContextPtr & context);

/// What makes a pooled worker that served an earlier borrow unfit to build this one on - found
/// before anything is sent to it, which is the last point at which it can still be replaced
/// instead of failing the query that borrowed it, for something that happened before it started.
///
/// - It exited while it sat in the pool. Built on, it would fail the query obscurely, on its first
///   write to a closed stdin.
/// - It wrote to its stdout after it was handed back, dead or alive. The bytes are an earlier
///   borrow's, so provably not this one's answer, and nothing would let the query tell them from
///   its own once it starts reading.
/// - Under `stderr_reaction` `throw`, it left stderr. The bytes are an earlier borrow's and can be
///   drained without the reaction, but nothing tells when the command has finished writing them:
///   one in the middle of a burst writes the rest once room is made, and this query would fail for
///   a diagnostic it did not cause. Under every other reaction stray stderr only lands in a log line,
///   and the worker is kept.
enum class UnfitReusedWorker : uint8_t
{
    NONE,
    EXITED,
    STRAY_STDOUT,
    STRAY_STDERR,
};

UnfitReusedWorker inspectReusedWorker(const ShellCommand & worker, bool stderr_throws);

/// Logs why a worker `inspectReusedWorker` found unfit is discarded, with what it left on its stderr -
/// read now, before the process goes with its pipes: nobody else will ever read it. `subject` names
/// the worker, `with` what goes with it. Can throw (reading, formatting and logging allocate).
void reportUnfitReusedWorker(
    const ShellCommand & worker, UnfitReusedWorker reason, const char * log_name, std::string_view subject, std::string_view with);

}
