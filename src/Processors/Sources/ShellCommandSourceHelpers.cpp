#include <Processors/Sources/ShellCommandSourceHelpers.h>

#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/UDFProcessSubtreeSampler.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <base/errnoToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNSUPPORTED_METHOD;
}

void logDirtyChannelDiscard(
    const char * log_name,
    TimeoutReadBufferFromFileDescriptor & output,
    const TimeoutReadBufferFromFileDescriptor::ChannelState & state,
    std::string_view advice)
{
    const bool read_stderr = state.stderr_has_unread_output || (output.stderrIsObserved() && output.stderrHasPendingOutput());
    const String leftover_stderr = read_stderr ? output.consumePendingStderr() : String{};

    if (state.stdout_hung_up && !state.stdout_has_unread_output)
    {
        /// What it said on its way out is the one clue to why it exited.
        if (leftover_stderr.empty())
            LOG_DEBUG(getLogger(log_name), "The process of a pooled command exited after answering, so it was not returned to the pool.");
        else
            LOG_WARNING(
                getLogger(log_name),
                "The process of a pooled command exited after answering, so it was not returned to the pool. Stderr: {}",
                leftover_stderr);
        return;
    }

    LOG_WARNING(
        getLogger(log_name),
        "A pooled command left unread output on its {} after answering, so its process was discarded instead of reused. {}{}{}",
        state.stdout_has_unread_output && state.stderr_has_unread_output ? "stdout and stderr"
                                                                         : (state.stdout_has_unread_output ? "stdout" : "stderr"),
        advice,
        leftover_stderr.empty() ? "" : " Stderr: ",
        leftover_stderr);
}

String readLeftoverStderrOfExitedProcess(const ShellCommand & process)
{
    static constexpr size_t max_reported = 4_KiB;
    return readWhatThePipeHolds(process.err.getFD(), max_reported);
}

ContextMutablePtr makeContextForReadingCommandOutput(const ContextPtr & context, bool read_fixed_number_of_rows)
{
    auto context_for_reading = Context::createCopy(context);
    if (read_fixed_number_of_rows)
        context_for_reading->setSetting("input_format_parallel_parsing", false);
    context_for_reading->setSetting("input_format_csv_detect_header", false);
    context_for_reading->setSetting("input_format_tsv_detect_header", false);
    context_for_reading->setSetting("input_format_custom_detect_header", false);
    return context_for_reading;
}

void recordPooledReleaseNoThrow(UDFProcessSubtreeSampler * sampler, bool is_pooled, const char * log_name) noexcept
{
    if (!sampler || !is_pooled)
        return;

    try
    {
        sampler->recordReleased();
    }
    catch (...)
    {
        tryLogCurrentException(log_name);
    }
}

void recordNonPooledUsage(UDFProcessSubtreeSampler & sampler, ShellCommand & command, const char * log_name)
{
    if (!command.isWaitCalled())
    {
        try
        {
            command.tryWaitWithoutStatusCheck();
        }
        catch (...)
        {
            tryLogCurrentException(log_name);
        }
    }

    /// Peak memory is independent of the wait: it comes from /proc VmHWM sampled during IO and
    /// stamped by recordExecutableElapsed. CPU requires the wait4 rusage and is recorded only when
    /// the wait succeeded.
    sampler.recordExecutableElapsed();

    if (command.wasChildResourceUsageCaptured())
        sampler.recordExecutableFinished(command.getChildUserTimeMicroseconds(), command.getChildSystemTimeMicroseconds());
}

bool restoreBlockingInputs(ShellCommand & command) noexcept
{
    bool restored = tryMakeFdBlocking(command.in.getFD());
    for (auto & [_, write_buffer] : command.write_fds)
        restored = tryMakeFdBlocking(write_buffer.getFD()) && restored;
    if (!restored)
    {
        try
        {
            LOG_WARNING(getLogger("ShellCommandSource"), "Cannot restore the blocking mode of an input of a pooled command: {}", errnoToString());
        }
        catch (...) // NOLINT(bugprone-empty-catch) Ok: a log line that cannot be written must not fail a noexcept function
        {
        }
    }
    return restored;
}

void waitForCommandExit(
    ShellCommand & command,
    TimeoutReadBufferFromFileDescriptor & output,
    bool close_inputs_first,
    ShellCommand::WaitDrainingOptions options,
    CommandThatDidNotExit what_happened)
{
    options.stderr_sink = [&output](std::string_view str) { output.consumeStderrBytes(str); };
    try
    {
        /// Inside: closing flushes, and a failure there is a failure of this wait like any other,
        /// reported with what the command said on stderr.
        if (close_inputs_first)
            command.closeInputs();

        const bool reaped = command.waitDrainingOutput(options);
        if (!reaped && options.check_exit_status)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "{} did not exit within command_termination_timeout ({} seconds){}, so its exit code could not be "
                "checked; it will be signalled. Give it a longer command_termination_timeout, or set check_exit_code "
                "to 0 for a command that is not expected to exit on its own",
                what_happened.subject, command.terminationTimeoutSeconds(), what_happened.after);
    }
    catch (Exception & e)
    {
        /// Enriched with what the command said on stderr: the buffered part (`log_first`/`log_last`),
        /// and under `throw` what was accumulated - which this wait may have read only now, after the
        /// check that gives stderr priority over the exit status has already passed. A command that
        /// writes its diagnostic and exits with an error must not come out as a bare exit status.
        String stderr_content = output.consumeBufferedStderr();
        if (!stderr_content.empty())
            e.addMessage("Stderr: {}", stderr_content);
        if (output.hasStderr())
            e.addMessage("Executable generates stderr: {}", output.getStderr());
        throw;
    }

    if (output.hasStderr())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Executable generates stderr: {}", output.getStderr());
}

std::function<void()> queryKilledCheck(const ContextPtr & context)
{
    return [query_status = context->getProcessListElement()]
    {
        if (query_status)
            query_status->throwIfKilled();
    };
}

UnfitReusedWorker inspectReusedWorker(const ShellCommand & worker, bool stderr_throws)
{
    /// One probe of stdout for both questions. A probe that fails (`POLLERR`, `POLLNVAL`) is a
    /// worker not to build on, like one that exited.
    const Int16 stdout_events = ShellCommand::pendingEvents(worker.out.getFD());
    if ((stdout_events & POLLIN) != 0)
        return UnfitReusedWorker::STRAY_STDOUT;
    if ((stdout_events & (POLLHUP | POLLERR | POLLNVAL)) != 0)
        return UnfitReusedWorker::EXITED;
    if (stderr_throws && TimeoutReadBufferFromFileDescriptor::pipeHasPendingOutput(worker.err.getFD()))
        return UnfitReusedWorker::STRAY_STDERR;
    return UnfitReusedWorker::NONE;
}

void reportUnfitReusedWorker(
    const ShellCommand & worker, UnfitReusedWorker reason, const char * log_name, std::string_view subject, std::string_view with)
{
    const String leftover_stderr = readLeftoverStderrOfExitedProcess(worker);
    const auto log = getLogger(log_name);
    switch (reason)
    {
        case UnfitReusedWorker::NONE:
            return;
        case UnfitReusedWorker::EXITED:
            if (leftover_stderr.empty())
                LOG_DEBUG(log, "{} (pid {}) exited while it was idle in the pool; it is discarded{} and a replacement is "
                    "started for this borrow.", subject, worker.getPid(), with);
            else
                LOG_WARNING(log, "{} (pid {}) exited while it was idle in the pool, after writing to its stderr; it is "
                    "discarded{} and a replacement is started for this borrow. Stderr: {}",
                    subject, worker.getPid(), with, leftover_stderr);
            return;
        case UnfitReusedWorker::STRAY_STDOUT:
            LOG_WARNING(log, "{} (pid {}) had unread output on its stdout when it was borrowed, so it wrote after the "
                "response of an earlier invocation; it is discarded{} and a replacement is started for this borrow. The "
                "command must write nothing past its answer.{}{}",
                subject, worker.getPid(), with, leftover_stderr.empty() ? "" : " Stderr: ", leftover_stderr);
            return;
        case UnfitReusedWorker::STRAY_STDERR:
            LOG_WARNING(log, "{} (pid {}) had unread output on its stderr when it was borrowed under stderr_reaction "
                "'throw', so it wrote after the response of an earlier invocation and may still be writing; it is "
                "discarded{} and a replacement is started for this borrow. Stderr: {}",
                subject, worker.getPid(), with, leftover_stderr);
            return;
    }
}

}
