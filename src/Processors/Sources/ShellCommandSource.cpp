#include <Processors/Sources/ShellCommandSource.h>

#include <Processors/Sources/ShellCommandHolder.h>
#include <Processors/Sources/ShellCommandSourceHelpers.h>
#include <Processors/Sources/TimeoutPipeBuffers.h>
#include <Processors/Sources/ShellCommandSharedMemorySource.h>

#include <fcntl.h>
#include <poll.h>

#include <Common/CurrentThread.h>
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

#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Core/Block.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Field.h>

#include <atomic>

#include <fmt/ranges.h>

#include <csignal>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int UDF_EXECUTION_FAILED;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{
    /** A stream, that get child process and sends data using tasks in background threads.
    * For each send data task background thread is created. Send data task must send data to process input pipes.
    * ShellCommandPoolSource receives data from process stdout.
    *
    * If a borrowed holder is passed in constructor then after source is destroyed process is returned to pool.
    */
    class ShellCommandSource final : public ISource
    {
    public:

        using SendDataTask = std::function<void()>;

        /// `command_holder_` and `worker_is_reused_` are those of a pooled worker; empty and false
        /// for a command of its own.
        ShellCommandSource(
            ContextPtr context_,
            const ShellCommandSourceCoordinator::Configuration & coordinator_configuration,
            SharedHeader sample_block_,
            std::unique_ptr<ShellCommand> && command_,
            VectorWithMemoryTracking<SendDataTask> && send_data_tasks,
            const ShellCommandSourceConfiguration & configuration_,
            BorrowedShellCommandHolder && command_holder_,
            bool worker_is_reused_)
            : ISource(std::make_shared<const Block>(sample_block_->cloneEmpty()))
            , context(context_)
            , format(coordinator_configuration.format)
            , sample_block(sample_block_)
            , configuration(configuration_)
            /// Reads the descriptors out of the command without taking it yet - see the declaration
            /// of `command` for why this object takes ownership as late as it can.
            , timeout_command_out(
                  command_->out.getFD(),
                  command_->err.getFD(),
                  coordinator_configuration.command_read_timeout_milliseconds,
                  coordinator_configuration.stderr_reaction,
                  configuration_.sampler.get())
            , is_pooled(static_cast<bool>(command_holder_))
            , check_exit_code(coordinator_configuration.check_exit_code)
            , is_user_defined_function(coordinator_configuration.is_user_defined_function)
            , worker_is_reused(worker_is_reused_)
            , command_holder(std::move(command_holder_))
            , command(std::move(command_))
        {
            /// Everything the constructor does lives in this try: a borrowed process holder and its
            /// worker are already owned by this object (the caller's locals were moved from in the
            /// member initializer list above), so an exception that escapes here would skip
            /// `cleanup`. The pool's slot would not be lost - `BorrowedShellCommandHolder` returns
            /// the holder from wherever it is - but the worker would be destroyed rather than
            /// handed back with it, whatever `cleanup` would have decided about it. Copying the
            /// context and changing its settings can throw - MEMORY_LIMIT_EXCEEDED, say.
            try
            {
                context = makeContextForReadingCommandOutput(context, configuration.read_fixed_number_of_rows);

                /// Before anything is sent to a worker that has served somebody else.
                quarantineReusedWorker();

                auto thread_group = CurrentThread::getGroup();

                /// From here on requests reach the worker.
                sending_started = true;
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
                /// A failure before the send threads were started - copying the context, an
                /// allocation past the memory limit - reached nothing of the worker: no request was
                /// sent, so it is at the boundary it was borrowed at and goes back to the pool as it
                /// is, rather than being killed and replaced over something that was not its doing.
                /// One that `quarantineReusedWorker` found unfit is marked invalid and goes.
                worker_untouched = !sending_started && !command_is_invalid && command != nullptr;

                /// Handed back as it was borrowed: `createPipe` made its stdin non-blocking for the
                /// send tasks, and it is the send task that makes it blocking again (`reset`). A
                /// worker whose stdin cannot be restored is not handed back.
                if (worker_untouched)
                    worker_untouched = restoreBlockingInputs(*command);

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
                if (is_pooled)
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
                    /// The send task closes stdin only when it has sent everything; on this path
                    /// (cancellation, a failure downstream) it may not have, and the wait inside
                    /// would then sit out `command_termination_timeout` in front of a child that is
                    /// blocked reading its input. Closed first, so that the child sees EOF and exits;
                    /// the send threads were joined at the top of this function. Idempotent.
                    closeCommandInputsNoThrow();
                    recordNonPooledUsage(*configuration.sampler, *command, "ShellCommandSource");
                }
            }

            if (command_is_invalid)
                command = nullptr;

            if (command_holder)
            {
                bool valid_command = answeredInFull();

                if (command && valid_command)
                    valid_command = pipeWorkerIsAtACleanBoundary();

                /// See the constructor: nothing was sent to it.
                if (worker_untouched)
                    valid_command = true;

                if (command && valid_command)
                    command_holder->returnCommand(std::move(command));

                /// A worker that is not going back to the pool has to die before its slot does: the
                /// query waiting for that slot starts a replacement at once, so leaving this process
                /// to be destroyed later - with a `command_termination_timeout` wait in front of it -
                /// lets the pool run over `pool_size` for as long as that takes. Its inputs are
                /// closed first; the send threads were joined at the top of this function.
                /// It gets what it always got: EOF on its inputs and `command_termination_timeout` to
                /// exit on it - a command may have cleanup to do (a `finally`, a `SIGTERM` handler) -
                /// then `termination_signal` (`~ShellCommand`), after which it is reaped by
                /// `ShellCommandsHolder` once it goes. A command that ignores the signal outlives its
                /// slot, and the pool runs over `pool_size` for as long as it lives - as it always did.
                if (command)
                    closeCommandInputsNoThrow();

                command = nullptr;

                command_holder.returnToPool();
            }
        }

        /// Whether the command has answered with every row it was asked for, which leaves it at the
        /// boundary after its answer. With the count read from its output
        /// (`read_number_of_rows_from_process_output`), that count is known only once it has been
        /// read: before that, `number_of_rows_to_read` is zero and "zero rows read of zero" would
        /// take a worker whose input was sent and whose answer has not arrived yet for one that has
        /// answered - and the next query would read that answer as its own.
        bool answeredInFull() const
        {
            return configuration.read_fixed_number_of_rows
                && (!configuration.read_number_of_rows_from_process_output || row_count_read)
                && current_read_rows >= configuration.number_of_rows_to_read;
        }

        /// The teardown paths must not throw, and closing a descriptor can (a `WriteBufferFromFile`
        /// finalizes itself on the way out). A worker whose inputs could not be closed is being
        /// thrown away anyway: the destructor's bounded wait and its signal are what is left.
        void closeCommandInputsNoThrow() noexcept
        {
            try
            {
                command->closeInputs();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource");
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
                        row_count_read = true;

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

                /// A command that produces more rows than requested violates the UDF protocol. A
                /// row format cannot hand over more than `max_block_size` rows at once, but a
                /// block format (`Native`, `Arrow`) returns the command's block whole, so the
                /// excess can arrive inside this very chunk. Detect it here, before the chunk
                /// leaves the source, so that the exception marks the command invalid and a
                /// pooled worker is discarded instead of being returned as if it had answered
                /// correctly - which is what counting the rows further up the pipeline would do.
                if (is_user_defined_function && configuration.read_fixed_number_of_rows
                    && current_read_rows + chunk.getNumRows() > configuration.number_of_rows_to_read)
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Executable UDF wrong result, expected {} row(s), but the command produced more (at least {})",
                        configuration.number_of_rows_to_read,
                        current_read_rows + chunk.getNumRows());

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
                if (is_pooled)
                {
                    bool valid_command = answeredInFull();

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
                ///
                /// Not for a worker that `checkPooledWorkerAfterAnswering` has already waited for:
                /// that wait read its stderr to the end and closed the descriptors, and a poll on a
                /// closed number would be a poll on whatever another thread has opened under it
                /// since.
                if (!wait_for_command && command && !command->isWaitCalled() && timeout_command_out.stderrIsObserved())
                {
                    static constexpr size_t late_stderr_drain_ms = 10;
                    timeout_command_out.drainStderrFully(late_stderr_drain_ms);

                    if (timeout_command_out.hasStderr())
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Executable generates stderr: {}", timeout_command_out.getStderr());
                }

                if (wait_for_command && (check_exit_code || timeout_command_out.stderrIsObserved()))
                {
                    /// The worker is about to exit - on the EOF the inputs are closed for below, or
                    /// at the latest when the wait after that reaps it - and `/proc/<pid>` goes
                    /// with it: a zombie has no `VmHWM` left, and a reaped pid has nothing at all.
                    /// The borrow's CPU and peak resident set have to be read before either, or
                    /// this - a discarded worker, which is the case the accounting is most wanted
                    /// for - reports zeros. A no-op off the pool path, and idempotent, so `cleanup`
                    /// can call the same thing for every path that does not come through here.
                    recordPooledResourceUsageNoThrow();

                    /// A pooled worker keeps its stdin open across borrows - the send task leaves
                    /// it so, for the next request - and this one is not going back: it answered
                    /// short, or the query finished with it early. The wait below is bounded by
                    /// `command_termination_timeout`, but a live worker that still has its stdin
                    /// would sit out the whole of it waiting for a request, hold the pool's slot
                    /// for that long, and then be signalled instead of exiting on its own. Closed
                    /// here, so that it sees EOF and exits the way it is written to; the send
                    /// threads are joined above, so nothing is writing into it. Not only for the
                    /// pool: a non-pooled command started without input pipes (a dictionary's
                    /// `loadAll`, an `Executable` table without input queries) had no send task to
                    /// close its stdin either, and would sit out the same timeout. Idempotent.
                    ///
                    /// Every input, not only `stdin`: a command given several input queries reads
                    /// the rest from the extra descriptors, and one written to exit when its inputs
                    /// are done waits for EOF on all of them. Closed by `waitForCommandExit`, after the
                    /// reader has stopped.

                    /// Stop reading the child's stdout before this wait touches the same descriptor.
                    /// The source can be finished from above - a `LIMIT` downstream closes the
                    /// output port - while `ParallelParsingInputFormat` still has a segmentator
                    /// thread reading that pipe on its own; draining it here would then be a second
                    /// reader on one descriptor, and closing it afterwards would pull it out from
                    /// under that thread. Destroying the executor joins it first. `cleanup` does
                    /// the same for the paths that never reach here, and both are idempotent.
                    stopReadingCommandOutput();

                    /// `waitDrainingOutput` rather than `wait`: the command may still be
                    /// writing. Reading its stdout stops at the row count this source asked
                    /// for, and stderr is only drained until it goes quiet, so a command that
                    /// carries on writing past either is blocked in `write` on a full pipe -
                    /// and `wait` reaps before it closes anything, so it would never return.
                    /// Draining lets the command reach its own exit.
                    ///
                    /// With `check_exit_code` a non-pooled command is waited for without a bound,
                    /// as the blocking `wait` this replaces did: a command whose cleanup
                    /// outlasts `command_termination_timeout` and then exits successfully passes,
                    /// as it always has. A pooled worker being discarded was never waited for, and
                    /// it gets `command_termination_timeout` and no more; one that does not exit
                    /// within it fails the query rather than being waved through, because a
                    /// status that cannot be read is not a passing one. `check_exit_code = 0` is
                    /// how a command that is not expected to exit promptly is configured.
                    ///
                    /// Waited for even without `check_exit_code`, when stderr is observed: this
                    /// is the last stretch in which the command can write, and a line it writes
                    /// on its way out has to reach `stderr_reaction` whether or not its exit
                    /// status is anyone's business. That costs a command which does not exit
                    /// on stdin EOF nothing it was not already paying: `~ShellCommand` waits
                    /// the same `command_termination_timeout` before it signals, and the two
                    /// waits draw from one deadline (`remainingTerminationTimeoutMs`), so the
                    /// budget is spent once, here instead of there.
                    /// A downstream `LIMIT` can finish the port before the source reaches EOF.
                    /// A producer that goes on writing is not kept alive by draining it: its
                    /// stdout is closed after a limited amount of extra output, or once
                    /// `command_termination_timeout` has passed, and it dies on `SIGPIPE`, which is
                    /// not reported as its failure. The wait for its exit stays unbounded all the
                    /// same, so a command that has stopped writing and takes its time to exit
                    /// passes, as it does when it is read to the end. A command that neither
                    /// writes nor exits is waited for until the query is killed - as the blocking
                    /// `wait` did, which could not be killed.
                    const bool output_abandoned = !finished
                        && (!configuration.read_fixed_number_of_rows || current_read_rows < configuration.number_of_rows_to_read);
                    waitForCommandExit(*command, timeout_command_out, /*close_inputs_first=*/ true,
                        {
                            .stderr_sink = {},
                            .check_exit_status = check_exit_code,
                            .unbounded_status_wait = !is_pooled,
                            .limit_stdout_drain = output_abandoned,
                            .check_cancelled = queryKilledCheck(context),
                        },
                        {.subject = "The command", .after = " after its stdin was closed"});
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
            /// Nothing to poll once the streams are gone, for the reasons the two probes give.
            if (!command || command->isWaitCalled() || command->isStdoutClosed())
                return;

            const auto state = timeout_command_out.channelState(/*consider_buffered_output=*/ false);

            if (!state.stdout_hung_up)
                return;

            /// The worker is gone either way; it must not go back to the pool.
            command_is_invalid = true;

            if (!check_exit_code)
                return;

            stopReadingCommandOutput();

            /// The wait below reaps the worker, and `/proc/<pid>` goes with it: read the borrow's
            /// CPU and peak resident set first, as every other wait that discards a worker does,
            /// or exactly this case - answered and exited - reports zeros. Idempotent, so
            /// `cleanup` calling it again afterwards is harmless.
            recordPooledResourceUsageNoThrow();

            /// A pooled worker keeps its stdin open across borrows, and this one is not going back:
            /// closed before the wait, so that a worker that hung up its stdout but is still alive
            /// on its stdin sees EOF and exits at once, rather than sitting out the whole
            /// `command_termination_timeout` and failing the query for an exit code that was a
            /// close away. The send threads are joined by the caller, so nothing is writing into
            /// them - and it is every input, not only `stdin`: a command given several input
            /// queries waits for EOF on all of them before it exits.
            /// The same rule as the wait in `prepare` that discards a worker: a status that could
            /// not be read within `command_termination_timeout` is not a passing status. A worker
            /// that closed its stdout and then lingers has not been checked, and `check_exit_code`
            /// promises that it is.
            waitForCommandExit(*command, timeout_command_out, /*close_inputs_first=*/ true,
                {
                    .stderr_sink = {},
                    .check_exit_status = true,
                    .unbounded_status_wait = false,
                    .limit_stdout_drain = false,
                    .check_cancelled = queryKilledCheck(context),
                },
                {.subject = "The command closed its stdout but", .after = ""});
        }

        /// Looks over a reused pooled worker's pipes before this borrow sends it anything.
        ///
        /// The probe when the worker was handed back proved only that it was quiet at that instant.
        /// A command that goes quiet, is pooled, and then writes leaves those bytes waiting for
        /// whoever borrows it next - and this transport has no framing that would let this query
        /// tell them apart from its own answer. So the two pipes are treated very differently.
        ///
        /// Late stdout is fatal. A worker found with it at the borrow, before the source was built,
        /// has already been replaced (`createPipe`); what this catches is a byte that landed after
        /// that look, and there is no replacing the worker from inside the source. The
        /// shared-memory transport can afford to read such a byte and then reject the frame,
        /// because every response carries the id of the request it answers; here the first thing
        /// this query parses would simply be somebody else's rows, silently and plausibly. A query
        /// failed loudly is worth a great deal more than a query answered wrongly, so that is what
        /// happens, and the worker does not go back to the pool.
        ///
        /// Late stderr is not fatal - nothing can mistake it for output - but it must not go
        /// through `stderr_reaction` either, or this query fails for a diagnostic it did not cause.
        /// It is reported against the worker instead, and the pipe is emptied: a worker that filled
        /// it is blocked in `write` and would not read this borrow's input at all.
        void quarantineReusedWorker()
        {
            if (!worker_is_reused || !command)
                return;

            try
            {
                timeout_command_out.clearStderrOfAnEarlierBorrow("ShellCommandSource");
            }
            catch (...)
            {
                /// The earlier query's stderr may still be on the pipe, or the command still blocked
                /// writing it: the worker is not at a known boundary, so it is not built on. The
                /// query fails, and the worker does not go back to the pool.
                command_is_invalid = true;
                throw;
            }

            const auto state = timeout_command_out.channelState();
            if (!state.stdout_has_unread_output && !state.stdout_hung_up)
                return;

            command_is_invalid = true;

            /// So is a stdout that has hung up. `createPipe` replaces a worker it finds exited, but
            /// one that exits between that look and this one is past replacing - the pipes this
            /// source reads are already its - and the request below would go to a process that
            /// cannot answer, failing this query obscurely, on a write to a closed stdin, for an
            /// exit that happened while the worker sat idle. It fails here instead, saying so, and
            /// the worker does not go back to the pool: the next query starts a replacement. Not
            /// `UNSUPPORTED_METHOD`: nothing is wrong with the command's configuration or protocol,
            /// the process went away - most likely it exited.
            if (!state.stdout_has_unread_output)
                throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
                    "The stdout of the process of a pooled command hung up (most likely the process exited) after "
                    "it was checked at the borrow and before this query sent it anything, so it cannot answer; the "
                    "query fails and the process is discarded. The next query starts a replacement");

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
        /// Pending stderr is a reason to discard only under `stderr_reaction` `throw`: there a
        /// line the command wrote after its rows is a verdict on the query that caused it, that
        /// query has already succeeded, and the only way not to pin the verdict on the next one is
        /// not to hand the worker on. Under every other reaction those bytes are log lines (or
        /// nothing), and they are taken off the pipe here and put through the reaction while this
        /// query is still the one on the thread - the right query to attribute them to - and the
        /// worker is kept: a command that logs a line after its rows is not a reason to pay for a
        /// process per call. (Under `none` the drain is also what keeps the pipe from filling up
        /// across borrows until the command blocks in `write`.)
        bool pipeWorkerIsAtACleanBoundary() noexcept
        {
            /// The same rule as the shared-memory probe (`controlChannelIsClean`): a command whose
            /// streams are gone has nothing left to poll - the descriptor numbers this buffer
            /// cached may stand for something else by now - and a worker without a stdout cannot
            /// answer the next query anyway. `waitDrainingOutput` closes stdout on its own where a
            /// command floods it past its rows, which is how this can be reached without a reaped
            /// child.
            if (!command || command->isWaitCalled() || command->isStdoutClosed())
                return false;

            /// The drain polls and reads; either can fail, and this function may not throw. A
            /// worker whose pipes could not even be probed is not one to hand on: discard it.
            try
            {
                static constexpr size_t stderr_drain_budget_ms = 100;
                if (!timeout_command_out.stderrThrows())
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
                logDirtyChannelDiscard("ShellCommandSource", timeout_command_out, state,
                    "The command must write nothing past the rows it was asked for, and must write diagnostics "
                    "before them rather than after.");
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
        void recordPooledResourceUsageNoThrow() const noexcept
        {
            recordPooledReleaseNoThrow(configuration.sampler.get(), is_pooled, "ShellCommandSource");
        }

        ContextPtr context;
        std::string format;
        SharedHeader sample_block;

        ShellCommandSourceConfiguration configuration;

        TimeoutReadBufferFromFileDescriptor timeout_command_out;

        size_t current_read_rows = 0;

        /// Whether the command is a pooled worker, borrowed with `command_holder`. Not asked of
        /// `command_holder` itself, which is empty again once `cleanup` has returned it.
        const bool is_pooled;

        bool check_exit_code = false;
        /// Whether the source serves an executable UDF, whose protocol is one row per input row: only
        /// there is a command that answers with more rows than it was sent wrong. The `executable_pool`
        /// dictionary source and the `ExecutablePool` table engine have always taken a block with more
        /// rows than they asked for as it is.
        const bool is_user_defined_function;

        QueryPipeline pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;

        VectorWithMemoryTracking<ThreadFromGlobalPool> send_data_threads;

        std::mutex send_data_lock;
        std::exception_ptr exception_during_send_data;

        std::atomic<bool> command_is_invalid {false};

        /// Whether `command` is a process that has already served a borrow, and may therefore have
        /// left something on its pipes. A freshly started one cannot have.
        bool worker_is_reused = false;

        /// Whether the send threads have been started, so that requests may have reached the worker.
        bool sending_started = false;

        /// Whether the row count has been read from the output (`answeredInFull`).
        bool row_count_read = false;

        /// Set when the constructor fails before anything was sent to the worker - see there.
        bool worker_untouched = false;

        /// Taken over after every other member, because every other member has to be able to throw
        /// without costing a healthy pooled worker: until this object owns these two they still
        /// belong to `createPipe`, whose scope guard hands the worker back to its holder. The pool's
        /// slot does not depend on this order: `BorrowedShellCommandHolder` returns the holder to
        /// the pool from wherever it is. `timeout_command_out` allocates its buffer and `pipeline`
        /// allocates in its default constructor, so this is not a theoretical ordering. Destroyed
        /// first for the same reason they are constructed last, which is safe: `cleanup` has
        /// already joined the send-data threads and handed the command back, and
        /// ~TimeoutReadBufferFromFileDescriptor deliberately does not touch its descriptors.
        ///
        /// The holder is declared before the command, so the command is destroyed before it: on a
        /// path on which `cleanup` did not get to return the holder, its destructor does, and a
        /// worker that is not going back with it has to die before its slot is released (see
        /// `cleanup`).
        BorrowedShellCommandHolder command_holder;
        std::unique_ptr<ShellCommand> command;
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
}

std::optional<std::string_view> findSharedMemoryConfigurationKey(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, bool except_the_switch)
{
    for (const auto & shared_memory_key : SHARED_MEMORY_CONFIGURATION_KEYS)
    {
        if (except_the_switch && shared_memory_key == SHARED_MEMORY_CONFIGURATION_KEYS.front())
            continue;
        if (config.has(config_prefix + "." + std::string(shared_memory_key)))
            return shared_memory_key;
    }
    return std::nullopt;
}

void checkSharedMemoryIsNotConfigured(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & surface)
{
    if (const auto shared_memory_key = findSharedMemoryConfigurationKey(config, config_prefix, /*except_the_switch=*/ false))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "{}: `{}` is not supported here - the shared-memory transport is available for executable "
            "user defined functions only",
            surface, *shared_memory_key);
}

ShellCommandSourceCoordinator::ShellCommandSourceCoordinator(const Configuration & configuration_)
    : configuration(configuration_)
{
    if (configuration.is_executable_pool)
        process_pool = std::make_shared<ProcessPool>(configuration.pool_size ? configuration.pool_size : std::numeric_limits<size_t>::max());
}

namespace
{

/// How a process is started for `createPipe`. It is given the descriptors the child has to inherit -
/// the shared-memory region, if any - so that the same builder serves both transports: the pipe
/// transport passes none. The command's extra inputs, past the first one on stdin, are written to
/// descriptors 3, 4, ...
ShellCommandHolder::ShellCommandBuilderFunc makeCommandBuilder(
    const ShellCommandSourceCoordinator::Configuration & configuration,
    const std::string & command,
    const VectorWithMemoryTracking<std::string> & arguments,
    size_t num_inputs,
    bool collect_resource_usage)
{
    ShellCommand::Config command_config(command);
    command_config.arguments = arguments;
    command_config.pipe_capacity = configuration.command_pipe_capacity;
    for (size_t i = 1; i < num_inputs; ++i)
        command_config.write_fds.emplace_back(i + 2);

    command_config.terminate_in_destructor_strategy
        = ShellCommand::DestructorStrategy{true /*terminate_in_destructor*/, SIGTERM, configuration.command_termination_timeout_seconds};
    command_config.register_in_udf_process_registry = configuration.is_user_defined_function;

    /// A shared-memory command shares its region with the server, and the server stops charging
    /// for the region when it drops the process. A process that outlived its termination signal,
    /// or a descendant that inherited the descriptor, could still write into the region after that
    /// and take pages nobody is charged for. So such a command runs in a process group of its own,
    /// and its group is ended with `SIGKILL` before the region goes - when the command exits, or
    /// when it is dropped (see `ShellCommand::Config::own_process_group`).
    command_config.own_process_group = configuration.use_shared_memory;
    command_config.collect_resource_usage = collect_resource_usage;

    const bool execute_direct = configuration.execute_direct;
    return [command_config, execute_direct](const std::vector<std::pair<int, int>> & inherited_fds) mutable // STYLE_CHECK_ALLOW_STD_CONTAINERS -- the type of `ShellCommand::Config::inherited_fds`
    {
        command_config.inherited_fds = inherited_fds;
        if (execute_direct)
            return ShellCommand::executeDirect(command_config);
        return ShellCommand::execute(command_config);
    };
}

/// Borrows a holder from the pool, waiting up to `max_command_execution_time`; a holder the pool
/// allocates anew starts its processes with `build_process`. The holder is owned by a
/// `BorrowedShellCommandHolder` from the moment it is borrowed, so whatever throws after that
/// returns it to the pool.
BorrowedShellCommandHolder borrowHolderFromPool(
    const std::shared_ptr<ProcessPool> & process_pool,
    const ShellCommandSourceCoordinator::Configuration & configuration,
    const ShellCommandHolder::ShellCommandBuilderFunc & build_process,
    UDFProcessSubtreeSampler * sampler)
{
    ShellCommandHolderPtr borrowed;
    bool result = process_pool->tryBorrowObject(
        borrowed,
        [&build_process]() { return std::make_unique<ShellCommandHolder>(ShellCommandHolder::ShellCommandBuilderFunc(build_process)); },
        /// Saturated rather than wrapped: a huge `max_command_execution_time`, meant as "wait
        /// forever", must not come out as a fraction of a second. The pool saturates the
        /// deadline it computes from this in turn.
        configuration.max_command_execution_time_seconds > std::numeric_limits<size_t>::max() / 1000
            ? std::numeric_limits<size_t>::max()
            : configuration.max_command_execution_time_seconds * 1000);

    /// Empty when the borrow timed out.
    BorrowedShellCommandHolder holder(std::move(borrowed), process_pool);

    /// Pool wait is frozen here on both the success and the timeout-failure
    /// paths so that `PoolWaitMicroseconds` always records contention for a
    /// slot. Any time spent later in `buildCommand` (cold spawn) lands in
    /// `ElapsedMicroseconds` instead.
    if (sampler)
        sampler->recordPoolWaitDone();

    if (!result)
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Could not get process from pool, max command execution timeout exceeded {} seconds",
            configuration.max_command_execution_time_seconds);

    return holder;
}

/// The process of a borrowed `holder`, for the pipe transport: the one it keeps from an earlier
/// borrow (then `worker_is_reused`), or a fresh one. A reused worker found unfit
/// (`inspectReusedWorker`) is replaced here, before anything is built on it - as on the
/// shared-memory path. A byte that lands on its stdout between this look and the first request is
/// the one case left, and `quarantineReusedWorker` fails the query for it rather than answer it
/// wrongly.
std::unique_ptr<ShellCommand> takePooledWorker(
    ShellCommandHolder & holder,
    const ShellCommandSourceCoordinator::Configuration & configuration,
    UDFProcessSubtreeSampler * sampler,
    bool & worker_is_reused)
{
    /// Asked before building, because building is what consumes the stored process.
    worker_is_reused = holder.hasReturnedCommand();
    std::unique_ptr<ShellCommand> process = holder.buildCommand();

    if (worker_is_reused)
    {
        const auto reason = inspectReusedWorker(*process, configuration.stderr_reaction == ExternalCommandStderrReaction::THROW);
        if (reason != UnfitReusedWorker::NONE)
        {
            /// Killed and reaped at once, nobody being interested in how it exits
            /// (`ShellCommand::discardWithoutGrace`), after the report, which reads what it left on
            /// its pipes and logs it - and on every path: the report can throw (reading, formatting
            /// and logging allocate - `MEMORY_LIMIT_EXCEEDED`), and the worker found unfit must not
            /// go back to the holder for the next borrow to be built on.
            {
                SCOPE_EXIT({
                    process->discardWithoutGrace();
                    process.reset();
                });
                reportUnfitReusedWorker(*process, reason, "ShellCommandSource", "The process of a pooled command", "");
            }
            process = holder.buildCommand();
            worker_is_reused = false;
        }
    }

    /// Borrow acquired: capture pid for procfs sampling. The pre-snapshot
    /// runs here so `clear_refs` and the utime/stime baseline cover only
    /// the work attributable to this borrow.
    ///
    /// `recordPidAcquired` allocates (vector return from `walkSubtree`,
    /// `unordered_set` and `unordered_map` inserts) and is not noexcept.
    /// Sampling is best-effort, so a failure here must not fail the query
    /// that is otherwise ready to run: swallow it and drop one pre baseline.
    if (sampler)
    {
        try
        {
            sampler->recordPidAcquired(process->getPid());
        }
        catch (...)
        {
            tryLogCurrentException("ShellCommandSource");
        }
    }

    return process;
}

/// One task per input pipe, each of which writes its pipe to the command in the input format: the
/// first to its stdin, the rest to descriptors 3, 4, ... The inputs of a pooled worker are left open
/// for the next borrow.
VectorWithMemoryTracking<ShellCommandSource::SendDataTask> makeSendDataTasks(
    ShellCommand & process,
    Pipes & input_pipes,
    const ShellCommandSourceCoordinator::Configuration & configuration,
    const ContextPtr & context,
    UDFProcessSubtreeSampler * sampler,
    bool is_executable_pool)
{
    VectorWithMemoryTracking<ShellCommandSource::SendDataTask> tasks;
    tasks.reserve(input_pipes.size());

    for (size_t i = 0; i < input_pipes.size(); ++i)
    {
        WriteBufferFromFile * write_buffer = nullptr;

        if (i == 0)
        {
            write_buffer = &process.in;
        }
        else
        {
            int descriptor = static_cast<int>(i) + 2;
            auto it = process.write_fds.find(descriptor);
            if (it == process.write_fds.end())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Process does not contain descriptor to write {}", descriptor);

            write_buffer = &it->second;
        }

        int write_buffer_fd = write_buffer->getFD();
        /// Only the primary stdin pipe (i == 0) contributes to InputBytes.
        /// Additional write descriptors carry side-channel data that isn't
        /// part of the UDF's observable input.
        UDFProcessSubtreeSampler * write_sampler = (i == 0) ? sampler : nullptr;
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

    return tasks;
}

}

Pipe ShellCommandSourceCoordinator::createPipe(
    const std::string & command,
    const VectorWithMemoryTracking<std::string> & arguments,
    Pipes && input_pipes,
    Block sample_block,
    ContextPtr context,
    const ShellCommandSourceConfiguration & source_configuration)
{
    UDFProcessSubtreeSampler * sampler = source_configuration.sampler.get();

    if (configuration.use_shared_memory && input_pipes.size() != 1)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Shared-memory mode supports exactly one input pipe, got {}", input_pipes.size());

    const bool is_executable_pool = (process_pool != nullptr);
    auto build_process = makeCommandBuilder(
        configuration, command, arguments, input_pipes.size(), /*collect_resource_usage=*/ !is_executable_pool && sampler != nullptr);

    /// Declared before `process`, so that a process that is still here when this function exits is
    /// destroyed before the holder returns to the pool: a worker that is not going back with the
    /// holder has to die before its slot is released, as in the sources' `cleanup`.
    BorrowedShellCommandHolder process_holder;
    std::unique_ptr<ShellCommand> process;

    /// Whether the process below has already served a borrow, and may therefore have left something
    /// on its pipes - see `ShellCommandSource::quarantineReusedWorker`.
    bool worker_is_reused = false;

    /// A borrowed holder is handed over to the source below, which returns it to the pool when the
    /// query is done. Until that hand-over happens the holder is only this local, and anything can
    /// throw in between - building the command, or a member initializer of the source, which runs
    /// before the source's own constructor cleanup can take over. The holder itself goes back to
    /// the pool all the same, when `process_holder` is destroyed (`BorrowedShellCommandHolder`);
    /// what this guard adds is the worker. It fires only while the local still owns the holder: on
    /// the normal path the source has taken it and this is a no-op.
    SCOPE_EXIT_SAFE({
        /// Hand the worker back to its holder as well when the source never took it: nothing
        /// was sent to it, so it is still at a clean protocol boundary, and killing it would
        /// cost the next query a process spawn over a failure that never reached this one.
        /// Handed back as it was borrowed, as by the source's own constructor cleanup: the send
        /// tasks prepared below make its input descriptors non-blocking, and a worker whose
        /// descriptors cannot be restored is not handed back.
        if (process_holder && process && restoreBlockingInputs(*process))
            process_holder->returnCommand(std::move(process));
    });

    if (is_executable_pool)
        process_holder = borrowHolderFromPool(process_pool, configuration, build_process, sampler);

    if (configuration.use_shared_memory)
    {
        /// The region and the process are both created inside the source, in that order: the
        /// process inherits the region's descriptor at `exec`, so it has to exist first. Doing
        /// it there also means a failure anywhere along the way - reserving a region, charging its
        /// memory, starting the process - is handled by the source's constructor cleanup, which
        /// decides what goes back to the pool with the borrowed holder.
        return createShellCommandSharedMemoryPipe(
            context,
            configuration,
            std::make_shared<const Block>(std::move(sample_block)),
            std::move(build_process),
            std::move(input_pipes[0]),
            source_configuration,
            std::move(process_holder));
    }

    if (is_executable_pool)
    {
        process = takePooledWorker(*process_holder, configuration, sampler, worker_is_reused);
    }
    else
    {
        process = build_process({});

        /// Record the child pid so sampleExecutablePeak can walk the subtree
        /// during IO. No-op when sampler is null.
        if (sampler)
            sampler->recordExecutablePid(process->getPid());
    }

    auto tasks = makeSendDataTasks(*process, input_pipes, configuration, context, sampler, is_executable_pool);

    auto source = std::make_unique<ShellCommandSource>(
        context,
        configuration,
        std::make_shared<const Block>(std::move(sample_block)),
        std::move(process),
        std::move(tasks),
        source_configuration,
        std::move(process_holder),
        worker_is_reused);

    return Pipe(std::move(source));
}

}
