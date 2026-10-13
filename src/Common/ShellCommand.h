#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <string_view>
#include <unordered_map>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>


namespace DB
{


/** Lets you run the command,
  *  read it stdout and stderr; write to stdin;
  *  wait for completion.
  *
  * The implementation is similar to the popen function from POSIX (see libc source code).
  *
  * The most important difference: uses vfork instead of fork.
  * This is done because fork does not work (with a memory shortage error),
  *  with some overcommit settings, if the address space of the process is more than half the amount of available memory.
  * Also, changing memory maps - a fairly resource-intensive operation.
  *
  * The second difference - allows to work simultaneously with stdin, and with stdout, and with stderr of running process,
  *  and also to obtain the return code and completion status.
  */
class ShellCommand final
{
public:
    ~ShellCommand();

    struct DestructorStrategy final
    {
        explicit DestructorStrategy(bool terminate_in_destructor_, int termination_signal_, size_t wait_for_normal_exit_before_termination_seconds_ = 0)
            : terminate_in_destructor(terminate_in_destructor_), termination_signal(termination_signal_)
            , wait_for_normal_exit_before_termination_seconds(wait_for_normal_exit_before_termination_seconds_)
        {
        }

        bool terminate_in_destructor;
        int termination_signal;

        /// If terminate in destructor is true, command will wait until send SIGTERM signal to created process
        size_t wait_for_normal_exit_before_termination_seconds = 0;
    };

    struct Config
    {
        Config(const std::string & command_) /// NOLINT
            : command(command_)
        {}

        Config(const char * command_) /// NOLINT
            : command(command_)
        {}

        std::string command;

        VectorWithMemoryTracking<std::string> arguments;

        std::vector<int> read_fds;

        std::vector<int> write_fds;

        /// Descriptors of this process that the child inherits, as `{child_fd, parent_fd}`: in the
        /// child, `parent_fd` is `dup2`-ed onto `child_fd` before `exec`. The copy is not
        /// close-on-exec whatever the original is, which is the point: the original can - and for
        /// a shared-memory region does - stay close-on-exec, so that no other child started from
        /// another thread in the meantime gets it by accident. Only this child does, and only under
        /// the number it is told: the original is closed in the child before `exec` whether or not
        /// it was close-on-exec, so a caller handing over a pipe end does not leave a second copy
        /// of it in the child under the original number - unless that number is itself where
        /// something else is installed in the child (a standard stream, a `read_fds`/`write_fds`
        /// pipe, another inherited descriptor), in which case what is there now is what stays.
        std::vector<std::pair<int, int>> inherited_fds;

        bool pipe_stdin_only = false;

        size_t pipe_capacity = 0;

        DestructorStrategy terminate_in_destructor_strategy = DestructorStrategy(false, 0);

        /// When true, `tryWaitImpl` reaps with `wait4` and captures the child's
        /// `rusage` (read back via `getChild*`/`wasChildResourceUsageCaptured`).
        /// When false (the default) it reaps with plain `waitpid` and allocates
        /// nothing. Set for executable (non-pool) UDFs, which read the usage.
        bool collect_resource_usage = false;

        /// When true, the child pid is tracked in the global `UDFProcessRegistry`
        /// from spawn until reaped. Off by default; enabled only for executable
        /// and executable_pool UDFs.
        bool register_in_udf_process_registry = false;

        /// When true, the child is started in a process group of its own (`setpgid(0, 0)` before
        /// `exec`), and the group is ended as a whole rather than one process. Whenever the child
        /// exits, its group is sent `SIGKILL` before the child is reaped, so descendants it left
        /// behind die with it; and when the wait for a normal exit in the destructor is over
        /// (`terminate_in_destructor_strategy`), the whole group, the child included, is sent
        /// `SIGKILL` in place of `termination_signal`. For a child that shares something with the
        /// server that the server is about to stop accounting for (the region of a shared-memory
        /// UDF): after `SIGKILL` nothing in the group runs user code again, so nothing can write
        /// into it any more. A descendant that left the group (`setsid`, `setpgid`) is out of reach.
        /// The group is not the server's, so signals sent to the server's group - a terminal's
        /// Ctrl-C, `kill(0, ...)` - do not reach it; the server ends it on its own teardown.
        bool own_process_group = false;
    };

    pid_t getPid() const
    {
        return pid;
    }

    bool isWaitCalled() const
    {
        return wait_called;
    }

    /// What is pending on the pipe `fd` reads from right now, as `poll` revents: a probe that never
    /// waits and never throws. A failed `poll` is logged and answered as `POLLERR`, which every caller
    /// reads as a reason not to build on the process - the fail-closed side.
    static Int16 pendingEvents(int fd) noexcept;

    /// Whether the write end of the pipe `fd` reads from is closed, with nothing left to read.
    static bool pipeHasEnded(int fd) noexcept;

    /// For a command thrown away with nobody interested in how it exits - a pooled worker found
    /// unfit before anything was sent to it, on the thread of a query that did not cause it: no
    /// grace period (`endTerminationGracePeriod`), and the destructor kills it with `SIGKILL` and
    /// reaps it rather than sending `termination_signal`.
    void discardWithoutGrace() noexcept;

    /// Closes every descriptor the command reads its input from: its `stdin` and the extra
    /// `write_fds` of a command that was given more than one input (`executable` with several
    /// input queries). A command is written to exit when its inputs reach EOF, so a teardown that
    /// closed only `stdin` would leave one that waits for all of them running until its
    /// `command_termination_timeout` ran out - and, where the teardown wants its exit status, fail
    /// the query for a status that was a `close` away. Idempotent, and the send tasks that write
    /// into these buffers are joined by the callers before they call this.
    void closeInputs();

    /// Whether the command's `stdout` descriptor is gone. `wait` closes every stream once the
    /// child has been reaped, and `waitDrainingOutput` closes this one on its own where a command
    /// floods its stdout past what was asked of it (see there), without reaping it. A reader that
    /// cached the descriptor number - `TimeoutReadBufferFromFileDescriptor` does, because it may
    /// outlive the streams - must not poll it after that: the number may already stand for an
    /// unrelated descriptor another thread has opened since.
    bool isStdoutClosed() const
    {
        return out.getFD() < 0;
    }

    void setDoNotTerminate()
    {
        do_not_terminate = true;
    }

    /// True once the child has been reaped by `tryWaitImpl` and its
    /// resource usage was captured.
    bool wasChildResourceUsageCaptured() const noexcept;

    /// User-mode CPU time consumed by the reaped child. Zero if
    /// `wasChildResourceUsageCaptured` returns false.
    UInt64 getChildUserTimeMicroseconds() const noexcept;

    /// Kernel-mode CPU time consumed by the reaped child. Zero if
    /// `wasChildResourceUsageCaptured` returns false.
    UInt64 getChildSystemTimeMicroseconds() const noexcept;

    /// Run the command using /bin/sh -c.
    /// If terminate_in_destructor is true, send terminate signal in destructor and don't wait process.
    static std::unique_ptr<ShellCommand> execute(const Config & config);

    /// Run the executable with the specified arguments. `arguments` - without argv[0].
    /// If terminate_in_destructor is true, send terminate signal in destructor and don't wait process.
    static std::unique_ptr<ShellCommand> executeDirect(const Config & config);

    /// Wait for the process to end, throw an exception if the code is not 0 or if the process was not completed by itself.
    void wait();

    /// Wait for the process to finish, see the return code. To throw an exception if the process was not completed independently.
    int tryWait();

    /// Returns if process terminated.
    /// If process terminated, then handle return code.
    bool waitIfProccesTerminated();

    /// Collect the child's `rusage` without inspecting the exit status, so a non-zero
    /// or signalled exit is not raised as an error. Returns whether the child was waited.
    bool tryWaitWithoutStatusCheck();

    /// How long the command is given to exit on its own before it is signalled
    /// (`command_termination_timeout`). Zero means it is given no time at all, which a caller that
    /// wants the exit status has to know about: there is then no difference between a command that
    /// is slow to exit and one that never will, and reporting the second is not warranted.
    UInt64 terminationTimeoutSeconds() const
    {
        return config.terminate_in_destructor_strategy.wait_for_normal_exit_before_termination_seconds;
    }

    using StderrSink = std::function<void(std::string_view)>;
    /// Wait for a child that is being thrown away, discarding whatever it writes to `stdout` and
    /// `stderr` meanwhile, bounded by the shared `command_termination_timeout` budget.
    ///
    /// `wait` cannot be used for this: it reaps first and closes the pipes only afterwards, so a
    /// child that is blocked writing into an output pipe nobody drains any more never gets to
    /// exit, and the wait never returns. Closing its stdin does not help - a process blocked in
    /// `write` is not waiting for input - and no timeout applies to a blocking `waitpid`. Draining
    /// lets such a child run to its own exit, which also keeps the exit status meaningful: closing
    /// the output pipes instead would kill it with `SIGPIPE` and report our own teardown as the
    /// command's failure.
    ///
    /// Only `stdout` and `stderr` are drained. `Config::read_fds` - extra descriptors the child
    /// could also write to - has no user in the codebase; a command that acquires one and floods it
    /// would have to be drained here as well.
    ///
    /// `stderr_sink`, when given, receives what the child writes to `stderr` while it is being
    /// waited for. Without it those bytes are simply dropped, which is all a caller with no use for
    /// them can do - but a caller that has one must be given them: an executable UDF with
    /// `stderr_reaction` `throw` promises that anything the command writes to `stderr` fails the
    /// query, and this is the last stretch in which a command can still write. It is called on this
    /// thread, and an exception from it propagates: the child is then left to the destructor.
    ///
    /// Returns whether the child was reaped. One that is still running when the budget runs out is
    /// left to the destructor, which closes the pipes and signals it. Throws on a non-zero or
    /// signalled exit, exactly like `wait`.
    ///
    /// `check_exit_status` is how a caller with `check_exit_code` switched off waits without
    /// turning the command's own exit code into a query failure: the child is still reaped, and
    /// its output still reaches `stderr_sink`, but a non-zero or signalled exit is not raised.
    ///
    /// `unbounded_status_wait` is for the one caller that used to `wait` for a command with no
    /// bound at all - the non-pooled command, whether its output has ended or a consumer abandoned
    /// it (see `limit_stdout_drain` for how a command that keeps writing is ended then). With it, and with the status
    /// wanted, the wait for the exit status keeps that meaning whatever `command_termination_timeout`
    /// is: the output is drained, so a child blocked in `write` still gets to its exit, but a
    /// command that takes its time over cleanup is waited for exactly as before rather than failed
    /// for it - existing `executable` configurations rely on that. A pooled worker that is being
    /// discarded was never waited for before, and for it the budget is the budget: a worker which
    /// closes its stdout and then never exits must not pin the query, and the pool's slot, forever.
    /// `limit_stdout_drain` is for a consumer that abandoned the output early: stdout is closed after
    /// the stray-output limit even when the status is checked, and also once
    /// `command_termination_timeout` has passed, however little arrived - so that a command that
    /// keeps writing dies on `SIGPIPE`, while one that has stopped writing is not affected. That
    /// `SIGPIPE` is not raised even with the status checked: it was caused here, after the consumer
    /// had what it wanted. Any other status is checked as usual. A command that neither writes nor
    /// exits is waited for, with `unbounded_status_wait`, until `check_cancelled` throws.
    /// `check_cancelled` may throw to interrupt the wait; cancellation also ends the termination
    /// grace period so the destructor can stop the command promptly.
    struct WaitDrainingOptions
    {
        StderrSink stderr_sink;
        bool check_exit_status = true;
        bool unbounded_status_wait = false;
        bool limit_stdout_drain = false;
        std::function<void()> check_cancelled;
    };

    bool waitDrainingOutput(const WaitDrainingOptions & options);

    WriteBufferFromFile in;        /// If the command reads from stdin, do not forget to call in.close() after writing all the data there.
    ReadBufferFromFile out;
    ReadBufferFromFile err;

    std::unordered_map<int, ReadBufferFromFile> read_fds;
    std::unordered_map<int, WriteBufferFromFile> write_fds;
private:

    pid_t pid;
    Config config;
    bool wait_called = false;
    bool do_not_terminate = false;

    /// Whether the child has been reaped, or found not to be a child of this process any more
    /// (`ECHILD`). Not the same as `wait_called`, which a bounded wait sets before it starts and
    /// leaves set when it runs out with the child still alive. Once this is true, the child's pid -
    /// and the number of the group it leads, with `Config::own_process_group` - may belong to
    /// somebody else, so it is neither signalled nor waited for.
    bool child_reaped = false;

    /// Set by `discardWithoutGrace`.
    bool reap_on_destruction = false;

    /// Set by `killAndReapNoThrow`, which runs once.
    bool kill_and_reap_done = false;

    /// Waits for the child to exit on its own within what is left of the shared termination
    /// deadline (`remainingTerminationTimeoutMs`). Returns whether it did.
    bool waitForExitWithinGracePeriod();

    /// Sends `termination_signal` to the child and hands it over to `ShellCommandsHolder`, which
    /// reaps it once it goes on the signal.
    void signalAndHandOverChild();

    /// Hands a child this object will not reap to `ShellCommandsHolder`, which reaps it once it
    /// exits. False if the holder would not take it (it could not be registered): the child is then
    /// still this object's, as if nothing had been called.
    bool handOverChildToReaper() noexcept;

    /// For a child that has `SIGKILL` and was not reaped within its bound: handed to the reaper, or,
    /// if the reaper would not take it, forgotten - it is dead or as good as, so its registry entry
    /// goes, and the zombie is logged.
    void handOverOrForgetKilledChild() noexcept;

    /// `tryWait`, logging what it throws: for the destructor's path.
    void tryWaitNoThrow() noexcept;

    /// Records that the child has been reaped or is not a child of this process any more
    /// (`child_reaped`): from then on it is neither waited for nor signalled - `wait_called` is set
    /// too, which is what the destructor looks at - and it leaves `UDFProcessRegistry`, whose
    /// sampling would otherwise follow its pid to whatever process gets the number next.
    void forgetChild();

    /// Sends `SIGKILL` to the process group of a child that has exited and is not reaped yet
    /// (`Config::own_process_group`), so that what it left behind in its group dies with it.
    void killGroupOfExitedChild();

    /// CPU time of the reaped child, taken from `wait4` rusage and stored by value
    /// at reap time. The reap path performs no allocation, so a memory-limit
    /// `exception` can never fail a query whose child has already exited.
    bool child_resource_usage_captured = false;
    UInt64 child_user_time_us = 0;
    UInt64 child_system_time_us = 0;

    /// Identifies which incarnation of the pid this wrapper owns, so reap removes
    /// only its own entry and never one belonging to a later process that reused
    /// the pid. Stamped by `UDFProcessRegistry::add` at spawn; 0 for non-UDF
    /// commands, which never register.
    UInt64 udf_registry_generation = 0;

    /// Absolute monotonic deadline (ns, `clock_gettime_ns`) shared by the cleanup-side
    /// wait and the destructor-side wait so `command_termination_timeout` bounds their
    /// SUM, not each separately. Armed lazily by `remainingTerminationTimeoutMs`; 0 means
    /// not yet armed.
    UInt64 termination_deadline_ns = 0;

    /// Milliseconds left until the shared termination deadline, arming it on the first
    /// call from `wait_for_normal_exit_before_termination_seconds`. Returns 0 once the
    /// deadline has passed, so both wait paths stop at one shared budget.
    UInt64 remainingTerminationTimeoutMs();

    /// Ends the grace period `command_termination_timeout` gives the command to exit on its own:
    /// the destructor then signals it at once instead of waiting. Private: a caller that ends it
    /// for a command it discards wants `discardWithoutGrace`, which also has the command killed
    /// and reaped rather than signalled and left a zombie.
    void endTerminationGracePeriod() noexcept;

    ShellCommand(pid_t pid_, int & in_fd_, int & out_fd_, int & err_fd_, const Config & config);

    bool tryWaitProcessWithTimeout(size_t timeout_in_milliseconds);

    /// Sends `SIGKILL` to the child - and to its whole process group, with `whole_group` - and reaps
    /// it. Only while it is still an unreaped child of this process: otherwise its pid may belong to
    /// somebody else, and it is only forgotten (`forgetChild`).
    void killAndReapNoThrow(bool whole_group) noexcept;
    struct tryWaitResult;

    struct TryWaitOptions
    {
        /// Whether to wait for the child to exit, rather than only to reap it if it has.
        bool blocking = true;

        /// `false` reaps the child without decoding its exit status, so a non-zero or signalled
        /// exit is not raised as an error.
        bool check_exit_status = true;

        /// `false` leaves the child's pipes open after it has been reaped. Only
        /// `waitDrainingOutput` wants that, and it wants it badly: reaping closes the descriptors,
        /// and whatever the child had already written and not yet been read is gone with them -
        /// including, under `stderr_reaction` `throw`, the diagnostic the query was supposed to fail
        /// on. The caller closes them itself once it has read them to the end.
        bool close_streams = true;
    };

    tryWaitResult tryWaitImpl(const TryWaitOptions & options);

    /// Closes everything `tryWaitImpl` would have closed on a reap.
    void closeStreams();

    /// The same without throwing, each stream on its own: for the destructor's path.
    void closeStreamsNoThrow() noexcept;

    /// Reads both output pipes until they end or `budget_ms` runs out, handing what comes off
    /// stderr to `stderr_sink`. Does not reap and does not touch the termination deadline.
    /// Reads exactly the bytes the pipes hold at this moment (`FIONREAD`), with no deadline: they
    /// are there, so the reads cannot block, and nothing may cost them - see `waitDrainingOutput`.
    /// `stdout_bytes_drained`, if given, is increased by what is taken off `stdout`, as by `drainOutputPipes`.
    void readBufferedOutput(int (&drain_fds)[2], const StderrSink & stderr_sink, size_t * stdout_bytes_drained = nullptr) const;

    /// Reads what the pipes hold, for at most `budget_ms`, handing what comes off stderr to
    /// `stderr_sink`. `stdout_bytes_drained`, if given, is increased by what is taken off `stdout`.
    /// `exit_fd`, if given, is the child's `pidfd`: the drain returns as soon as the child exits, and
    /// waits for that instead of coming back in short steps.
    void drainOutputPipes(
        int (&drain_fds)[2], const StderrSink & stderr_sink, UInt64 budget_ms, size_t * stdout_bytes_drained, int exit_fd) const;

    /// One `read` of `drain_fds[i]` (0 is stdout, 1 is stderr) into `buffer`, retried on `EINTR`:
    /// what it takes goes to `stderr_sink` or is counted into `stdout_bytes_drained`. Returns the
    /// bytes read; on EOF or an error that will not go away the descriptor is dropped from
    /// `drain_fds`, on `EAGAIN` it is kept.
    size_t readPipeOnce(
        int (&drain_fds)[2], size_t i, char * buffer, size_t size, const StderrSink & stderr_sink, size_t * stdout_bytes_drained) const;

    /// Whether both output pipes still in `drain_fds` have hung up with nothing left to read.
    bool outputPipesHaveEnded(const int (&drain_fds)[2]) const;

    /// Calls `check_cancelled` of `waitDrainingOutput`, if there is one. If it throws, the
    /// termination grace period is ended first, so that the destructor can stop the command promptly.
    void checkCancelled(const std::function<void()> & check_cancelled);

    /// Closes `stdout` for good once `waitDrainingOutput` no longer wants what comes off it (see
    /// the definition), and drops it from `drain_fds`. Returns whether it was closed here - a
    /// `stdout` that the command has closed itself is only dropped.
    bool closeAbandonedStdout(int (&drain_fds)[2], const WaitDrainingOptions & options, size_t stdout_bytes_drained);

    /// Waits up to `timeout_ms` for the child to exit, leaving it unreaped - on `exit_fd`, the
    /// child's `pidfd`, when there is one.
    void waitForExitLeavingUnreaped(int exit_fd, UInt64 timeout_ms) const;

    /// The short grace `waitDrainingOutput` gives a child whose output has ended once its budget
    /// has run out: arms `exit_grace_deadline_ns` on the first call, and waits one step for the
    /// exit. Returns whether there was any grace left, that is, whether to go on waiting.
    bool waitInExitGrace(UInt64 & exit_grace_deadline_ns, int exit_fd) const;

    void handleProcessRetcode(int retcode) const;

    /// Decodes a raw `waitpid` status and throws for anything but a clean zero exit. Separate from
    /// the reap so a caller can read the child's last words off its pipes before this can throw.
    void handleProcessStatus(int status) const;

    static LoggerPtr getLogger();

    /// Print command name and the list of arguments to log. NOTE: No escaping of arguments is performed.
    static void logCommand(const char * filename, char * const argv[]);

    static std::unique_ptr<ShellCommand> executeImpl(const char * filename, char * const argv[], const Config & config);

    /// In the parent, after `vfork`: reads the report of a child that failed before `exec` off the
    /// read end of its error pipe, `report_fd`, and throws `CANNOT_CREATE_CHILD_PROCESS` for it.
    /// Returns if the child has started.
    /// `own_process_group`: the child leads a group of its own, which is ended with it if it is
    /// killed here.
    static void checkChildSetupReport(pid_t child_pid, int report_fd, bool own_process_group);
    /// Kills and reaps a child that has no `ShellCommand` to own it (its start could not be
    /// established, or the constructor threw): the group it leads too, with `whole_group`. A child
    /// not reaped within a bound is handed to the reaper (`ShellCommandsHolder::addSignalledChild`).
    static void killAndReapOrphan(pid_t child_pid, bool whole_group) noexcept;
    /// What `killAndReapNoThrow` and `killAndReapOrphan` share: sends `SIGKILL` to the child - and to
    /// the group it leads, with `whole_group` - and waits a bound for it to be reaped. Returns whether
    /// it is gone (reaped, or not a child of this process any more); otherwise it is still the
    /// caller's, to hand over to the reaper.
    static bool killAndWaitForChild(pid_t child_pid, bool whole_group);

    /// The read of the child's error pipe failed with `read_error`: whether the child started is
    /// established from the child itself. Throws unless it is running.
    static void handleUnreadableChildReport(pid_t child_pid, int read_error, bool own_process_group);
};


}
