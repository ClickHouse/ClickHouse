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
    };

    pid_t getPid() const
    {
        return pid;
    }

    bool isWaitCalled() const
    {
        return wait_called;
    }

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
    /// `no_grace_means_unbounded` is for the one caller that used to `wait` for a command with no
    /// bound at all - the non-pooled command whose output has ended - and for which a grace period
    /// of zero would otherwise turn that wait into a single probe that fails nondeterministically:
    /// with it, zero keeps the old meaning, and the wait for the exit status is unbounded. A
    /// pooled worker that is being discarded was never waited for before, and for it zero means
    /// what it means everywhere else: no grace, signal at once - so that a worker which closes
    /// its stdout and then never exits cannot pin the query, and the pool's slot, forever.
    /// How long the command is given to exit on its own before it is signalled
    /// (`command_termination_timeout`). Zero means it is given no time at all, which a caller that
    /// wants the exit status has to know about: there is then no difference between a command that
    /// is slow to exit and one that never will, and reporting the second is not warranted.
    UInt64 terminationTimeoutSeconds() const
    {
        return config.terminate_in_destructor_strategy.wait_for_normal_exit_before_termination_seconds;
    }

    using StderrSink = std::function<void(std::string_view)>;
    bool waitDrainingOutput(const StderrSink & stderr_sink = {}, bool check_exit_status = true, bool no_grace_means_unbounded = false);

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

    ShellCommand(pid_t pid_, int & in_fd_, int & out_fd_, int & err_fd_, const Config & config);

    bool tryWaitProcessWithTimeout(size_t timeout_in_seconds);
    struct tryWaitResult;

    /// `close_streams = false` leaves the child's pipes open after it has been reaped. Only
    /// `waitDrainingOutput` wants that, and it wants it badly: reaping closes the descriptors, and
    /// whatever the child had already written and not yet been read is gone with them - including,
    /// under `stderr_reaction` `throw`, the diagnostic the query was supposed to fail on. The
    /// caller closes them itself once it has read them to the end.
    tryWaitResult tryWaitImpl(bool blocking, bool check_exit_status = true, bool close_streams = true);

    /// Closes everything `tryWaitImpl` would have closed on a reap.
    void closeStreams();

    /// Reads both output pipes until they end or `budget_ms` runs out, handing what comes off
    /// stderr to `stderr_sink`. Does not reap and does not touch the termination deadline.
    /// Reads exactly the bytes the pipes hold at this moment (`FIONREAD`), with no deadline: they
    /// are there, so the reads cannot block, and nothing may cost them - see `waitDrainingOutput`.
    void readBufferedOutput(int (&drain_fds)[2], const StderrSink & stderr_sink) const;

    /// Reads what the pipes hold, for at most `budget_ms`. With `budget_is_quiet_time` the budget
    /// is spent only while nothing arrives: every read pushes the deadline forward, so what is
    /// already in the pipes is read whole however long that takes, and only the wait for more is
    /// bounded - within a hard cap of `max_total_ms`, for a grandchild that keeps the pipe fed.
    /// `stdout_bytes_drained`, if given, is increased by the number of bytes taken off `stdout`.
    void drainOutputPipes(
        int (&drain_fds)[2],
        const StderrSink & stderr_sink,
        UInt64 budget_ms,
        bool budget_is_quiet_time = false,
        UInt64 max_total_ms = 0,
        size_t * stdout_bytes_drained = nullptr) const;

    void handleProcessRetcode(int retcode) const;

    /// Decodes a raw `waitpid` status and throws for anything but a clean zero exit. Separate from
    /// the reap so a caller can read the child's last words off its pipes before this can throw.
    void handleProcessStatus(int status) const;

    static LoggerPtr getLogger();

    /// Print command name and the list of arguments to log. NOTE: No escaping of arguments is performed.
    static void logCommand(const char * filename, char * const argv[]);

    static std::unique_ptr<ShellCommand> executeImpl(const char * filename, char * const argv[], const Config & config);
};


}
