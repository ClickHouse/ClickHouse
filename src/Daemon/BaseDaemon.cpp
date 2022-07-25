#ifdef HAS_RESERVED_IDENTIFIER
#    pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#include <Daemon/BaseDaemon.h>
#include <Daemon/SentryWriter.h>

#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>
#if defined(OS_LINUX)
    #include <sys/prctl.h>
#endif
#include <cerrno>
#include <cstring>
#include <csignal>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <typeinfo>
#include <base/scope_guard.h>

#include <Poco/ErrorHandler.h>
#include <Poco/Exception.h>
#include <Poco/Message.h>
#include <Poco/Util/Application.h>

#include <base/argsToConfig.h>
#include <base/coverage.h>
#include <base/getThreadId.h>
#include <base/sleep.h>
#include <Common/ErrorHandlers.h>

#include <filesystem>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteHelpers.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Elf.h>
#include <Common/Exception.h>
#include <Common/PipeFDs.h>
#include <Common/StackTrace.h>
#include <Common/SymbolIndex.h>
#include <Common/getExecutablePath.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnJSONPatternFormatter.h>
#include <Loggers/OwnPatternFormatter.h>

#include <Common/config_version.h>

#if defined(OS_DARWIN)
#    pragma GCC diagnostic ignored "-Wunused-macros"
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#    define _XOPEN_SOURCE 700 // ucontext is not available without _XOPEN_SOURCE
#endif
#include <ucontext.h>

namespace fs = std::filesystem;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_SET_SIGNAL_HANDLER;
        extern const int CANNOT_SEND_SIGNAL;
    }
}

DB::PipeFDs signal_pipe;


/** Reset signal handler to the default and send signal to itself.
  * It's called from user signal handler to write core dump.
  */
static void call_default_signal_handler(int sig)
{
    if (SIG_ERR == signal(sig, SIG_DFL))
        DB::throwFromErrno("Cannot set signal handler.", DB::ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);

    if (0 != raise(sig))
        DB::throwFromErrno("Cannot send signal.", DB::ErrorCodes::CANNOT_SEND_SIGNAL);
}

static const size_t signal_pipe_buf_size
    = sizeof(int) + sizeof(siginfo_t) + sizeof(ucontext_t *) + sizeof(StackTrace) + sizeof(UInt32) + sizeof(void *);

using signal_function = void(int, siginfo_t *, void *);

static void writeSignalIDtoSignalPipe(int sig)
{
    auto saved_errno = errno; /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);
    DB::writeBinary(sig, out);
    out.next();

    errno = saved_errno;
}

/** Signal handler for HUP */
static void closeLogsSignalHandler(int sig, siginfo_t *, void *)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    writeSignalIDtoSignalPipe(sig);
}

static void terminateRequestedSignalHandler(int sig, siginfo_t *, void *)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    writeSignalIDtoSignalPipe(sig);
}


/** Handler for "fault" or diagnostic signals. Send data about fault to separate thread to write into log.
  */
static void signalHandler(int sig, siginfo_t * info, void * context)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno; /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const ucontext_t * signal_context = reinterpret_cast<ucontext_t *>(context);
    const StackTrace stack_trace(*signal_context);

    DB::writeBinary(sig, out);
    DB::writePODBinary(*info, out);
    DB::writePODBinary(signal_context, out);
    DB::writePODBinary(stack_trace, out);
    DB::writeBinary(static_cast<UInt32>(getThreadId()), out);
    DB::writePODBinary(DB::current_thread, out);

    out.next();

    if (sig != SIGTSTP) /// This signal is used for debugging.
    {
        /// The time that is usually enough for separate thread to print info into log.
        sleepForSeconds(20); /// FIXME: use some feedback from threads that process stacktrace
        call_default_signal_handler(sig);
    }

    errno = saved_errno;
}


/// Avoid link time dependency on DB/Interpreters - will use this function only when linked.
__attribute__((__weak__)) void collectCrashLog(Int32 signal, UInt64 thread_id, const String & query_id, const StackTrace & stack_trace);


/** The thread that read info about signal or std::terminate from pipe.
  * On HUP, close log files (for new files to be opened later).
  * On information about std::terminate, write it to log.
  * On other signals, write info to log.
  */
class SignalListener : public Poco::Runnable
{
public:
    enum Signals : int
    {
        StdTerminate = -1,
        StopThread = -2,
        SanitizerTrap = -3,
    };

    explicit SignalListener(BaseDaemon & daemon_) : log(&Poco::Logger::get("BaseDaemon")), daemon(daemon_) { }

    void run() override
    {
        static_assert(PIPE_BUF >= 512);
        static_assert(
            signal_pipe_buf_size <= PIPE_BUF,
            "Only write of PIPE_BUF to pipe is atomic and the minimal known PIPE_BUF across supported platforms is 512");
        char buf[signal_pipe_buf_size];
        DB::ReadBufferFromFileDescriptor in(signal_pipe.fds_rw[0], signal_pipe_buf_size, buf);

        while (!in.eof())
        {
            int sig = 0;
            DB::readBinary(sig, in);
            // We may log some specific signals afterwards, with different log
            // levels and more info, but for completeness we log all signals
            // here at trace level.
            // Don't use strsignal here, because it's not thread-safe.
            LOG_TRACE(log, "Received signal {}", sig);

            if (sig == Signals::StopThread)
            {
                LOG_INFO(log, "Stop SignalListener thread");
                break;
            }
            else if (sig == SIGHUP)
            {
                LOG_DEBUG(log, "Received signal to close logs.");
                BaseDaemon::instance().closeLogs(BaseDaemon::instance().logger());
                LOG_INFO(log, "Opened new log file after received signal.");
            }
            else if (sig == Signals::StdTerminate)
            {
                UInt32 thread_num;
                std::string message;

                DB::readBinary(thread_num, in);
                DB::readBinary(message, in);

                onTerminate(message, thread_num);
            }
            else if (sig == SIGINT || sig == SIGQUIT || sig == SIGTERM)
            {
                daemon.handleSignal(sig);
            }
            else
            {
                siginfo_t info{};
                ucontext_t * context{};
                StackTrace stack_trace(NoCapture{});
                UInt32 thread_num{};
                DB::ThreadStatus * thread_ptr{};

                if (sig != SanitizerTrap)
                {
                    DB::readPODBinary(info, in);
                    DB::readPODBinary(context, in);
                }

                DB::readPODBinary(stack_trace, in);
                DB::readBinary(thread_num, in);
                DB::readPODBinary(thread_ptr, in);

                /// This allows to receive more signals if failure happens inside onFault function.
                /// Example: segfault while symbolizing stack trace.
                std::thread([=, this] { onFault(sig, info, context, stack_trace, thread_num, thread_ptr); }).detach();
            }
        }
    }

private:
    Poco::Logger * log;
    BaseDaemon & daemon;

    void onTerminate(std::string_view message, UInt32 thread_num) const
    {
        size_t pos = message.find('\n');

        LOG_FATAL(
            log,
            "(version {}{}, {}) (from thread {}) {}",
            VERSION_STRING,
            VERSION_OFFICIAL,
            daemon.build_id_info,
            thread_num,
            message.substr(0, pos));

        /// Print trace from std::terminate exception line-by-line to make it easy for grep.
        while (pos != std::string_view::npos)
        {
            ++pos;
            size_t next_pos = message.find('\n', pos);
            size_t size = next_pos;
            if (next_pos != std::string_view::npos)
                size = next_pos - pos;

            LOG_FATAL(log, "{}", message.substr(pos, size));
            pos = next_pos;
        }
    }

    void onFault(
        int sig,
        const siginfo_t & info,
        ucontext_t * context,
        const StackTrace & stack_trace,
        UInt32 thread_num,
        DB::ThreadStatus * thread_ptr) const
    {
        DB::ThreadStatus thread_status;

        String query_id;
        String query;

        /// Send logs from this thread to client if possible.
        /// It will allow client to see failure messages directly.
        if (thread_ptr)
        {
            query_id = std::string(thread_ptr->getQueryId());

            if (auto thread_group = thread_ptr->getThreadGroup())
            {
                query = thread_group->one_line_query;
            }

            if (auto logs_queue = thread_ptr->getInternalTextLogsQueue())
                DB::CurrentThread::attachInternalTextLogsQueue(logs_queue, DB::LogsLevel::trace);
        }

        LOG_FATAL(log, "########################################");

        if (query_id.empty())
        {
            LOG_FATAL(
                log,
                "(version {}{}, {}) (from thread {}) (no query) Received signal {} ({})",
                VERSION_STRING,
                VERSION_OFFICIAL,
                daemon.build_id_info,
                thread_num,
                strsignal(sig),
                sig);
        }
        else
        {
            LOG_FATAL(
                log,
                "(version {}{}, {}) (from thread {}) (query_id: {}) (query: {}) Received signal {} ({})",
                VERSION_STRING,
                VERSION_OFFICIAL,
                daemon.build_id_info,
                thread_num,
                query_id,
                query,
                strsignal(sig),
                sig);
        }

        String error_message;

        if (sig != SanitizerTrap)
            error_message = signalToErrorMessage(sig, info, *context);
        else
            error_message = "Sanitizer trap.";

        LOG_FATAL(log, fmt::runtime(error_message));

        if (stack_trace.getSize())
        {
            /// Write bare stack trace (addresses) just in case if we will fail to print symbolized stack trace.
            /// NOTE: This still require memory allocations and mutex lock inside logger.
            ///       BTW we can also print it to stderr using write syscalls.

            std::stringstream bare_stacktrace; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            bare_stacktrace << "Stack trace:";
            for (size_t i = stack_trace.getOffset(); i < stack_trace.getSize(); ++i)
                bare_stacktrace << ' ' << stack_trace.getFramePointers()[i];

            LOG_FATAL(log, fmt::runtime(bare_stacktrace.str()));
        }

        /// Write symbolized stack trace line by line for better grep-ability.
        stack_trace.toStringEveryLine([&](const std::string & s) { LOG_FATAL(log, fmt::runtime(s)); });

#if defined(OS_LINUX)
        /// Write information about binary checksum. It can be difficult to calculate, so do it only after printing stack trace.
        /// Please keep the below log messages in-sync with the ones in programs/server/Server.cpp
        String calculated_binary_hash = getHashOfLoadedBinaryHex();
        if (daemon.stored_binary_hash.empty())
        {
            LOG_FATAL(log, "Integrity check of the executable skipped because the reference checksum could not be read."
                " (calculated checksum: {})", calculated_binary_hash);
        }
        else if (calculated_binary_hash == daemon.stored_binary_hash)
        {
            LOG_FATAL(log, "Integrity check of the executable successfully passed (checksum: {})", calculated_binary_hash);
        }
        else
        {
            LOG_FATAL(log, "Calculated checksum of the executable ({0}) does not correspond"
                " to the reference checksum stored in the executable ({1})."
                " This may indicate one of the following:"
                " - the executable was changed just after startup;"
                " - the executable was corrupted on disk due to faulty hardware;"
                " - the loaded executable was corrupted in memory due to faulty hardware;"
                " - the file was intentionally modified;"
                " - a logical error in the code."
                , calculated_binary_hash, daemon.stored_binary_hash);
        }
#endif

        /// Write crash to system.crash_log table if available.
        if (collectCrashLog)
            collectCrashLog(sig, thread_num, query_id, stack_trace);

        /// Send crash report to developers (if configured)
        if (sig != SanitizerTrap)
            SentryWriter::onFault(sig, error_message, stack_trace);

        /// When everything is done, we will try to send these error messages to client.
        if (thread_ptr)
            thread_ptr->onFatalError();
    }
};


#if defined(SANITIZER)
extern "C" void __sanitizer_set_death_callback(void (*)());

static void sanitizerDeathCallback()
{
    DENY_ALLOCATIONS_IN_SCOPE;
    /// Also need to send data via pipe. Otherwise it may lead to deadlocks or failures in printing diagnostic info.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const StackTrace stack_trace;

    int sig = SignalListener::SanitizerTrap;
    DB::writeBinary(sig, out);
    DB::writePODBinary(stack_trace, out);
    DB::writeBinary(UInt32(getThreadId()), out);
    DB::writePODBinary(DB::current_thread, out);

    out.next();

    /// The time that is usually enough for separate thread to print info into log.
    sleepForSeconds(20);
}
#endif


/** To use with std::set_terminate.
  * Collects slightly more info than __gnu_cxx::__verbose_terminate_handler,
  *  and send it to pipe. Other thread will read this info from pipe and asynchronously write it to log.
  * Look at libstdc++-v3/libsupc++/vterminate.cc for example.
  */
[[noreturn]] static void terminate_handler()
{
    static thread_local bool terminating = false;
    if (terminating)
        abort();

    terminating = true;

    std::string log_message;

    if (std::current_exception())
        log_message = "Terminate called for uncaught exception:\n" + DB::getCurrentExceptionMessage(true);
    else
        log_message = "Terminate called without an active exception";

    /// POSIX.1 says that write(2)s of less than PIPE_BUF bytes must be atomic - man 7 pipe
    /// And the buffer should not be too small because our exception messages can be large.
    static constexpr size_t buf_size = PIPE_BUF;

    if (log_message.size() > buf_size - 16)
        log_message.resize(buf_size - 16);

    char buf[buf_size];
    DB::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], buf_size, buf);

    DB::writeBinary(static_cast<int>(SignalListener::StdTerminate), out);
    DB::writeBinary(static_cast<UInt32>(getThreadId()), out);
    DB::writeBinary(log_message, out);
    out.next();

    abort();
}


static std::string createDirectory(const std::string & file)
{
    fs::path path = fs::path(file).parent_path();
    if (path.empty())
        return "";
    fs::create_directories(path);
    return path;
}


static bool tryCreateDirectories(Poco::Logger * logger, const std::string & path)
{
    try
    {
        fs::create_directories(path);
        return true;
    }
    catch (...)
    {
        LOG_WARNING(logger, "{}: when creating {}, {}", __PRETTY_FUNCTION__, path, DB::getCurrentExceptionMessage(true));
    }
    return false;
}


void BaseDaemon::reloadConfiguration()
{
    /** If the program is not run in daemon mode and 'config-file' is not specified,
      *  then we use config from 'config.xml' file in current directory,
      *  but will log to console (or use parameters --log-file, --errorlog-file from command line)
      *  instead of using files specified in config.xml.
      * (It's convenient to log in console when you start server without any command line parameters.)
      */
    config_path = config().getString("config-file", getDefaultConfigFileName());
    DB::ConfigProcessor config_processor(config_path, false, true);
    config_processor.setConfigPath(fs::path(config_path).parent_path());
    loaded_config = config_processor.loadConfig(/* allow_zk_includes = */ true);

    if (last_configuration != nullptr)
        config().removeConfiguration(last_configuration);
    last_configuration = loaded_config.configuration.duplicate();
    config().add(last_configuration, PRIO_DEFAULT, false);
}


BaseDaemon::BaseDaemon() = default;


BaseDaemon::~BaseDaemon()
{
    writeSignalIDtoSignalPipe(SignalListener::StopThread);
    signal_listener_thread.join();
    /// Reset signals to SIG_DFL to avoid trying to write to the signal_pipe that will be closed after.
    for (int sig : handled_signals)
        if (SIG_ERR == signal(sig, SIG_DFL))
            DB::throwFromErrno("Cannot set signal handler.", DB::ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);
    signal_pipe.close();
}


void BaseDaemon::terminate()
{
    if (::raise(SIGTERM) != 0)
        throw Poco::SystemException("cannot terminate process");
}

void BaseDaemon::kill()
{
    dumpCoverageReportIfPossible();
    pid_file.reset();
    /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
    /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
    _exit(128 + SIGKILL);
}

std::string BaseDaemon::getDefaultCorePath() const
{
    return "/opt/cores/";
}

std::string BaseDaemon::getDefaultConfigFileName() const
{
    return "config.xml";
}

void BaseDaemon::closeFDs()
{
#if defined(OS_FREEBSD) || defined(OS_DARWIN)
    fs::path proc_path{"/dev/fd"};
#else
    fs::path proc_path{"/proc/self/fd"};
#endif
    if (fs::is_directory(proc_path)) /// Hooray, proc exists
    {
        /// in /proc/self/fd directory filenames are numeric file descriptors.
        /// Iterate directory separately from closing fds to avoid closing iterated directory fd.
        std::vector<int> fds;
        for (const auto & path : fs::directory_iterator(proc_path))
            fds.push_back(DB::parse<int>(path.path().filename()));

        for (const auto & fd : fds)
        {
            if (fd > 2 && fd != signal_pipe.fds_rw[0] && fd != signal_pipe.fds_rw[1])
                ::close(fd);
        }
    }
    else
    {
        int max_fd = -1;
#if defined(_SC_OPEN_MAX)
        max_fd = sysconf(_SC_OPEN_MAX);
        if (max_fd == -1)
#endif
            max_fd = 256; /// bad fallback
        for (int fd = 3; fd < max_fd; ++fd)
            if (fd != signal_pipe.fds_rw[0] && fd != signal_pipe.fds_rw[1])
                ::close(fd);
    }
}

namespace
{
/// In debug version on Linux, increase oom score so that clickhouse is killed
/// first, instead of some service. Use a carefully chosen random score of 555:
/// the maximum is 1000, and chromium uses 300 for its tab processes. Ignore
/// whatever errors that occur, because it's just a debugging aid and we don't
/// care if it breaks.
#if defined(OS_LINUX) && !defined(NDEBUG)
void debugIncreaseOOMScore()
{
    const std::string new_score = "555";
    try
    {
        DB::WriteBufferFromFile buf("/proc/self/oom_score_adj");
        buf.write(new_score.c_str(), new_score.size());
        buf.close();
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(&Poco::Logger::root(), "Failed to adjust OOM score: '{}'.", e.displayText());
        return;
    }
    LOG_INFO(&Poco::Logger::root(), "Set OOM score adjustment to {}", new_score);
}
#else
void debugIncreaseOOMScore()
{
}
#endif
}

void BaseDaemon::initialize(Application & self)
{
    closeFDs();

    ServerApplication::initialize(self);

    /// now highest priority (lowest value) is PRIO_APPLICATION = -100, we want higher!
    argsToConfig(argv(), config(), PRIO_APPLICATION - 100);

    bool is_daemon = config().getBool("application.runAsDaemon", false);
    if (is_daemon)
    {
        /** When creating pid file and looking for config, will search for paths relative to the working path of the program when started.
          */
        std::string path = fs::path(config().getString("application.path")).replace_filename("");
        if (0 != chdir(path.c_str()))
            throw Poco::Exception("Cannot change directory to " + path);
    }

    reloadConfiguration();

    /// This must be done before creation of any files (including logs).
    mode_t umask_num = 0027;
    if (config().has("umask"))
    {
        std::string umask_str = config().getString("umask");
        std::stringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        stream << umask_str;
        stream >> std::oct >> umask_num;
    }
    umask(umask_num);

    DB::ConfigProcessor(config_path).savePreprocessedConfig(loaded_config, "");

    /// Write core dump on crash.
    {
        struct rlimit rlim;
        if (getrlimit(RLIMIT_CORE, &rlim))
            throw Poco::Exception("Cannot getrlimit");
        /// 1 GiB by default. If more - it writes to disk too long.
        rlim.rlim_cur = config().getUInt64("core_dump.size_limit", 1024 * 1024 * 1024);

        if (rlim.rlim_cur && setrlimit(RLIMIT_CORE, &rlim))
        {
            /// It doesn't work under address/thread sanitizer. http://lists.llvm.org/pipermail/llvm-bugs/2013-April/027880.html
            std::cerr << "Cannot set max size of core file to " + std::to_string(rlim.rlim_cur) << std::endl;
        }
    }

    /// This must be done before any usage of DateLUT. In particular, before any logging.
    if (config().has("timezone"))
    {
        const std::string config_timezone = config().getString("timezone");
        if (0 != setenv("TZ", config_timezone.data(), 1))
            throw Poco::Exception("Cannot setenv TZ variable");

        tzset();
        DateLUT::setDefaultTimezone(config_timezone);
    }

    std::string log_path = config().getString("logger.log", "");
    if (!log_path.empty())
        log_path = fs::path(log_path).replace_filename("");

    /** Redirect stdout, stderr to separate files in the log directory (or in the specified file).
      * Some libraries write to stderr in case of errors in debug mode,
      *  and this output makes sense even if the program is run in daemon mode.
      * We have to do it before buildLoggers, for errors on logger initialization will be written to these files.
      * If logger.stderr is specified then stderr will be forcibly redirected to that file.
      */
    if ((!log_path.empty() && is_daemon) || config().has("logger.stderr"))
    {
        std::string stderr_path = config().getString("logger.stderr", log_path + "/stderr.log");

        /// Check that stderr is writable before freopen(),
        /// since freopen() will make stderr invalid on error,
        /// and logging to stderr will be broken,
        /// so the following code (that is used in every program) will not write anything:
        ///
        ///     int main(int argc, char ** argv)
        ///     {
        ///         try
        ///         {
        ///             DB::SomeApp app;
        ///             return app.run(argc, argv);
        ///         }
        ///         catch (...)
        ///         {
        ///             std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        ///             return 1;
        ///         }
        ///     }
        if (access(stderr_path.c_str(), W_OK))
        {
            int fd;
            if ((fd = creat(stderr_path.c_str(), 0600)) == -1 && errno != EEXIST)
                throw Poco::OpenFileException("File " + stderr_path + " (logger.stderr) is not writable");
            if (fd != -1)
                ::close(fd);
        }

        if (!freopen(stderr_path.c_str(), "a+", stderr))
            throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);

        /// Disable buffering for stderr
        setbuf(stderr, nullptr);
    }

    if ((!log_path.empty() && is_daemon) || config().has("logger.stdout"))
    {
        std::string stdout_path = config().getString("logger.stdout", log_path + "/stdout.log");
        if (!freopen(stdout_path.c_str(), "a+", stdout))
            throw Poco::OpenFileException("Cannot attach stdout to " + stdout_path);
    }

    /// Change path for logging.
    if (!log_path.empty())
    {
        std::string path = createDirectory(log_path);
        if (is_daemon && chdir(path.c_str()) != 0)
            throw Poco::Exception("Cannot change directory to " + path);
    }
    else
    {
        if (is_daemon && chdir("/tmp") != 0)
            throw Poco::Exception("Cannot change directory to /tmp");
    }

    /// sensitive data masking rules are not used here
    buildLoggers(config(), logger(), self.commandName());

    /// After initialized loggers but before initialized signal handling.
    if (should_setup_watchdog)
        setupWatchdog();

    /// Create pid file.
    if (config().has("pid"))
        pid_file.emplace(config().getString("pid"), DB::StatusFile::write_pid);

    if (is_daemon)
    {
        /** Change working directory to the directory to write core dumps.
          * We have to do it after buildLoggers, because there is the case when config files was in current directory.
          */

        std::string core_path = config().getString("core_path", "");
        if (core_path.empty())
            core_path = getDefaultCorePath();

        tryCreateDirectories(&logger(), core_path);

        if (!(fs::exists(core_path) && fs::is_directory(core_path)))
        {
            core_path = !log_path.empty() ? log_path : "/opt/";
            tryCreateDirectories(&logger(), core_path);
        }

        if (0 != chdir(core_path.c_str()))
            throw Poco::Exception("Cannot change directory to " + core_path);
    }

    initializeTerminationAndSignalProcessing();
    logRevision();
    debugIncreaseOOMScore();

    for (const auto & key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
    {
        graphite_writers.emplace(key, std::make_unique<GraphiteWriter>(key));
    }
}


static void addSignalHandler(const std::vector<int> & signals, signal_function handler, std::vector<int> * out_handled_signals)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO;

#if defined(OS_DARWIN)
    sigemptyset(&sa.sa_mask);
    for (auto signal : signals)
        sigaddset(&sa.sa_mask, signal);
#else
    if (sigemptyset(&sa.sa_mask))
        throw Poco::Exception("Cannot set signal handler.");

    for (auto signal : signals)
        if (sigaddset(&sa.sa_mask, signal))
            throw Poco::Exception("Cannot set signal handler.");
#endif

    for (auto signal : signals)
        if (sigaction(signal, &sa, nullptr))
            throw Poco::Exception("Cannot set signal handler.");

    if (out_handled_signals)
        std::copy(signals.begin(), signals.end(), std::back_inserter(*out_handled_signals));
}


static void blockSignals(const std::vector<int> & signals)
{
    sigset_t sig_set;

#if defined(OS_DARWIN)
    sigemptyset(&sig_set);
    for (auto signal : signals)
        sigaddset(&sig_set, signal);
#else
    if (sigemptyset(&sig_set))
        throw Poco::Exception("Cannot block signal.");

    for (auto signal : signals)
        if (sigaddset(&sig_set, signal))
            throw Poco::Exception("Cannot block signal.");
#endif

    if (pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        throw Poco::Exception("Cannot block signal.");
}


void BaseDaemon::initializeTerminationAndSignalProcessing()
{
    SentryWriter::initialize(config());
    std::set_terminate(terminate_handler);

    /// We want to avoid SIGPIPE when working with sockets and pipes, and just handle return value/errno instead.
    blockSignals({SIGPIPE});

    /// Setup signal handlers.
    /// SIGTSTP is added for debugging purposes. To output a stack trace of any running thread at anytime.
    addSignalHandler({SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE, SIGTSTP, SIGTRAP}, signalHandler, &handled_signals);
    addSignalHandler({SIGHUP}, closeLogsSignalHandler, &handled_signals);
    addSignalHandler({SIGINT, SIGQUIT, SIGTERM}, terminateRequestedSignalHandler, &handled_signals);

#if defined(SANITIZER)
    __sanitizer_set_death_callback(sanitizerDeathCallback);
#endif

    /// Set up Poco ErrorHandler for Poco Threads.
    static KillingErrorHandler killing_error_handler;
    Poco::ErrorHandler::set(&killing_error_handler);

    signal_pipe.setNonBlockingWrite();
    signal_pipe.tryIncreaseSize(1 << 20);

    signal_listener = std::make_unique<SignalListener>(*this);
    signal_listener_thread.start(*signal_listener);

#if defined(__ELF__) && !defined(OS_FREEBSD)
    String build_id_hex = DB::SymbolIndex::instance()->getBuildIDHex();
    if (build_id_hex.empty())
        build_id_info = "no build id";
    else
        build_id_info = "build id: " + build_id_hex;
#else
    build_id_info = "no build id";
#endif

#if defined(OS_LINUX)
    std::string executable_path = getExecutablePath();

    if (!executable_path.empty())
        stored_binary_hash = DB::Elf(executable_path).getStoredBinaryHash();
#endif
}

void BaseDaemon::logRevision() const
{
    Poco::Logger::root().information(
        "Starting " + std::string{VERSION_FULL} + " with revision " + std::to_string(ClickHouseRevision::getVersionRevision()) + ", "
        + build_id_info + ", PID " + std::to_string(getpid()));
}

void BaseDaemon::defineOptions(Poco::Util::OptionSet & new_options)
{
    new_options.addOption(Poco::Util::Option("config-file", "C", "load configuration from a given file")
                              .required(false)
                              .repeatable(false)
                              .argument("<file>")
                              .binding("config-file"));

    new_options.addOption(Poco::Util::Option("log-file", "L", "use given log file")
                              .required(false)
                              .repeatable(false)
                              .argument("<file>")
                              .binding("logger.log"));

    new_options.addOption(Poco::Util::Option("errorlog-file", "E", "use given log file for errors only")
                              .required(false)
                              .repeatable(false)
                              .argument("<file>")
                              .binding("logger.errorlog"));

    new_options.addOption(
        Poco::Util::Option("pid-file", "P", "use given pidfile").required(false).repeatable(false).argument("<file>").binding("pid"));

    Poco::Util::ServerApplication::defineOptions(new_options);
}

void BaseDaemon::handleSignal(int signal_id)
{
    if (signal_id == SIGINT || signal_id == SIGQUIT || signal_id == SIGTERM)
    {
        std::lock_guard lock(signal_handler_mutex);
        {
            ++terminate_signals_counter;
            sigint_signals_counter += signal_id == SIGINT;
            signal_event.notify_all();
        }

        onInterruptSignals(signal_id);
    }
    else
        throw DB::Exception(std::string("Unsupported signal: ") + strsignal(signal_id), 0);
}

void BaseDaemon::onInterruptSignals(int signal_id)
{
    is_cancelled = true;
    LOG_INFO(&logger(), "Received termination signal ({})", strsignal(signal_id));

    if (sigint_signals_counter >= 2)
    {
        LOG_INFO(&logger(), "Received second signal Interrupt. Immediately terminate.");
        call_default_signal_handler(signal_id);
        /// If the above did not help.
        _exit(128 + signal_id);
    }
}


void BaseDaemon::waitForTerminationRequest()
{
    /// NOTE: as we already process signals via pipe, we don't have to block them with sigprocmask in threads
    std::unique_lock<std::mutex> lock(signal_handler_mutex);
    signal_event.wait(lock, [this]() { return terminate_signals_counter > 0; });
}


void BaseDaemon::shouldSetupWatchdog(char * argv0_)
{
    should_setup_watchdog = true;
    argv0 = argv0_;
}


void BaseDaemon::setupWatchdog()
{
    /// Initialize in advance to avoid double initialization in forked processes.
    DateLUT::instance();

    std::string original_process_name;
    if (argv0)
        original_process_name = argv0;

    while (true)
    {
        static pid_t pid = -1;
        pid = fork();

        if (-1 == pid)
            throw Poco::Exception("Cannot fork");

        if (0 == pid)
        {
            logger().information("Forked a child process to watch");
#if defined(OS_LINUX)
            if (0 != prctl(PR_SET_PDEATHSIG, SIGKILL))
                logger().warning("Cannot do prctl to ask termination with parent.");
#endif
            return;
        }

        /// Change short thread name and process name.
        setThreadName("clckhouse-watch"); /// 15 characters

        if (argv0)
        {
            const char * new_process_name = "clickhouse-watchdog";
            memset(argv0, 0, original_process_name.size());
            memcpy(argv0, new_process_name, std::min(strlen(new_process_name), original_process_name.size()));
        }

        /// If streaming compression of logs is used then we write watchdog logs to cerr
        if (config().getRawString("logger.stream_compress", "false") == "true")
        {
            if (config().has("logger.json"))
            {
                Poco::AutoPtr<OwnJSONPatternFormatter> pf = new OwnJSONPatternFormatter;
                Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel(std::cerr));
                logger().setChannel(log);
            }
            else
            {
                Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter;
                Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel(std::cerr));
                logger().setChannel(log);
            }
        }

        logger().information(fmt::format("Will watch for the process with pid {}", pid));

        /// Forward signals to the child process.
        addSignalHandler(
            {SIGHUP, SIGINT, SIGQUIT, SIGTERM},
            [](int sig, siginfo_t *, void *)
            {
                /// Forward all signals except INT as it can be send by terminal to the process group when user press Ctrl+C,
                /// and we process double delivery of this signal as immediate termination.
                if (sig == SIGINT)
                    return;

                const char * error_message = "Cannot forward signal to the child process.\n";
                if (0 != ::kill(pid, sig))
                {
                    auto res = write(STDERR_FILENO, error_message, strlen(error_message));
                    (void)res;
                }
            },
            nullptr);

        int status = 0;
        do
        {
            if (-1 != waitpid(pid, &status, WUNTRACED | WCONTINUED) || errno == ECHILD)
            {
                if (WIFSTOPPED(status))
                    logger().warning(fmt::format("Child process was stopped by signal {}.", WSTOPSIG(status)));
                else if (WIFCONTINUED(status))
                    logger().warning(fmt::format("Child process was continued."));
                else
                    break;
            }
            else if (errno != EINTR)
                throw Poco::Exception("Cannot waitpid, errno: " + std::string(strerror(errno)));
        } while (true);

        if (errno == ECHILD)
        {
            logger().information("Child process no longer exists.");
            _exit(WEXITSTATUS(status));
        }

        if (WIFEXITED(status))
        {
            logger().information(fmt::format("Child process exited normally with code {}.", WEXITSTATUS(status)));
            _exit(WEXITSTATUS(status));
        }

        if (WIFSIGNALED(status))
        {
            int sig = WTERMSIG(status);

            if (sig == SIGKILL)
            {
                logger().fatal(fmt::format(
                    "Child process was terminated by signal {} (KILL)."
                    " If it is not done by 'forcestop' command or manually,"
                    " the possible cause is OOM Killer (see 'dmesg' and look at the '/var/log/kern.log' for the details).",
                    sig));
            }
            else
            {
                logger().fatal(fmt::format("Child process was terminated by signal {}.", sig));

                if (sig == SIGINT || sig == SIGTERM || sig == SIGQUIT)
                    _exit(128 + sig);
            }
        }
        else
        {
            logger().fatal("Child process was not exited normally by unknown reason.");
        }

        /// Automatic restart is not enabled but you can play with it.
#if 1
        _exit(WEXITSTATUS(status));
#else
        logger().information("Will restart.");
        if (argv0)
            memcpy(argv0, original_process_name.c_str(), original_process_name.size());
#endif
    }
}


String BaseDaemon::getStoredBinaryHash() const
{
    return stored_binary_hash;
}
