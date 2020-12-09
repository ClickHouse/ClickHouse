#include <daemon/BaseDaemon.h>
#include <daemon/SentryWriter.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <cxxabi.h>
#include <unistd.h>

#include <typeinfo>
#include <sys/resource.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <memory>
#include <ext/scope_guard.h>

#include <Poco/Observer.h>
#include <Poco/AutoPtr.h>
#include <Poco/PatternFormatter.h>
#include <Poco/TaskManager.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Message.h>
#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/ErrorHandler.h>
#include <Poco/Condition.h>
#include <Poco/SyslogChannel.h>
#include <Poco/DirectoryIterator.h>

#include <common/logger_useful.h>
#include <common/ErrorHandlers.h>
#include <common/argsToConfig.h>
#include <common/getThreadId.h>
#include <common/coverage.h>
#include <common/sleep.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/PipeFDs.h>
#include <Common/StackTrace.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/MemorySanitizer.h>
#include <Common/SymbolIndex.h>

#if !defined(ARCADIA_BUILD)
#   include <Common/config_version.h>
#endif

#if defined(OS_DARWIN)
#   pragma GCC diagnostic ignored "-Wunused-macros"
#   define _XOPEN_SOURCE 700  // ucontext is not available without _XOPEN_SOURCE
#endif
#include <ucontext.h>


DB::PipeFDs signal_pipe;


/** Reset signal handler to the default and send signal to itself.
  * It's called from user signal handler to write core dump.
  */
static void call_default_signal_handler(int sig)
{
    signal(sig, SIG_DFL);
    raise(sig);
}

const char * msan_strsignal(int sig)
{
    // Apparently strsignal is not instrumented by MemorySanitizer, so we
    // have to unpoison it to avoid msan reports inside fmt library when we
    // print it.
    const char * signal_name = strsignal(sig);
    __msan_unpoison_string(signal_name);
    return signal_name;
}

static constexpr size_t max_query_id_size = 127;

static const size_t signal_pipe_buf_size =
    sizeof(int)
    + sizeof(siginfo_t)
    + sizeof(ucontext_t)
    + sizeof(StackTrace)
    + sizeof(UInt32)
    + max_query_id_size + 1    /// query_id + varint encoded length
    + sizeof(void*);


using signal_function = void(int, siginfo_t*, void*);

static void writeSignalIDtoSignalPipe(int sig)
{
    auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);
    DB::writeBinary(sig, out);
    out.next();

    errno = saved_errno;
}

/** Signal handler for HUP / USR1 */
static void closeLogsSignalHandler(int sig, siginfo_t *, void *)
{
    writeSignalIDtoSignalPipe(sig);
}

static void terminateRequestedSignalHandler(int sig, siginfo_t *, void *)
{
    writeSignalIDtoSignalPipe(sig);
}


/** Handler for "fault" or diagnostic signals. Send data about fault to separate thread to write into log.
  */
static void signalHandler(int sig, siginfo_t * info, void * context)
{
    auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const ucontext_t signal_context = *reinterpret_cast<ucontext_t *>(context);
    const StackTrace stack_trace(signal_context);

    StringRef query_id = DB::CurrentThread::getQueryId();   /// This is signal safe.
    query_id.size = std::min(query_id.size, max_query_id_size);

    DB::writeBinary(sig, out);
    DB::writePODBinary(*info, out);
    DB::writePODBinary(signal_context, out);
    DB::writePODBinary(stack_trace, out);
    DB::writeBinary(UInt32(getThreadId()), out);
    DB::writeStringBinary(query_id, out);
    DB::writePODBinary(DB::current_thread, out);

    out.next();

    if (sig != SIGTSTP) /// This signal is used for debugging.
    {
        /// The time that is usually enough for separate thread to print info into log.
        sleepForSeconds(10);
        call_default_signal_handler(sig);
    }

    errno = saved_errno;
}


/// Avoid link time dependency on DB/Interpreters - will use this function only when linked.
__attribute__((__weak__)) void collectCrashLog(
    Int32 signal, UInt64 thread_id, const String & query_id, const StackTrace & stack_trace);


/** The thread that read info about signal or std::terminate from pipe.
  * On HUP / USR1, close log files (for new files to be opened later).
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

    explicit SignalListener(BaseDaemon & daemon_)
        : log(&Poco::Logger::get("BaseDaemon"))
        , daemon(daemon_)
    {
    }

    void run() override
    {
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
            else if (sig == SIGHUP || sig == SIGUSR1)
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
            else if (sig == SIGINT ||
                sig == SIGQUIT ||
                sig == SIGTERM)
            {
                daemon.handleSignal(sig);
            }
            else
            {
                siginfo_t info;
                ucontext_t context;
                StackTrace stack_trace(NoCapture{});
                UInt32 thread_num;
                std::string query_id;
                DB::ThreadStatus * thread_ptr{};

                if (sig != SanitizerTrap)
                {
                    DB::readPODBinary(info, in);
                    DB::readPODBinary(context, in);
                }

                DB::readPODBinary(stack_trace, in);
                DB::readBinary(thread_num, in);
                DB::readBinary(query_id, in);
                DB::readPODBinary(thread_ptr, in);

                /// This allows to receive more signals if failure happens inside onFault function.
                /// Example: segfault while symbolizing stack trace.
                std::thread([=, this] { onFault(sig, info, context, stack_trace, thread_num, query_id, thread_ptr); }).detach();
            }
        }
    }

private:
    Poco::Logger * log;
    BaseDaemon & daemon;

    void onTerminate(const std::string & message, UInt32 thread_num) const
    {
        LOG_FATAL(log, "(version {}{}, {}) (from thread {}) {}",
            VERSION_STRING, VERSION_OFFICIAL, daemon.build_id_info, thread_num, message);
    }

    void onFault(
        int sig,
        const siginfo_t & info,
        const ucontext_t & context,
        const StackTrace & stack_trace,
        UInt32 thread_num,
        const std::string & query_id,
        DB::ThreadStatus * thread_ptr) const
    {
        DB::ThreadStatus thread_status;

        /// Send logs from this thread to client if possible.
        /// It will allow client to see failure messages directly.
        if (thread_ptr)
        {
            if (auto logs_queue = thread_ptr->getInternalTextLogsQueue())
                DB::CurrentThread::attachInternalTextLogsQueue(logs_queue, DB::LogsLevel::trace);
        }

        LOG_FATAL(log, "########################################");

        if (query_id.empty())
        {
            LOG_FATAL(log, "(version {}{}, {}) (from thread {}) (no query) Received signal {} ({})",
                VERSION_STRING, VERSION_OFFICIAL, daemon.build_id_info,
                thread_num, msan_strsignal(sig), sig);
        }
        else
        {
            LOG_FATAL(log, "(version {}{}, {}) (from thread {}) (query_id: {}) Received signal {} ({})",
                VERSION_STRING, VERSION_OFFICIAL, daemon.build_id_info,
                thread_num, query_id, msan_strsignal(sig), sig);
        }

        String error_message;

        if (sig != SanitizerTrap)
            error_message = signalToErrorMessage(sig, info, context);
        else
            error_message = "Sanitizer trap.";

        LOG_FATAL(log, error_message);

        if (stack_trace.getSize())
        {
            /// Write bare stack trace (addresses) just in case if we will fail to print symbolized stack trace.
            /// NOTE This still require memory allocations and mutex lock inside logger. BTW we can also print it to stderr using write syscalls.

            std::stringstream bare_stacktrace;
            bare_stacktrace << "Stack trace:";
            for (size_t i = stack_trace.getOffset(); i < stack_trace.getSize(); ++i)
                bare_stacktrace << ' ' << stack_trace.getFramePointers()[i];

            LOG_FATAL(log, bare_stacktrace.str());
        }

        /// Write symbolized stack trace line by line for better grep-ability.
        stack_trace.toStringEveryLine([&](const std::string & s) { LOG_FATAL(log, s); });

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
    /// Also need to send data via pipe. Otherwise it may lead to deadlocks or failures in printing diagnostic info.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const StackTrace stack_trace;

    StringRef query_id = DB::CurrentThread::getQueryId();
    query_id.size = std::min(query_id.size, max_query_id_size);

    int sig = SignalListener::SanitizerTrap;
    DB::writeBinary(sig, out);
    DB::writePODBinary(stack_trace, out);
    DB::writeBinary(UInt32(getThreadId()), out);
    DB::writeStringBinary(query_id, out);
    DB::writePODBinary(DB::current_thread, out);

    out.next();

    /// The time that is usually enough for separate thread to print info into log.
    sleepForSeconds(10);
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

    static const size_t buf_size = 1024;

    if (log_message.size() > buf_size - 16)
        log_message.resize(buf_size - 16);

    char buf[buf_size];
    DB::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], buf_size, buf);

    DB::writeBinary(static_cast<int>(SignalListener::StdTerminate), out);
    DB::writeBinary(UInt32(getThreadId()), out);
    DB::writeBinary(log_message, out);
    out.next();

    abort();
}


static std::string createDirectory(const std::string & file)
{
    auto path = Poco::Path(file).makeParent();
    if (path.toString().empty())
        return "";
    Poco::File(path).createDirectories();
    return path.toString();
};


static bool tryCreateDirectories(Poco::Logger * logger, const std::string & path)
{
    try
    {
        Poco::File(path).createDirectories();
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
    config_path = config().getString("config-file", "config.xml");
    DB::ConfigProcessor config_processor(config_path, false, true);
    config_processor.setConfigPath(Poco::Path(config_path).makeParent().toString());
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
    {
        signal(sig, SIG_DFL);
    }
    signal_pipe.close();
}


void BaseDaemon::terminate()
{
    getTaskManager().cancelAll();
    if (::raise(SIGTERM) != 0)
        throw Poco::SystemException("cannot terminate process");
}

void BaseDaemon::kill()
{
    dumpCoverageReportIfPossible();
    pid.reset();
    if (::raise(SIGKILL) != 0)
        throw Poco::SystemException("cannot kill process");
}

void BaseDaemon::sleep(double seconds)
{
    wakeup_event.reset();
    wakeup_event.tryWait(seconds * 1000);
}

void BaseDaemon::wakeup()
{
    wakeup_event.set();
}

std::string BaseDaemon::getDefaultCorePath() const
{
    return "/opt/cores/";
}

void BaseDaemon::closeFDs()
{
#if defined(OS_FREEBSD) || defined(OS_DARWIN)
    Poco::File proc_path{"/dev/fd"};
#else
    Poco::File proc_path{"/proc/self/fd"};
#endif
    if (proc_path.isDirectory()) /// Hooray, proc exists
    {
        std::vector<std::string> fds;
        /// in /proc/self/fd directory filenames are numeric file descriptors
        proc_path.list(fds);
        for (const auto & fd_str : fds)
        {
            int fd = DB::parse<int>(fd_str);
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
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(&Poco::Logger::root(), "Failed to adjust OOM score: '{}'.", e.displayText());
        return;
    }
    LOG_INFO(&Poco::Logger::root(), "Set OOM score adjustment to {}", new_score);
}
#else
void debugIncreaseOOMScore() {}
#endif
}

void BaseDaemon::initialize(Application & self)
{
    closeFDs();

    task_manager = std::make_unique<Poco::TaskManager>();
    ServerApplication::initialize(self);

    /// now highest priority (lowest value) is PRIO_APPLICATION = -100, we want higher!
    argsToConfig(argv(), config(), PRIO_APPLICATION - 100);

    bool is_daemon = config().getBool("application.runAsDaemon", false);
    if (is_daemon)
    {
        /** When creating pid file and looking for config, will search for paths relative to the working path of the program when started.
          */
        std::string path = Poco::Path(config().getString("application.path")).setFileName("").toString();
        if (0 != chdir(path.c_str()))
            throw Poco::Exception("Cannot change directory to " + path);
    }

    reloadConfiguration();

    /// This must be done before creation of any files (including logs).
    mode_t umask_num = 0027;
    if (config().has("umask"))
    {
        std::string umask_str = config().getString("umask");
        std::stringstream stream;
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
        log_path = Poco::Path(log_path).setFileName("").toString();

    /** Redirect stdout, stderr to separate files in the log directory (or in the specified file).
      * Some libraries write to stderr in case of errors in debug mode,
      *  and this output makes sense even if the program is run in daemon mode.
      * We have to do it before buildLoggers, for errors on logger initialization will be written to these files.
      * If logger.stderr is specified then stderr will be forcibly redirected to that file.
      */
    if ((!log_path.empty() && is_daemon) || config().has("logger.stderr"))
    {
        std::string stderr_path = config().getString("logger.stderr", log_path + "/stderr.log");
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

    /// Create pid file.
    if (config().has("pid"))
        pid.emplace(config().getString("pid"), DB::StatusFile::write_pid);

    /// Change path for logging.
    if (!log_path.empty())
    {
        std::string path = createDirectory(log_path);
        if (is_daemon
            && chdir(path.c_str()) != 0)
            throw Poco::Exception("Cannot change directory to " + path);
    }
    else
    {
        if (is_daemon
            && chdir("/tmp") != 0)
            throw Poco::Exception("Cannot change directory to /tmp");
    }

    // sensitive data masking rules are not used here
    buildLoggers(config(), logger(), self.commandName());

    if (is_daemon)
    {
        /** Change working directory to the directory to write core dumps.
          * We have to do it after buildLoggers, because there is the case when config files was in current directory.
          */

        std::string core_path = config().getString("core_path", "");
        if (core_path.empty())
            core_path = getDefaultCorePath();

        tryCreateDirectories(&logger(), core_path);

        Poco::File cores = core_path;
        if (!(cores.exists() && cores.isDirectory()))
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


void BaseDaemon::initializeTerminationAndSignalProcessing()
{
    SentryWriter::initialize(config());
    std::set_terminate(terminate_handler);

    /// We want to avoid SIGPIPE when working with sockets and pipes, and just handle return value/errno instead.
    {
        sigset_t sig_set;
        if (sigemptyset(&sig_set) || sigaddset(&sig_set, SIGPIPE) || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
            throw Poco::Exception("Cannot block signal.");
    }

    /// Setup signal handlers.
    auto add_signal_handler =
        [this](const std::vector<int> & signals, signal_function handler)
        {
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sa.sa_sigaction = handler;
            sa.sa_flags = SA_SIGINFO;

            {
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

                std::copy(signals.begin(), signals.end(), std::back_inserter(handled_signals));
            }
        };

    /// SIGTSTP is added for debugging purposes. To output a stack trace of any running thread at anytime.

    add_signal_handler({SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE, SIGTSTP}, signalHandler);
    add_signal_handler({SIGHUP, SIGUSR1}, closeLogsSignalHandler);
    add_signal_handler({SIGINT, SIGQUIT, SIGTERM}, terminateRequestedSignalHandler);

#if defined(SANITIZER)
    __sanitizer_set_death_callback(sanitizerDeathCallback);
#endif

    /// Set up Poco ErrorHandler for Poco Threads.
    static KillingErrorHandler killing_error_handler;
    Poco::ErrorHandler::set(&killing_error_handler);

    signal_pipe.setNonBlocking();
    signal_pipe.tryIncreaseSize(1 << 20);

    signal_listener = std::make_unique<SignalListener>(*this);
    signal_listener_thread.start(*signal_listener);

#if defined(__ELF__) && !defined(__FreeBSD__)
    String build_id_hex = DB::SymbolIndex::instance()->getBuildIDHex();
    if (build_id_hex.empty())
        build_id_info = "no build id";
    else
        build_id_info = "build id: " + build_id_hex;
#else
    build_id_info = "no build id";
#endif
}

void BaseDaemon::logRevision() const
{
    Poco::Logger::root().information("Starting " + std::string{VERSION_FULL}
        + " with revision " + std::to_string(ClickHouseRevision::getVersionRevision())
        + ", " + build_id_info
        + ", PID " + std::to_string(getpid()));
}

/// Makes server shutdown if at least one Poco::Task have failed.
void BaseDaemon::exitOnTaskError()
{
    Poco::Observer<BaseDaemon, Poco::TaskFailedNotification> obs(*this, &BaseDaemon::handleNotification);
    getTaskManager().addObserver(obs);
}

/// Used for exitOnTaskError()
void BaseDaemon::handleNotification(Poco::TaskFailedNotification *_tfn)
{
    task_failed = true;
    Poco::AutoPtr<Poco::TaskFailedNotification> fn(_tfn);
    Poco::Logger * lg = &(logger());
    LOG_ERROR(lg, "Task '{}' failed. Daemon is shutting down. Reason - {}", fn->task()->name(), fn->reason().displayText());
    ServerApplication::terminate();
}

void BaseDaemon::defineOptions(Poco::Util::OptionSet & new_options)
{
    new_options.addOption(
        Poco::Util::Option("config-file", "C", "load configuration from a given file")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("config-file"));

    new_options.addOption(
        Poco::Util::Option("log-file", "L", "use given log file")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("logger.log"));

    new_options.addOption(
        Poco::Util::Option("errorlog-file", "E", "use given log file for errors only")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("logger.errorlog"));

    new_options.addOption(
        Poco::Util::Option("pid-file", "P", "use given pidfile")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("pid"));

    Poco::Util::ServerApplication::defineOptions(new_options);
}

void BaseDaemon::handleSignal(int signal_id)
{
    if (signal_id == SIGINT ||
        signal_id == SIGQUIT ||
        signal_id == SIGTERM)
    {
        std::unique_lock<std::mutex> lock(signal_handler_mutex);
        {
            ++terminate_signals_counter;
            sigint_signals_counter += signal_id == SIGINT;
            signal_event.notify_all();
        }

        onInterruptSignals(signal_id);
    }
    else
        throw DB::Exception(std::string("Unsupported signal: ") + msan_strsignal(signal_id), 0);
}

void BaseDaemon::onInterruptSignals(int signal_id)
{
    is_cancelled = true;
    LOG_INFO(&logger(), "Received termination signal ({})", msan_strsignal(signal_id));

    if (sigint_signals_counter >= 2)
    {
        LOG_INFO(&logger(), "Received second signal Interrupt. Immediately terminate.");
        kill();
    }
}


void BaseDaemon::waitForTerminationRequest()
{
    std::unique_lock<std::mutex> lock(signal_handler_mutex);
    signal_event.wait(lock, [this](){ return terminate_signals_counter > 0; });
}
