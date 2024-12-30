#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <base/defines.h>
#include <base/errnoToString.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Core/Settings.h>
#include <Daemon/BaseDaemon.h>
#include <Daemon/SentryWriter.h>
#include <Common/GWPAsan.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/resource.h>

#if defined(OS_LINUX)
#include <sys/prctl.h>
#endif
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <unistd.h>

#include <Poco/Message.h>
#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/ErrorHandler.h>
#include <Poco/Pipe.h>
#include <Common/ErrorHandlers.h>
#include <Common/SignalHandlers.h>
#include <base/argsToConfig.h>
#include <base/coverage.h>
#include <base/scope_guard.h>

#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/SymbolIndex.h>
#include <Common/getExecutablePath.h>
#include <Common/Elf.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <filesystem>

#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>

#include <Common/config_version.h>

#if defined(OS_DARWIN)
#   pragma clang diagnostic ignored "-Wunused-macros"
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#   define _XOPEN_SOURCE 700  // ucontext is not available without _XOPEN_SOURCE
#endif
#include <ucontext.h>

namespace fs = std::filesystem;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYSTEM_ERROR;
        extern const int LOGICAL_ERROR;
    }
}

using namespace DB;


static bool getenvBool(const char * name)
{
    bool res = false;
    const char * env_var = getenv(name); // NOLINT(concurrency-mt-unsafe)
    if (env_var && 0 == strcmp(env_var, "1"))
        res = true;
    return res;
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
        LOG_WARNING(logger, "{}: when creating {}, {}", __PRETTY_FUNCTION__, path, getCurrentExceptionMessage(true));
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
    ConfigProcessor config_processor(config_path, false, true);
    ConfigProcessor::setConfigPath(fs::path(config_path).parent_path());
    loaded_config = config_processor.loadConfig(/* allow_zk_includes = */ true);

    if (last_configuration != nullptr)
        config().removeConfiguration(last_configuration);
    last_configuration = loaded_config.configuration.duplicate();
    config().add(last_configuration, PRIO_DEFAULT, false);
}


BaseDaemon::BaseDaemon() = default;


BaseDaemon::~BaseDaemon()
{
    try
    {
        writeSignalIDtoSignalPipe(SignalListener::StopThread);
        signal_listener_thread.join();
        HandledSignals::instance().reset();
        SentryWriter::resetInstance();
    }
    catch (...)
    {
        tryLogCurrentException(&logger());
    }

    disableLogging();
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
#if !defined(USE_XRAY)
    /// NOTE: may benefit from close_range() (linux 5.9+)
#if defined(OS_FREEBSD) || defined(OS_DARWIN)
    fs::path proc_path{"/dev/fd"};
#else
    fs::path proc_path{"/proc/self/fd"};
#endif

    const auto & signal_pipe = HandledSignals::instance().signal_pipe;
    if (fs::is_directory(proc_path)) /// Hooray, proc exists
    {
        /// in /proc/self/fd directory filenames are numeric file descriptors.
        /// Iterate directory separately from closing fds to avoid closing iterated directory fd.
        std::vector<int> fds;
        for (const auto & path : fs::directory_iterator(proc_path))
            fds.push_back(parse<int>(path.path().filename()));

        for (const auto & fd : fds)
        {
            if (fd > 2 && fd != signal_pipe.fds_rw[0] && fd != signal_pipe.fds_rw[1])
            {
                int err = ::close(fd);
                /// NOTE: it is OK to ignore error here since at least one fd
                /// is already closed (for proc_path), and there can be some
                /// tricky cases, likely.
                (void)err;
            }
        }
    }
    else
    {
        int max_fd = -1;
#if defined(_SC_OPEN_MAX)
        // fd cannot be > INT_MAX
        max_fd = static_cast<int>(sysconf(_SC_OPEN_MAX));
        if (max_fd == -1)
#endif
            max_fd = 256; /// bad fallback
        for (int fd = 3; fd < max_fd; ++fd)
        {
            if (fd != signal_pipe.fds_rw[0] && fd != signal_pipe.fds_rw[1])
            {
                int err = ::close(fd);
                /// NOTE: it is OK to get EBADF here, since it is simply
                /// iterator over all possible fds, without any checks does
                /// this process has this fd or not.
                (void)err;
            }
        }
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

    ConfigProcessor(config_path).savePreprocessedConfig(loaded_config, "");

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
        if (0 != setenv("TZ", config_timezone.data(), 1)) // NOLINT(concurrency-mt-unsafe) // ok if not called concurrently with other setenv/getenv
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
        ///             SomeApp app;
        ///             return app.run(argc, argv);
        ///         }
        ///         catch (...)
        ///         {
        ///             std::cerr << getCurrentExceptionMessage(true) << "\n";
        ///             return 1;
        ///         }
        ///     }
        if (access(stderr_path.c_str(), W_OK))
        {
            int fd;
            if ((fd = creat(stderr_path.c_str(), 0600)) == -1 && errno != EEXIST)
                throw Poco::OpenFileException("File " + stderr_path + " (logger.stderr) is not writable");
            if (fd != -1)
            {
                [[maybe_unused]] int err = ::close(fd);
                chassert(!err || errno == EINTR);
            }
        }

        if (!freopen(stderr_path.c_str(), "a+", stderr))
            throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);

        /// Disable buffering for stderr
        setbuf(stderr, nullptr); // NOLINT(cert-msc24-c,cert-msc33-c, bugprone-unsafe-functions)
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

    /// sensitive data masking rules are not used here
    buildLoggers(config(), logger(), self.commandName());

    /// After initialized loggers but before initialized signal handling.
    if (should_setup_watchdog)
        setupWatchdog();

    /// Create pid file.
    if (config().has("pid"))
        pid_file.emplace(config().getString("pid"), StatusFile::write_pid);

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

    for (const auto & key : getMultipleKeysFromConfig(config(), "", "graphite"))
    {
        graphite_writers.emplace(key, std::make_unique<GraphiteWriter>(key));
    }
}


extern const char * GIT_HASH;

void BaseDaemon::initializeTerminationAndSignalProcessing()
{
    SentryWriter::initializeInstance(config());
    if (config().getBool("send_crash_reports.send_logical_errors", false))
    {
        /// In release builds send it to sentry (if it is configured)
        if (auto * sentry = SentryWriter::getInstance())
        {
            LOG_DEBUG(&logger(), "Enable sending LOGICAL_ERRORs to sentry");
            Exception::callback = [sentry](const std::string & msg, int code, bool remote, const Exception::FramePointers & trace)
            {
                if (!remote && code == ErrorCodes::LOGICAL_ERROR)
                {
                    SentryWriter::FramePointers frame_pointers;
                    for (size_t i = 0; i < trace.size(); ++i)
                        frame_pointers[i] = trace[i];
                    sentry->onException(code, msg, frame_pointers, /* offset= */ 0, trace.size());
                }
            };
        }
    }

    /// We want to avoid SIGPIPE when working with sockets and pipes, and just handle return value/errno instead.
    blockSignals({SIGPIPE});

    /// Setup signal handlers.
    HandledSignals::instance().setupTerminateHandler();
    HandledSignals::instance().setupCommonDeadlySignalHandlers();
    HandledSignals::instance().setupCommonTerminateRequestSignalHandlers();
    HandledSignals::instance().addSignalHandler({SIGHUP}, closeLogsSignalHandler, true);
    HandledSignals::instance().addSignalHandler({SIGCHLD}, childSignalHandler, true);

    /// Set up Poco ErrorHandler for Poco Threads.
    static KillingErrorHandler killing_error_handler;
    Poco::ErrorHandler::set(&killing_error_handler);

    signal_listener = std::make_unique<SignalListener>(this, getLogger("BaseDaemon"));
    signal_listener_thread.start(*signal_listener);

#if defined(__ELF__) && !defined(OS_FREEBSD)
    build_id = SymbolIndex::instance().getBuildIDHex();
#endif

#if defined(OS_LINUX)
    std::string executable_path = getExecutablePath();

    if (!executable_path.empty())
        stored_binary_hash = Elf(executable_path).getStoredBinaryHash();
#endif
}

void BaseDaemon::logRevision() const
{
    logger().information("Starting " + std::string{VERSION_FULL}
        + " (revision: " + std::to_string(ClickHouseRevision::getVersionRevision())
        + ", git hash: " + std::string(GIT_HASH)
        + ", build id: " + (build_id.empty() ? "<unknown>" : build_id) + ")"
        + ", PID " + std::to_string(getpid()));
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
        std::lock_guard lock(signal_handler_mutex);
        {
            ++terminate_signals_counter;
            signal_event.notify_all();
        }

        onInterruptSignals(signal_id);
    }
    else
        throw Exception::createDeprecated(std::string("Unsupported signal: ") + strsignal(signal_id), 0); // NOLINT(concurrency-mt-unsafe) // it is not thread-safe but ok in this context
}

void BaseDaemon::onInterruptSignals(int signal_id)
{
    is_cancelled = true;
    LOG_INFO(&logger(), "Received termination signal ({})", strsignal(signal_id)); // NOLINT(concurrency-mt-unsafe) // it is not thread-safe but ok in this context

    if (terminate_signals_counter >= 2)
    {
        LOG_INFO(&logger(), "This is the second termination signal. Immediately terminate.");
        call_default_signal_handler(signal_id);
        /// If the above did not help.
        _exit(128 + signal_id);
    }
}


void BaseDaemon::waitForTerminationRequest()
{
    /// NOTE: as we already process signals via pipe, we don't have to block them with sigprocmask in threads
    std::unique_lock<std::mutex> lock(signal_handler_mutex);
    signal_event.wait(lock, [this](){ return terminate_signals_counter > 0; });
}


void BaseDaemon::shouldSetupWatchdog(char * argv0_)
{
    should_setup_watchdog = true;
    argv0 = argv0_;
}


void BaseDaemon::setupWatchdog()
{
    /// Initialize in advance to avoid double initialization in forked processes.
    DateLUT::serverTimezoneInstance();

    std::string original_process_name;
    if (argv0)
        original_process_name = argv0;

    bool restart = getenvBool("CLICKHOUSE_WATCHDOG_RESTART");
    bool forward_signals = !getenvBool("CLICKHOUSE_WATCHDOG_NO_FORWARD");

    while (true)
    {
        /// This pipe is used to synchronize notifications to the service manager from the child process
        /// to be sent after the notifications from the parent process.
        Poco::Pipe notify_sync;

        static pid_t pid = -1;
        pid = fork();

        if (-1 == pid)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot fork");

        if (0 == pid)
        {
            updateCurrentThreadIdAfterFork();
            logger().information("Forked a child process to watch");
#if defined(OS_LINUX)
            if (0 != prctl(PR_SET_PDEATHSIG, SIGKILL))
                logger().warning("Cannot do prctl to ask termination with parent.");
#endif

            {
                notify_sync.close(Poco::Pipe::CLOSE_WRITE);
                /// Read from the pipe will block until the pipe is closed.
                /// This way we synchronize with the parent process.
                char buf[1];
                if (0 != notify_sync.readBytes(buf, sizeof(buf)))
                    throw Poco::Exception("Unexpected result while waiting for watchdog synchronization pipe to close.");
            }

            return;
        }

#if defined(OS_LINUX)
        /// Tell the service manager the actual main process is not this one but the forked process
        /// because it is going to be serving the requests and it is going to send "READY=1" notification
        /// when it is fully started.
        /// NOTE: we do this right after fork() and then notify the child process to "unblock" so that it finishes initialization
        /// and sends "READY=1" after we have sent "MAINPID=..."
        systemdNotify(fmt::format("MAINPID={}\n", pid));
#endif

        /// Close the pipe after notifying the service manager.
        /// The child process is waiting for the pipe to be closed.
        notify_sync.close();

        /// Change short thread name and process name.
        setThreadName("clckhouse-watch");   /// 15 characters

        if (argv0)
        {
            const char * new_process_name = "clickhouse-watchdog";
            memset(argv0, 0, original_process_name.size());
            memcpy(argv0, new_process_name, std::min(strlen(new_process_name), original_process_name.size()));
        }

        /// If streaming compression of logs is used then we write watchdog logs to cerr
        if (config().getRawString("logger.stream_compress", "false") == "true")
        {
            Poco::AutoPtr<OwnPatternFormatter> pf;
            if (config().getString("logger.formatting.type", "") == "json")
                pf = new OwnJSONPatternFormatter(config());
            else
                pf = new OwnPatternFormatter;
            Poco::AutoPtr<OwnFormattingChannel> log = new OwnFormattingChannel(pf, new Poco::ConsoleChannel(std::cerr));
            logger().setChannel(log);
        }

        /// Concurrent writing logs to the same file from two threads is questionable on its own,
        /// but rotating them from two threads is disastrous.
        if (auto * channel = dynamic_cast<OwnSplitChannel *>(logger().getChannel()))
        {
            channel->setChannelProperty("log", Poco::FileChannel::PROP_ROTATION, "never");
            channel->setChannelProperty("log", Poco::FileChannel::PROP_ROTATEONOPEN, "false");
        }

        logger().information(fmt::format("Will watch for the process with pid {}", pid));

        /// Forward signals to the child process.
        if (forward_signals)
        {
            HandledSignals::instance().addSignalHandler(
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
                false);
        }
        else
        {
            for (const auto & sig : {SIGHUP, SIGINT, SIGQUIT, SIGTERM})
            {
                if (SIG_ERR == signal(sig, SIG_IGN))
                {
                    char * signal_description = strsignal(sig); // NOLINT(concurrency-mt-unsafe)
                    throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot ignore {}", signal_description);
                }
            }
        }

        int status = 0;
        do
        {
            // Close log files to prevent keeping descriptors of unlinked rotated files.
            // On next log write files will be reopened.
            closeLogs(logger());

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
                throw Poco::Exception("Cannot waitpid, errno: " + errnoToString());
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

        int exit_code;

        if (WIFSIGNALED(status))
        {
            int sig = WTERMSIG(status);

            exit_code = 128 + sig;

            if (sig == SIGKILL)
            {
                logger().fatal(fmt::format("Child process was terminated by signal {} (KILL)."
                    " If it is not done by 'forcestop' command or manually,"
                    " the possible cause is OOM Killer (see 'dmesg' and look at the '/var/log/kern.log' for the details).", sig));
            }
            else
            {
                logger().fatal(fmt::format("Child process was terminated by signal {}.", sig));

                if (sig == SIGINT || sig == SIGTERM || sig == SIGQUIT)
                    _exit(exit_code);
            }
        }
        else
        {
            // According to POSIX, this should never happen.
            logger().fatal("Child process was not exited normally by unknown reason.");
            exit_code = 42;
        }

        if (restart)
        {
            logger().information("Will restart.");
            if (argv0)
                memcpy(argv0, original_process_name.c_str(), original_process_name.size());
        }
        else
            _exit(exit_code);
    }
}


String BaseDaemon::getStoredBinaryHash() const
{
    return stored_binary_hash;
}

#if defined(OS_LINUX)
void systemdNotify(const std::string_view & command)
{
    const char * path = getenv("NOTIFY_SOCKET");  // NOLINT(concurrency-mt-unsafe)

    if (path == nullptr)
        return; /// not using systemd

    int s = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);

    if (s == -1)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Can't create UNIX socket for systemd notify");

    SCOPE_EXIT({ close(s); });

    const size_t len = strlen(path);

    struct sockaddr_un addr;

    addr.sun_family = AF_UNIX;

    if (len < 2 || len > sizeof(addr.sun_path) - 1)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "NOTIFY_SOCKET env var value \"{}\" is wrong.", path);

    memcpy(addr.sun_path, path, len + 1); /// write last zero as well.

    size_t addrlen = offsetof(struct sockaddr_un, sun_path) + len;

    /// '@' means this is Linux abstract socket, per documentation sun_path[0] must be set to '\0' for it.
    if (path[0] == '@')
        addr.sun_path[0] = 0;
    else if (path[0] == '/')
        addrlen += 1; /// non-abstract-addresses should be zero terminated.
    else
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Wrong UNIX path \"{}\" in NOTIFY_SOCKET env var", path);

    const struct sockaddr *sock_addr = reinterpret_cast <const struct sockaddr *>(&addr);

    size_t sent_bytes_total = 0;
    while (sent_bytes_total < command.size())
    {
        auto sent_bytes = sendto(s, command.data() + sent_bytes_total, command.size() - sent_bytes_total, 0, sock_addr, static_cast<socklen_t>(addrlen));
        if (sent_bytes == -1)
        {
            if (errno == EINTR)
                continue;
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Failed to notify systemd, sendto returned error");
        }
        sent_bytes_total += sent_bytes;
    }
}
#endif
