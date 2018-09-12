#include <daemon/BaseDaemon.h>
#include <daemon/OwnFormattingChannel.h>
#include <daemon/OwnPatternFormatter.h>

#include <Common/Config/ConfigProcessor.h>
#include <daemon/OwnSplitChannel.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <cxxabi.h>
#include <execinfo.h>
#include <unistd.h>

#if USE_UNWIND
    #define UNW_LOCAL_ONLY
    #include <libunwind.h>
#endif

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif
#include <ucontext.h>

#include <typeinfo>
#include <common/logger_useful.h>
#include <common/ErrorHandlers.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <memory>
#include <Poco/Observer.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/TaskManager.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/MapConfiguration.h>
#include <Poco/Util/Application.h>
#include <Poco/Exception.h>
#include <Poco/ErrorHandler.h>
#include <Poco/Condition.h>
#include <Poco/SyslogChannel.h>
#include <Poco/DirectoryIterator.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/ClickHouseRevision.h>
#include <Common/config_version.h>
#include <daemon/OwnPatternFormatter.h>
#include <Common/CurrentThread.h>
#include <Poco/Net/RemoteSyslogChannel.h>


using Poco::Logger;
using Poco::AutoPtr;
using Poco::Observer;
using Poco::FormattingChannel;
using Poco::SplitterChannel;
using Poco::ConsoleChannel;
using Poco::FileChannel;
using Poco::Path;
using Poco::Message;
using Poco::Util::AbstractConfiguration;


constexpr char BaseDaemon::DEFAULT_GRAPHITE_CONFIG_NAME[];

/** For transferring information from signal handler to a separate thread.
  * If you need to do something serious in case of a signal (example: write a message to the log),
  *  then sending information to a separate thread through pipe and doing all the stuff asynchronously
  *  - is probably the only safe method for doing it.
  * (Because it's only safe to use reentrant functions in signal handlers.)
  */
struct Pipe
{
    union
    {
        int fds[2];
        struct
        {
            int read_fd;
            int write_fd;
        };
    };

    Pipe()
    {
        read_fd = -1;
        write_fd = -1;

        if (0 != pipe(fds))
            DB::throwFromErrno("Cannot create pipe");
    }

    void close()
    {
        if (-1 != read_fd)
        {
            ::close(read_fd);
            read_fd = -1;
        }

        if (-1 != write_fd)
        {
            ::close(write_fd);
            write_fd = -1;
        }
    }

    ~Pipe()
    {
        close();
    }
};


Pipe signal_pipe;


/** Reset signal handler to the default and send signal to itself.
  * It's called from user signal handler to write core dump.
  */
static void call_default_signal_handler(int sig)
{
    signal(sig, SIG_DFL);
    raise(sig);
}


using ThreadNumber = decltype(Poco::ThreadNumber::get());
static const size_t buf_size = sizeof(int) + sizeof(siginfo_t) + sizeof(ucontext_t) + sizeof(ThreadNumber);

using signal_function = void(int, siginfo_t*, void*);

static void writeSignalIDtoSignalPipe(int sig)
{
    char buf[buf_size];
    DB::WriteBufferFromFileDescriptor out(signal_pipe.write_fd, buf_size, buf);
    DB::writeBinary(sig, out);
    out.next();
}

/** Signal handler for HUP / USR1 */
static void closeLogsSignalHandler(int sig, siginfo_t * info, void * context)
{
    writeSignalIDtoSignalPipe(sig);
}

static void terminateRequestedSignalHandler(int sig, siginfo_t * info, void * context)
{
    writeSignalIDtoSignalPipe(sig);
}


thread_local bool already_signal_handled = false;

/** Handler for "fault" signals. Send data about fault to separate thread to write into log.
  */
static void faultSignalHandler(int sig, siginfo_t * info, void * context)
{
    if (already_signal_handled)
        return;
    already_signal_handled = true;

    char buf[buf_size];
    DB::WriteBufferFromFileDescriptor out(signal_pipe.write_fd, buf_size, buf);

    DB::writeBinary(sig, out);
    DB::writePODBinary(*info, out);
    DB::writePODBinary(*reinterpret_cast<const ucontext_t *>(context), out);
    DB::writeBinary(Poco::ThreadNumber::get(), out);

    out.next();

    /// The time that is usually enough for separate thread to print info into log.
    ::sleep(10);

    call_default_signal_handler(sig);
}


#if USE_UNWIND
size_t backtraceLibUnwind(void ** out_frames, size_t max_frames, ucontext_t & context)
{
    unw_cursor_t cursor;

    if (unw_init_local2(&cursor, &context, UNW_INIT_SIGNAL_FRAME) < 0)
        return 0;

    size_t i = 0;
    for (; i < max_frames; ++i)
    {
        unw_word_t ip;
        unw_get_reg(&cursor, UNW_REG_IP, &ip);
        out_frames[i] = reinterpret_cast<void*>(ip);

        /// NOTE This triggers "AddressSanitizer: stack-buffer-overflow". Looks like false positive.
        /// It's Ok, because we use this method if the program is crashed nevertheless.
        if (!unw_step(&cursor))
            break;
    }

    return i;
}
#endif


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
        StopThread = -2
    };

    explicit SignalListener(BaseDaemon & daemon_)
        : log(&Logger::get("BaseDaemon"))
        , daemon(daemon_)
    {
    }

    void run()
    {
        char buf[buf_size];
        DB::ReadBufferFromFileDescriptor in(signal_pipe.read_fd, buf_size, buf);

        while (!in.eof())
        {
            int sig = 0;
            DB::readBinary(sig, in);

            if (sig == Signals::StopThread)
            {
                LOG_INFO(log, "Stop SignalListener thread");
                break;
            }
            else if (sig == SIGHUP || sig == SIGUSR1)
            {
                LOG_DEBUG(log, "Received signal to close logs.");
                BaseDaemon::instance().closeLogs();
                LOG_INFO(log, "Opened new log file after received signal.");
            }
            else if (sig == Signals::StdTerminate)
            {
                ThreadNumber thread_num;
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
                ThreadNumber thread_num;

                DB::readPODBinary(info, in);
                DB::readPODBinary(context, in);
                DB::readBinary(thread_num, in);

                onFault(sig, info, context, thread_num);
            }
        }
    }

private:
    Logger * log;
    BaseDaemon & daemon;

private:
    void onTerminate(const std::string & message, ThreadNumber thread_num) const
    {
        LOG_ERROR(log, "(from thread " << thread_num << ") " << message);
    }

    void onFault(int sig, siginfo_t & info, ucontext_t & context, ThreadNumber thread_num) const
    {
        LOG_ERROR(log, "########################################");
        LOG_ERROR(log, "(from thread " << thread_num << ") "
            << "Received signal " << strsignal(sig) << " (" << sig << ")" << ".");

        void * caller_address = nullptr;

#if defined(__x86_64__)
        /// Get the address at the time the signal was raised from the RIP (x86-64)
        #if defined(__FreeBSD__)
        caller_address = reinterpret_cast<void *>(context.uc_mcontext.mc_rip);
        #elif defined(__APPLE__)
        caller_address = reinterpret_cast<void *>(context.uc_mcontext->__ss.__rip);
        #else
        caller_address = reinterpret_cast<void *>(context.uc_mcontext.gregs[REG_RIP]);
        auto err_mask = context.uc_mcontext.gregs[REG_ERR];
        #endif
#elif defined(__aarch64__)
        caller_address = reinterpret_cast<void *>(context.uc_mcontext.pc);
#endif

        switch (sig)
        {
            case SIGSEGV:
            {
                /// Print info about address and reason.
                if (nullptr == info.si_addr)
                    LOG_ERROR(log, "Address: NULL pointer.");
                else
                    LOG_ERROR(log, "Address: " << info.si_addr);

#if defined(__x86_64__) && !defined(__FreeBSD__) && !defined(__APPLE__)
                if ((err_mask & 0x02))
                    LOG_ERROR(log, "Access: write.");
                else
                    LOG_ERROR(log, "Access: read.");
#endif

                switch (info.si_code)
                {
                    case SEGV_ACCERR:
                        LOG_ERROR(log, "Attempted access has violated the permissions assigned to the memory area.");
                        break;
                    case SEGV_MAPERR:
                        LOG_ERROR(log, "Address not mapped to object.");
                        break;
                    default:
                        LOG_ERROR(log, "Unknown si_code.");
                        break;
                }
                break;
            }

            case SIGBUS:
            {
                switch (info.si_code)
                {
                    case BUS_ADRALN:
                        LOG_ERROR(log, "Invalid address alignment.");
                        break;
                    case BUS_ADRERR:
                        LOG_ERROR(log, "Non-existant physical address.");
                        break;
                    case BUS_OBJERR:
                        LOG_ERROR(log, "Object specific hardware error.");
                        break;

                    // Linux specific
#if defined(BUS_MCEERR_AR)
                    case BUS_MCEERR_AR:
                        LOG_ERROR(log, "Hardware memory error: action required.");
                        break;
#endif
#if defined(BUS_MCEERR_AO)
                    case BUS_MCEERR_AO:
                        LOG_ERROR(log, "Hardware memory error: action optional.");
                        break;
#endif

                    default:
                        LOG_ERROR(log, "Unknown si_code.");
                        break;
                }
                break;
            }

            case SIGILL:
            {
                switch (info.si_code)
                {
                    case ILL_ILLOPC:
                        LOG_ERROR(log, "Illegal opcode.");
                        break;
                    case ILL_ILLOPN:
                        LOG_ERROR(log, "Illegal operand.");
                        break;
                    case ILL_ILLADR:
                        LOG_ERROR(log, "Illegal addressing mode.");
                        break;
                    case ILL_ILLTRP:
                        LOG_ERROR(log, "Illegal trap.");
                        break;
                    case ILL_PRVOPC:
                        LOG_ERROR(log, "Privileged opcode.");
                        break;
                    case ILL_PRVREG:
                        LOG_ERROR(log, "Privileged register.");
                        break;
                    case ILL_COPROC:
                        LOG_ERROR(log, "Coprocessor error.");
                        break;
                    case ILL_BADSTK:
                        LOG_ERROR(log, "Internal stack error.");
                        break;
                    default:
                        LOG_ERROR(log, "Unknown si_code.");
                        break;
                }
                break;
            }

            case SIGFPE:
            {
                switch (info.si_code)
                {
                    case FPE_INTDIV:
                        LOG_ERROR(log, "Integer divide by zero.");
                        break;
                    case FPE_INTOVF:
                        LOG_ERROR(log, "Integer overflow.");
                        break;
                    case FPE_FLTDIV:
                        LOG_ERROR(log, "Floating point divide by zero.");
                        break;
                    case FPE_FLTOVF:
                        LOG_ERROR(log, "Floating point overflow.");
                        break;
                    case FPE_FLTUND:
                        LOG_ERROR(log, "Floating point underflow.");
                        break;
                    case FPE_FLTRES:
                        LOG_ERROR(log, "Floating point inexact result.");
                        break;
                    case FPE_FLTINV:
                        LOG_ERROR(log, "Floating point invalid operation.");
                        break;
                    case FPE_FLTSUB:
                        LOG_ERROR(log, "Subscript out of range.");
                        break;
                    default:
                        LOG_ERROR(log, "Unknown si_code.");
                        break;
                }
                break;
            }
        }

        static const int max_frames = 50;
        void * frames[max_frames];

#if USE_UNWIND
        int frames_size = backtraceLibUnwind(frames, max_frames, context);

        if (frames_size)
        {
#else
        /// No libunwind means no backtrace, because we are in a different thread from the one where the signal happened.
        /// So at least print the function where the signal happened.
        if (caller_address)
        {
            frames[0] = caller_address;
            int frames_size = 1;
#endif

            char ** symbols = backtrace_symbols(frames, frames_size);

            if (!symbols)
            {
                if (caller_address)
                    LOG_ERROR(log, "Caller address: " << caller_address);
            }
            else
            {
                for (int i = 0; i < frames_size; ++i)
                {
                    /// Perform demangling of names. Name is in parentheses, before '+' character.

                    char * name_start = nullptr;
                    char * name_end = nullptr;
                    char * demangled_name = nullptr;
                    int status = 0;

                    if (nullptr != (name_start = strchr(symbols[i], '('))
                        && nullptr != (name_end = strchr(name_start, '+')))
                    {
                        ++name_start;
                        *name_end = '\0';
                        demangled_name = abi::__cxa_demangle(name_start, 0, 0, &status);
                        *name_end = '+';
                    }

                    std::stringstream res;

                    res << i << ". ";

                    if (nullptr != demangled_name && 0 == status)
                    {
                        res.write(symbols[i], name_start - symbols[i]);
                        res << demangled_name << name_end;
                    }
                    else
                        res << symbols[i];

                    LOG_ERROR(log, res.rdbuf());
                }
            }
        }
    }
};


/** To use with std::set_terminate.
  * Collects slightly more info than __gnu_cxx::__verbose_terminate_handler,
  *  and send it to pipe. Other thread will read this info from pipe and asynchronously write it to log.
  * Look at libstdc++-v3/libsupc++/vterminate.cc for example.
  */
static void terminate_handler()
{
    static thread_local bool terminating = false;
    if (terminating)
    {
        abort();
        return; /// Just for convenience.
    }

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
    DB::WriteBufferFromFileDescriptor out(signal_pipe.write_fd, buf_size, buf);

    DB::writeBinary(static_cast<int>(SignalListener::StdTerminate), out);
    DB::writeBinary(Poco::ThreadNumber::get(), out);
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
        LOG_WARNING(logger, __PRETTY_FUNCTION__ << ": when creating " << path << ", " << DB::getCurrentExceptionMessage(true));
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
    loaded_config = ConfigProcessor(config_path, false, true).loadConfig(/* allow_zk_includes = */ true);
    if (last_configuration != nullptr)
        config().removeConfiguration(last_configuration);
    last_configuration = loaded_config.configuration.duplicate();
    config().add(last_configuration, PRIO_DEFAULT, false);
}


/// For creating and destroying unique_ptr of incomplete type.
BaseDaemon::BaseDaemon() = default;


BaseDaemon::~BaseDaemon()
{
    writeSignalIDtoSignalPipe(SignalListener::StopThread);
    signal_listener_thread.join();
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
    pid.clear();
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


void BaseDaemon::buildLoggers(Poco::Util::AbstractConfiguration & config)
{
    auto current_logger = config.getString("logger");
    if (config_logger == current_logger)
        return;
    config_logger = current_logger;

    bool is_daemon = config.getBool("application.runAsDaemon", false);

    /// Split logs to ordinary log, error log, syslog and console.
    /// Use extended interface of Channel for more comprehensive logging.
    Poco::AutoPtr<DB::OwnSplitChannel> split = new DB::OwnSplitChannel;

    auto log_level = config.getString("logger.level", "trace");
    const auto log_path = config.getString("logger.log", "");
    if (!log_path.empty())
    {
        createDirectory(log_path);
        std::cerr << "Logging " << log_level << " to " << log_path << std::endl;

        // Set up two channel chains.
        Poco::AutoPtr<FileChannel> log_file = new FileChannel;
        log_file->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(log_path).absolute().toString());
        log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));
        log_file->open();

        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this);

        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, log_file);
        split->addChannel(log);
    }

    const auto errorlog_path = config.getString("logger.errorlog", "");
    if (!errorlog_path.empty())
    {
        createDirectory(errorlog_path);
        std::cerr << "Logging errors to " << errorlog_path << std::endl;

        Poco::AutoPtr<FileChannel> error_log_file = new FileChannel;
        error_log_file->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(errorlog_path).absolute().toString());
        error_log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        error_log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        error_log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
        error_log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        error_log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
        error_log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));

        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this);

        Poco::AutoPtr<DB::OwnFormattingChannel> errorlog = new DB::OwnFormattingChannel(pf, error_log_file);
        errorlog->setLevel(Message::PRIO_NOTICE);
        errorlog->open();
        split->addChannel(errorlog);
    }

    /// "dynamic_layer_selection" is needed only for Yandex.Metrika, that share part of ClickHouse code.
    /// We don't need this configuration parameter.

    if (config.getBool("logger.use_syslog", false) || config.getBool("dynamic_layer_selection", false))
    {
        const std::string & cmd_name = commandName();

        if (config.has("logger.syslog.address"))
        {
            syslog_channel = new Poco::Net::RemoteSyslogChannel();
            // syslog address
            syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_LOGHOST, config.getString("logger.syslog.address"));
            if (config.has("logger.syslog.hostname"))
            {
                syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_HOST, config.getString("logger.syslog.hostname"));
            }
            syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_FORMAT, config.getString("logger.syslog.format", "syslog"));
            syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_USER"));
        }
        else
        {
            syslog_channel = new Poco::SyslogChannel();
            syslog_channel->setProperty(Poco::SyslogChannel::PROP_NAME, cmd_name);
            syslog_channel->setProperty(Poco::SyslogChannel::PROP_OPTIONS, config.getString("logger.syslog.options", "LOG_CONS|LOG_PID"));
            syslog_channel->setProperty(Poco::SyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_DAEMON"));
        }
        syslog_channel->open();

        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this, OwnPatternFormatter::ADD_LAYER_TAG);

        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, syslog_channel);
        split->addChannel(log);
    }

    if (config.getBool("logger.console", false) || (!config.hasProperty("logger.console") && !is_daemon && (isatty(STDIN_FILENO) || isatty(STDERR_FILENO))))
    {
        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(new OwnPatternFormatter(this), new Poco::ConsoleChannel);
        logger().warning("Logging " + log_level + " to console");
        split->addChannel(log);
    }

    split->open();
    logger().close();
    logger().setChannel(split);

    // Global logging level (it can be overridden for specific loggers).
    logger().setLevel(log_level);

    // Set level to all already created loggers
    std::vector <std::string> names;
    Logger::root().names(names);
    for (const auto & name : names)
        Logger::root().get(name).setLevel(log_level);

    // Attach to the root logger.
    Logger::root().setLevel(log_level);
    Logger::root().setChannel(logger().getChannel());

    // Explicitly specified log levels for specific loggers.
    AbstractConfiguration::Keys levels;
    config.keys("logger.levels", levels);

    if(!levels.empty())
        for(AbstractConfiguration::Keys::iterator it = levels.begin(); it != levels.end(); ++it)
            Logger::get(*it).setLevel(config.getString("logger.levels." + *it, "trace"));
}


void BaseDaemon::closeLogs()
{
    if (log_file)
        log_file->close();
    if (error_log_file)
        error_log_file->close();

    if (!log_file)
        logger().warning("Logging to console but received signal to close log file (ignoring).");
}

std::string BaseDaemon::getDefaultCorePath() const
{
    return "/opt/cores/";
}

void BaseDaemon::closeFDs()
{
#if defined(__FreeBSD__) || (defined(__APPLE__) && defined(__MACH__))
    Poco::File proc_path{"/dev/fd"};
#else
    Poco::File proc_path{"/proc/self/fd"};
#endif
    if (proc_path.isDirectory()) /// Hooray, proc exists
    {
        Poco::DirectoryIterator itr(proc_path), end;
        for (; itr != end; ++itr)
        {
            long fd = DB::parse<long>(itr.name());
            if (fd > 2 && fd != signal_pipe.read_fd && fd != signal_pipe.write_fd)
                ::close(fd);
        }
    }
    else
    {
        long max_fd = -1;
#ifdef _SC_OPEN_MAX
        max_fd = sysconf(_SC_OPEN_MAX);
        if (max_fd == -1)
#endif
            max_fd = 256; /// bad fallback
        for (long fd = 3; fd < max_fd; ++fd)
            if (fd != signal_pipe.read_fd && fd != signal_pipe.write_fd)
                ::close(fd);
    }
}

void BaseDaemon::initialize(Application & self)
{
    closeFDs();
    task_manager.reset(new Poco::TaskManager);
    ServerApplication::initialize(self);

    {
        /// Parsing all args and converting to config layer
        /// Test: -- --1=1 --1=2 --3 5 7 8 -9 10 -11=12 14= 15== --16==17 --=18 --19= --20 21 22 --23 --24 25 --26 -27 28 ---29=30 -- ----31 32 --33 3-4
        Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
        std::string key;
        for(auto & arg : argv())
        {
            auto key_start = arg.find_first_not_of('-');
            auto pos_minus = arg.find('-');
            auto pos_eq = arg.find('=');

            // old saved '--key', will set to some true value "1"
            if (!key.empty() && pos_minus != std::string::npos && pos_minus < key_start)
            {
                map_config->setString(key, "1");
                key = "";
            }

            if (pos_eq == std::string::npos)
            {
                if (!key.empty())
                {
                    if (pos_minus == std::string::npos || pos_minus > key_start)
                    {
                        map_config->setString(key, arg);
                    }
                    key = "";
                }
                if (pos_minus != std::string::npos && key_start != std::string::npos && pos_minus < key_start)
                    key = arg.substr(key_start);
                continue;
            }
            else
            {
                key = "";
            }

            if (key_start == std::string::npos)
                continue;

            if (pos_minus > key_start)
                continue;

            key = arg.substr(key_start, pos_eq - key_start);
            if (key.empty())
                continue;
            std::string value;
            if (arg.size() > pos_eq)
                value = arg.substr(pos_eq+1);

            map_config->setString(key, value);
            key = "";
        }
        /// now highest priority (lowest value) is PRIO_APPLICATION = -100, we want higher!
        config().add(map_config, PRIO_APPLICATION - 100);
    }

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
    if (config().has("umask"))
    {
        std::string umask_str = config().getString("umask");
        mode_t umask_num = 0;
        std::stringstream stream;
        stream << umask_str;
        stream >> std::oct >> umask_num;

        umask(umask_num);
    }

    ConfigProcessor(config_path).savePreprocessedConfig(loaded_config);

    /// Write core dump on crash.
    {
        struct rlimit rlim;
        if (getrlimit(RLIMIT_CORE, &rlim))
            throw Poco::Exception("Cannot getrlimit");
        /// 1 GiB by default. If more - it writes to disk too long.
        rlim.rlim_cur = config().getUInt64("core_dump.size_limit", 1024 * 1024 * 1024);

        if (setrlimit(RLIMIT_CORE, &rlim))
        {
            std::string message = "Cannot set max size of core file to " + std::to_string(rlim.rlim_cur);
        #if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(SANITIZER) && !defined(__APPLE__)
            throw Poco::Exception(message);
        #else
            /// It doesn't work under address/thread sanitizer. http://lists.llvm.org/pipermail/llvm-bugs/2013-April/027880.html
            std::cerr << message << std::endl;
        #endif
        }
    }

    /// This must be done before any usage of DateLUT. In particular, before any logging.
    if (config().has("timezone"))
    {
        if (0 != setenv("TZ", config().getString("timezone").data(), 1))
            throw Poco::Exception("Cannot setenv TZ variable");

        tzset();
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
    }

    if ((!log_path.empty() && is_daemon) || config().has("logger.stdout"))
    {
        std::string stdout_path = config().getString("logger.stdout", log_path + "/stdout.log");
        if (!freopen(stdout_path.c_str(), "a+", stdout))
            throw Poco::OpenFileException("Cannot attach stdout to " + stdout_path);
    }

    /// Create pid file.
    if (is_daemon && config().has("pid"))
        pid.seed(config().getString("pid"));

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

    buildLoggers(config());

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

    DB::CurrentThread::get();   /// TODO Why do we need this?
    logRevision();

    for (const auto & key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
    {
        graphite_writers.emplace(key, std::make_unique<GraphiteWriter>(key));
    }
}


void BaseDaemon::initializeTerminationAndSignalProcessing()
{
    std::set_terminate(terminate_handler);

    /// We want to avoid SIGPIPE when working with sockets and pipes, and just handle return value/errno instead.
    {
        sigset_t sig_set;
        if (sigemptyset(&sig_set) || sigaddset(&sig_set, SIGPIPE) || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
            throw Poco::Exception("Cannot block signal.");
    }

    /// Setup signal handlers.
    auto add_signal_handler =
        [](const std::vector<int> & signals, signal_function handler)
        {
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sa.sa_sigaction = handler;
            sa.sa_flags = SA_SIGINFO;

            {
                if (sigemptyset(&sa.sa_mask))
                    throw Poco::Exception("Cannot set signal handler.");

                for (auto signal : signals)
                    if (sigaddset(&sa.sa_mask, signal))
                        throw Poco::Exception("Cannot set signal handler.");

                for (auto signal : signals)
                    if (sigaction(signal, &sa, nullptr))
                        throw Poco::Exception("Cannot set signal handler.");
            }
        };

    add_signal_handler({SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE}, faultSignalHandler);
    add_signal_handler({SIGHUP, SIGUSR1}, closeLogsSignalHandler);
    add_signal_handler({SIGINT, SIGQUIT, SIGTERM}, terminateRequestedSignalHandler);

    /// Set up Poco ErrorHandler for Poco Threads.
    static KillingErrorHandler killing_error_handler;
    Poco::ErrorHandler::set(&killing_error_handler);

    signal_listener.reset(new SignalListener(*this));
    signal_listener_thread.start(*signal_listener);

}

void BaseDaemon::logRevision() const
{
    Logger::root().information("Starting " + std::string{VERSION_FULL} + " with revision " + std::to_string(ClickHouseRevision::get()));
}

/// Makes server shutdown if at least one Poco::Task have failed.
void BaseDaemon::exitOnTaskError()
{
    Observer<BaseDaemon, Poco::TaskFailedNotification> obs(*this, &BaseDaemon::handleNotification);
    getTaskManager().addObserver(obs);
}

/// Used for exitOnTaskError()
void BaseDaemon::handleNotification(Poco::TaskFailedNotification *_tfn)
{
    task_failed = true;
    AutoPtr<Poco::TaskFailedNotification> fn(_tfn);
    Logger *lg = &(logger());
    LOG_ERROR(lg, "Task '" << fn->task()->name() << "' failed. Daemon is shutting down. Reason - " << fn->reason().displayText());
    ServerApplication::terminate();
}

void BaseDaemon::defineOptions(Poco::Util::OptionSet& _options)
{
    Poco::Util::ServerApplication::defineOptions (_options);

    _options.addOption(
        Poco::Util::Option("config-file", "C", "load configuration from a given file")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("config-file"));

    _options.addOption(
        Poco::Util::Option("log-file", "L", "use given log file")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("logger.log"));

    _options.addOption(
        Poco::Util::Option("errorlog-file", "E", "use given log file for errors only")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("logger.errorlog"));

    _options.addOption(
        Poco::Util::Option("pid-file", "P", "use given pidfile")
            .required(false)
            .repeatable(false)
            .argument("<file>")
            .binding("pid"));
}

bool isPidRunning(pid_t pid)
{
    if (getpgid(pid) >= 0)
        return 1;
    return 0;
}

void BaseDaemon::PID::seed(const std::string & file_)
{
    file = Poco::Path(file_).absolute().toString();
    Poco::File poco_file(file);

    if (poco_file.exists())
    {
        pid_t pid_read = 0;
        {
            std::ifstream in(file);
            if (in.good())
            {
                in >> pid_read;
                if (pid_read && isPidRunning(pid_read))
                    throw Poco::Exception("Pid file exists and program running with pid = " + std::to_string(pid_read) + ", should not start daemon.");
            }
        }
        std::cerr << "Old pid file exists (with pid = " << pid_read << "), removing." << std::endl;
        poco_file.remove();
    }

    int fd = open(file.c_str(),
        O_CREAT | O_EXCL | O_WRONLY,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);

    if (-1 == fd)
    {
        file.clear();
        if (EEXIST == errno)
            throw Poco::Exception("Pid file exists, should not start daemon.");
        throw Poco::CreateFileException("Cannot create pid file.");
    }

    try
    {
        std::stringstream s;
        s << getpid();
        if (static_cast<ssize_t>(s.str().size()) != write(fd, s.str().c_str(), s.str().size()))
            throw Poco::Exception("Cannot write to pid file.");
    }
    catch (...)
    {
        close(fd);
        throw;
    }

    close(fd);
}

void BaseDaemon::PID::clear()
{
    if (!file.empty())
    {
        Poco::File(file).remove();
        file.clear();
    }
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
        throw DB::Exception(std::string("Unsupported signal: ") + strsignal(signal_id));
}

void BaseDaemon::onInterruptSignals(int signal_id)
{
    is_cancelled = true;
    LOG_INFO(&logger(), "Received termination signal (" << strsignal(signal_id) << ")");

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

