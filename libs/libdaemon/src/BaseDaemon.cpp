#include <daemon/BaseDaemon.h>

#include <Common/ConfigProcessor.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <cxxabi.h>
#include <execinfo.h>

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
#include <Poco/ScopedLock.h>
#include <Poco/Exception.h>
#include <Poco/ErrorHandler.h>
#include <Poco/NumberFormatter.h>
#include <Poco/Condition.h>
#include <Poco/SyslogChannel.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/ClickHouseRevision.h>
#include <daemon/OwnPatternFormatter.h>

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

/** Для передачи информации из обработчика сигнала для обработки в другом потоке.
  * Если при получении сигнала надо делать что-нибудь серьёзное (например, вывести сообщение в лог),
  *  то передать нужную информацию через pipe в другой поток и сделать там всю работу
  *  - один из немногих безопасных способов сделать это.
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


/** Устанавливает обработчик сигнала по умолчанию, и отправляет себе сигнал sig.
  * Вызывается изнутри пользовательского обработчика сигнала, чтобы записать core dump.
  */
static void call_default_signal_handler(int sig)
{
    signal(sig, SIG_DFL);
    kill(getpid(), sig);
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

/** Обработчик сигналов HUP / USR1 */
static void closeLogsSignalHandler(int sig, siginfo_t * info, void * context)
{
    writeSignalIDtoSignalPipe(sig);
}

static void terminateRequestedSignalHandler(int sig, siginfo_t * info, void * context)
{
    writeSignalIDtoSignalPipe(sig);
}


thread_local bool already_signal_handled = false;

/** Обработчик некоторых сигналов. Выводит информацию в лог (если получится).
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

    /// Время, за которое читающий из pipe поток должен попытаться успеть вывести в лог stack trace.
    ::sleep(10);

    call_default_signal_handler(sig);
}


static bool already_printed_stack_trace = false;

#if USE_UNWIND
size_t backtraceLibUnwind(void ** out_frames, size_t max_frames, ucontext_t & context)
{
    if (already_printed_stack_trace)
        return 0;

    unw_cursor_t cursor;

    if (unw_init_local2(&cursor, &context, UNW_INIT_SIGNAL_FRAME) < 0)
        return 0;

    size_t i = 0;
    for (; i < max_frames; ++i)
    {
        unw_word_t ip;
        unw_get_reg(&cursor, UNW_REG_IP, &ip);
        out_frames[i] = reinterpret_cast<void*>(ip);
        if (!unw_step(&cursor))
            break;
    }

    return i;
}
#endif

/** Получает информацию через pipe.
  * При получении сигнала HUP / USR1 закрывает лог-файлы.
  * При получении информации из std::terminate, выводит её в лог.
  * При получении других сигналов, выводит информацию в лог.
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

        if (sig == SIGSEGV)
        {
            /// Выводим информацию об адресе и о причине.
            if (nullptr == info.si_addr)
                LOG_ERROR(log, "Address: NULL pointer.");
            else
                LOG_ERROR(log, "Address: " << info.si_addr
                    << (info.si_code == SEGV_ACCERR ? ". Attempted access has violated the permissions assigned to the memory area." : ""));
        }

        if (already_printed_stack_trace)
            return;

        void * caller_address = nullptr;

#if defined(__x86_64__)
        /// Get the address at the time the signal was raised from the RIP (x86-64)
        #if defined(__FreeBSD__)
        caller_address = reinterpret_cast<void *>(context.uc_mcontext.mc_rip);
        #elif defined(__APPLE__)
        caller_address = reinterpret_cast<void *>(context.uc_mcontext->__ss.__rip);
        #else
        caller_address = reinterpret_cast<void *>(context.uc_mcontext.gregs[REG_RIP]);
        #endif
#elif defined(__aarch64__)
        caller_address = reinterpret_cast<void *>(context.uc_mcontext.pc);
#endif

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
                    /// Делаем demangling имён. Имя находится в скобках, до символа '+'.

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


/** Для использования с помощью std::set_terminate.
  * Собирает чуть больше информации, чем __gnu_cxx::__verbose_terminate_handler,
  *  и отправляет её в pipe. Другой поток читает из pipe и выводит её в лог.
  * См. исходники в libstdc++-v3/libsupc++/vterminate.cc
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

    /// Сюда записываем информацию для логгирования.
    std::stringstream log;

    std::type_info * t = abi::__cxa_current_exception_type();
    if (t)
    {
        /// Note that "name" is the mangled name.
        char const * name = t->name();
        {
            int status = -1;
            char * dem = 0;

            dem = abi::__cxa_demangle(name, 0, 0, &status);

            log << "Terminate called after throwing an instance of " << (status == 0 ? dem : name) << std::endl;

            if (status == 0)
                free(dem);
        }

        already_printed_stack_trace = true;

        /// If the exception is derived from std::exception, we can give more information.
        try
        {
            throw;
        }
        catch (DB::Exception & e)
        {
            log << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what() << std::endl;
        }
        catch (Poco::Exception & e)
        {
            log << "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what() << std::endl;
        }
        catch (const std::exception & e)
        {
            log << "what(): " << e.what() << std::endl;
        }
        catch (...)
        {
        }

        log << "Stack trace:\n\n" << StackTrace().toString() << std::endl;
    }
    else
    {
        log << "Terminate called without an active exception" << std::endl;
    }

    static const size_t buf_size = 1024;

    std::string log_message = log.str();
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


static std::string createDirectory(const std::string & _path)
{
    Poco::Path path(_path);
    std::string str;
    for(int j=0;j<path.depth();++j)
    {
        str += "/";
        str += path[j];

        int res = ::mkdir(str.c_str(), 0700);
        if( res && (errno!=EEXIST) )
        {
            throw std::runtime_error(std::string("Can't create dir - ") + str + " - " + strerror(errno));
        }
    }

    return str;
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
    /** Если программа запущена не в режиме демона, и не указан параметр config-file,
      *  то будем использовать параметры из файла config.xml в текущей директории,
      *  но игнорировать заданные в нём параметры логгирования.
      * (Чтобы при запуске с минимумом параметров, лог выводился в консоль.)
      * При этом, параметры логгирования, заданные в командной строке, не игнорируются.
      */
    std::string log_command_line_option = config().getString("logger.log", "");
    config_path = config().getString("config-file", "config.xml");
    loaded_config = ConfigProcessor(false, true).loadConfig(config_path, /* allow_zk_includes = */ true);
    if (last_configuration != nullptr)
        config().removeConfiguration(last_configuration);
    last_configuration = loaded_config.configuration.duplicate();
    config().add(last_configuration, PRIO_DEFAULT, false);
    log_to_console = !config().getBool("application.runAsDaemon", false) && log_command_line_option.empty();
}


/// Для создания и уничтожения unique_ptr, который в заголовочном файле объявлен от incomplete type.
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
    if (::kill(Poco::Process::id(), SIGTERM) != 0)
    {
        throw Poco::SystemException("cannot terminate process");
    }
}

void BaseDaemon::kill()
{
    pid.clear();
    Poco::Process::kill(getpid());
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


/// Строит необходимые логгеры
void BaseDaemon::buildLoggers()
{
    /// Сменим директорию для лога
    if (config().hasProperty("logger.log") && !log_to_console)
    {
        std::string path = createDirectory(config().getString("logger.log"));
        if (config().getBool("application.runAsDaemon", false)
            && chdir(path.c_str()) != 0)
            throw Poco::Exception("Cannot change directory to " + path);
    }
    else
    {
        if (config().getBool("application.runAsDaemon", false)
            && chdir("/tmp") != 0)
            throw Poco::Exception("Cannot change directory to /tmp");
    }

    if (config().hasProperty("logger.errorlog") && !log_to_console)
        createDirectory(config().getString("logger.errorlog"));

    if (config().hasProperty("logger.log") && !log_to_console)
    {
        std::cerr << "Should logs to " << config().getString("logger.log") << std::endl;

        // splitter
        Poco::AutoPtr<SplitterChannel> split = new SplitterChannel;

        // set up two channel chains
        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this);
        pf->setProperty("times", "local");
        Poco::AutoPtr<FormattingChannel> log = new FormattingChannel(pf);
        log_file = new FileChannel;
        log_file->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(config().getString("logger.log")).absolute().toString());
        log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config().getRawString("logger.size", "100M"));
        log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config().getRawString("logger.compress", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config().getRawString("logger.count", "1"));
        log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config().getRawString("logger.flush", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config().getRawString("logger.rotateOnOpen", "false"));
        log->setChannel(log_file);
        split->addChannel(log);
        log_file->open();

        if (config().hasProperty("logger.errorlog"))
        {
            std::cerr << "Should error logs to " << config().getString("logger.errorlog") << std::endl;
            Poco::AutoPtr<Poco::LevelFilterChannel> level = new Poco::LevelFilterChannel;
            level->setLevel(Message::PRIO_NOTICE);
            Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this);
            pf->setProperty("times", "local");
            Poco::AutoPtr<FormattingChannel> errorlog = new FormattingChannel(pf);
            error_log_file = new FileChannel;
            error_log_file->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(config().getString("logger.errorlog")).absolute().toString());
            error_log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config().getRawString("logger.size", "100M"));
            error_log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
            error_log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config().getRawString("logger.compress", "true"));
            error_log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config().getRawString("logger.count", "1"));
            error_log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config().getRawString("logger.flush", "true"));
            error_log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config().getRawString("logger.rotateOnOpen", "false"));
            errorlog->setChannel(error_log_file);
            level->setChannel(errorlog);
            split->addChannel(level);
            errorlog->open();
        }

        if (config().getBool("logger.use_syslog", false) || config().getBool("dynamic_layer_selection", false))
        {
            Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this, OwnPatternFormatter::ADD_LAYER_TAG);
            pf->setProperty("times", "local");
            Poco::AutoPtr<FormattingChannel> log = new FormattingChannel(pf);
            syslog_channel = new Poco::SyslogChannel(commandName(), Poco::SyslogChannel::SYSLOG_CONS | Poco::SyslogChannel::SYSLOG_PID, Poco::SyslogChannel::SYSLOG_DAEMON);
            log->setChannel(syslog_channel);
            split->addChannel(log);
            syslog_channel->open();
        }

        split->open();
        logger().close();
        logger().setChannel(split);
    }
    else
    {
        // Выводим на консоль
        Poco::AutoPtr<ConsoleChannel> file = new ConsoleChannel;
        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this);
        pf->setProperty("times", "local");
        Poco::AutoPtr<FormattingChannel> log = new FormattingChannel(pf);
        log->setChannel(file);

        logger().close();
        logger().setChannel(log);
        logger().warning("Logging to console");
    }

    // Уровни для всех
    logger().setLevel(config().getString("logger.level", "trace"));

    // Прикрутим к корневому логгеру
    Logger::root().setLevel(logger().getLevel());
    Logger::root().setChannel(logger().getChannel());

    // Уровни для явно указанных логгеров
    AbstractConfiguration::Keys levels;
    config().keys("logger.levels", levels);

    if(!levels.empty())
        for(AbstractConfiguration::Keys::iterator it = levels.begin(); it != levels.end(); ++it)
            Logger::get(*it).setLevel(config().getString("logger.levels." + *it, "trace"));
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

void BaseDaemon::initialize(Application & self)
{
    task_manager.reset(new Poco::TaskManager);
    ServerApplication::initialize(self);

    bool is_daemon = config().getBool("application.runAsDaemon", false);

    if (is_daemon)
    {
        /** При создании pid файла и поиске конфигурации, будем интерпретировать относительные пути
          * от директории запуска программы.
          */
        std::string path = Poco::Path(config().getString("application.path")).setFileName("").toString();
        if (0 != chdir(path.c_str()))
            throw Poco::Exception("Cannot change directory to " + path);
    }

    /// Считаем конфигурацию
    reloadConfiguration();

    /// В случае падения - сохраняем коры
    {
        struct rlimit rlim;
        if (getrlimit(RLIMIT_CORE, &rlim))
            throw Poco::Exception("Cannot getrlimit");
        /// 1 GiB. Если больше - они слишком долго пишутся на диск.
        rlim.rlim_cur = config().getUInt64("core_dump.size_limit", 1024 * 1024 * 1024);

        if (setrlimit(RLIMIT_CORE, &rlim))
        {
            std::string message = "Cannot set max size of core file to " + std::to_string(rlim.rlim_cur);
        #if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
            throw Poco::Exception(message);
        #else
            /// Не работает под address/thread sanitizer. http://lists.llvm.org/pipermail/llvm-bugs/2013-April/027880.html
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

    std::string log_path = config().getString("logger.log", "");
    if (!log_path.empty())
        log_path = Poco::Path(log_path).setFileName("").toString();

    if (is_daemon)
    {
        /** Переназначим stdout, stderr в отдельные файлы в директориях с логами.
          * Некоторые библиотеки пишут в stderr в случае ошибок или в отладочном режиме,
          *  и этот вывод иногда имеет смысл смотреть даже когда программа запущена в режиме демона.
          * Делаем это до buildLoggers, чтобы ошибки во время инициализации логгера, попали в эти файлы.
          */
        if (!log_path.empty())
        {
            std::string stdout_path = log_path + "/stdout";
            if (!freopen(stdout_path.c_str(), "a+", stdout))
                throw Poco::OpenFileException("Cannot attach stdout to " + stdout_path);

            std::string stderr_path = log_path + "/stderr";
            if (!freopen(stderr_path.c_str(), "a+", stderr))
                throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);
        }

        /// Создадим pid-file.
        if (is_daemon && config().has("pid"))
            pid.seed(config().getString("pid"));
    }

    buildLoggers();

    if (is_daemon)
    {
        /** Сменим директорию на ту, куда надо писать core файлы.
          * Делаем это после buildLoggers, чтобы не менять текущую директорию раньше.
          * Это важно, если конфиги расположены в текущей директории.
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

    /// Ставим terminate_handler
    std::set_terminate(terminate_handler);

    /// We want to avoid SIGPIPE when working with sockets and pipes, and just handle return value/errno instead.
    {
        sigset_t sig_set;
        if (sigemptyset(&sig_set) || sigaddset(&sig_set, SIGPIPE) || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
            throw Poco::Exception("Cannot block signal.");
    }

    /// Ставим обработчики сигналов
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
                    if (sigaction(signal, &sa, 0))
                        throw Poco::Exception("Cannot set signal handler.");
            }
        };

    add_signal_handler({SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE}, faultSignalHandler);
    add_signal_handler({SIGHUP, SIGUSR1}, closeLogsSignalHandler);
    add_signal_handler({SIGINT, SIGQUIT, SIGTERM}, terminateRequestedSignalHandler);

    /// Ставим ErrorHandler для потоков
    static KillingErrorHandler killing_error_handler;
    Poco::ErrorHandler::set(&killing_error_handler);

    /// Выведем ревизию демона
    logRevision();

    signal_listener.reset(new SignalListener(*this));
    signal_listener_thread.start(*signal_listener);

    for (const auto & key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
    {
        graphite_writers.emplace(key, std::make_unique<GraphiteWriter>(key));
    }
}

void BaseDaemon::logRevision() const
{
    Logger::root().information("Starting daemon with revision " + Poco::NumberFormatter::format(ClickHouseRevision::get()));
}

/// Заставляет демон завершаться, если хотя бы одна задача завершилась неудачно
void BaseDaemon::exitOnTaskError()
{
    Observer<BaseDaemon, Poco::TaskFailedNotification> obs(*this, &BaseDaemon::handleNotification);
    getTaskManager().addObserver(obs);
}

/// Используется при exitOnTaskError()
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
        Poco::Util::Option ("config-file", "C", "load configuration from a given file")
            .required (false)
            .repeatable (false)
            .argument ("<file>")
            .binding("config-file")
            );

    _options.addOption(
        Poco::Util::Option ("log-file", "L", "use given log file")
            .required (false)
            .repeatable (false)
            .argument ("<file>")
            .binding("logger.log")
            );

    _options.addOption(
        Poco::Util::Option ("errorlog-file", "E", "use given log file for errors only")
            .required (false)
            .repeatable (false)
            .argument ("<file>")
            .binding("logger.errorlog")
            );

    _options.addOption(
        Poco::Util::Option ("pid-file", "P", "use given pidfile")
            .required (false)
            .repeatable (false)
            .argument ("<file>")
            .binding("pid")
            );
}


void BaseDaemon::PID::seed(const std::string & file_)
{
    /// переведём путь в абсолютный
    file = Poco::Path(file_).absolute().toString();

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

