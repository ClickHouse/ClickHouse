#include <common/Daemon.h>

#include <DB/Common/ConfigProcessor.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/time.h>
#include <errno.h>

#include <string.h>
#include <signal.h>
#include <cxxabi.h>
#include <execinfo.h>
#include <ucontext.h>

#include <typeinfo>

#include <common/logger_useful.h>
#include <common/ErrorHandlers.h>

#include <sys/time.h>
#include <sys/resource.h>

#include <iostream>
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

#include <DB/Common/Exception.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <common/Revision.h>



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
		if (0 != pipe(fds))
			DB::throwFromErrno("Cannot create pipe");
	}

	~Pipe()
	{
		close(fds[0]);
		close(fds[1]);
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


typedef decltype(Poco::ThreadNumber::get()) ThreadNumber;
static const size_t buf_size = sizeof(int) + sizeof(siginfo_t) + sizeof(ucontext_t) + sizeof(ThreadNumber);


/** Обработчик сигналов HUP / USR1 */
static void close_logs_signal_handler(int sig, siginfo_t * info, void * context)
{
	char buf[buf_size];
	DB::WriteBufferFromFileDescriptor out(signal_pipe.write_fd, buf_size, buf);
	DB::writeBinary(sig, out);
	out.next();
}


/** Обработчик некоторых сигналов. Выводит информацию в лог (если получится).
  */
static void fault_signal_handler(int sig, siginfo_t * info, void * context)
{
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


/** Получает информацию через pipe.
  * При получении сигнала HUP / USR1 закрывает лог-файлы.
  * При получении информации из std::terminate, выводит её в лог.
  * При получении других сигналов, выводит информацию в лог.
  */
class SignalListener : public Poco::Runnable
{
public:
	SignalListener() : log(&Logger::get("Daemon"))
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

			if (sig == SIGHUP || sig == SIGUSR1)
			{
				LOG_DEBUG(log, "Received signal to close logs.");
				Daemon::instance().closeLogs();
				LOG_INFO(log, "Opened new log file after received signal.");
			}
			else if (sig == -1)		/// -1 для обозначения std::terminate.
			{
				ThreadNumber thread_num;
				std::string message;

				DB::readBinary(thread_num, in);
				DB::readBinary(message, in);

				onTerminate(message, thread_num);
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

		/// Get the address at the time the signal was raised from the RIP (x86-64)
		caller_address = reinterpret_cast<void *>(context.uc_mcontext.gregs[REG_RIP]);

		static const int max_frames = 50;
		void * frames[max_frames];
		int frames_size = backtrace(frames, max_frames);

		if (frames_size >= 2)
		{
			/// Overwrite sigaction with caller's address
			if (caller_address && (frames_size < 3 || caller_address != frames[2]))
				frames[1] = caller_address;

			char ** symbols = backtrace_symbols(frames, frames_size);

			if (!symbols)
			{
				if (caller_address)
					LOG_ERROR(log, "Caller address: " << caller_address);
			}
			else
			{
				for (int i = 1; i < frames_size; ++i)
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
	static __thread bool terminating = false;
	if (terminating)
	{
		abort();
		return;
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

	DB::writeBinary(-1, out);
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


void Daemon::reloadConfiguration()
{
	/** Если программа запущена не в режиме демона, и не указан параметр config-file,
	  *  то будем использовать параметры из файла config.xml в текущей директории,
	  *  но игнорировать заданные в нём параметры логгирования.
	  * (Чтобы при запуске с минимумом параметров, лог выводился в консоль.)
	  * При этом, параметры логгирования, заданные в командной строке, не игнорируются.
	  */
	std::string log_command_line_option = config().getString("logger.log", "");
	ConfigurationPtr processed_config = ConfigProcessor(false, true).loadConfig(config().getString("config-file", "config.xml"));
	config().add(processed_config.duplicate(), PRIO_DEFAULT, false);
	log_to_console = !config().getBool("application.runAsDaemon", false) && log_command_line_option.empty();
}


/** Форматирует по своему.
  * Некоторые детали невозможно получить, используя только Poco::PatternFormatter.
  *
  * Во-первых, используется номер потока не среди потоков Poco::Thread,
  *  а среди всех потоков, для которых был получен номер (см. ThreadNumber.h)
  *
  * Во-вторых, корректно выводится локальная дата и время.
  * Poco::PatternFormatter плохо работает с локальным временем,
  *  в ситуациях, когда в ближайшем будущем намечается отмена или введение daylight saving time.
  *  - см. исходники Poco и http://thread.gmane.org/gmane.comp.time.tz/8883
  *
  * Также сделан чуть более эффективным (что имеет мало значения).
  */
class OwnPatternFormatter : public Poco::PatternFormatter
{
public:
	enum Options
	{
		ADD_NOTHING = 0,
		ADD_LAYER_TAG = 1 << 0
	};
	OwnPatternFormatter(const Daemon & daemon_, Options options_ = ADD_NOTHING) : Poco::PatternFormatter(""), daemon(daemon_), options(options_) {}

	void format(const Message & msg, std::string & text) override
	{
		DB::WriteBufferFromString wb(text);

		/// в syslog тэг идет перед сообщением и до первого пробела.
		if (options & ADD_LAYER_TAG)
		{
			boost::optional<size_t> layer = daemon.getLayer();
			if (layer)
			{
				writeCString("layer[", wb);
				DB::writeIntText(*layer, wb);
				writeCString("]: ", wb);
			}
		}

		/// Выведем время с точностью до миллисекунд.
		timeval tv;
		if (0 != gettimeofday(&tv, nullptr))
			DB::throwFromErrno("Cannot gettimeofday");

		/// Поменяем разделители у даты для совместимости.
		DB::writeDateTimeText<'.', ':'>(tv.tv_sec, wb);

		int milliseconds = tv.tv_usec / 1000;
		DB::writeChar('.', wb);
		DB::writeChar('0' + ((milliseconds / 100) % 10), wb);
		DB::writeChar('0' + ((milliseconds / 10) % 10), wb);
		DB::writeChar('0' + (milliseconds % 10), wb);

		writeCString(" [ ", wb);
		DB::writeIntText(Poco::ThreadNumber::get(), wb);
		writeCString(" ] <", wb);
		DB::writeString(getPriorityName(static_cast<int>(msg.getPriority())), wb);
		writeCString("> ", wb);
		DB::writeString(msg.getSource(), wb);
		writeCString(": ", wb);
		DB::writeString(msg.getText(), wb);
	}

private:
	const Daemon & daemon;
	Options options;
};


/// Для создания и уничтожения unique_ptr, который в заголовочном файле объявлен от incomplete type.
Daemon::Daemon() = default;
Daemon::~Daemon() = default;


void Daemon::terminate()
{
	getTaskManager().cancelAll();
	ServerApplication::terminate();
}

void Daemon::kill()
{
	pid.clear();
	Poco::Process::kill(getpid());
}

void Daemon::sleep(double seconds)
{
	wakeup_event.reset();
	wakeup_event.tryWait(seconds * 1000);
}

void Daemon::wakeup()
{
	wakeup_event.set();
}


/// Строит необходимые логгеры
void Daemon::buildLoggers()
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
		Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(*this);
		pf->setProperty("times", "local");
		Poco::AutoPtr<FormattingChannel> log = new FormattingChannel(pf);
		log_file = new FileChannel;
		log_file->setProperty("path", Poco::Path(config().getString("logger.log")).absolute().toString());
		log_file->setProperty("rotation", config().getRawString("logger.size", "100M"));
		log_file->setProperty("archive", "number");
		log_file->setProperty("compress", config().getRawString("logger.compress", "true"));
		log_file->setProperty("purgeCount", config().getRawString("logger.count", "1"));
		log->setChannel(log_file);
		split->addChannel(log);
		log_file->open();

		if (config().hasProperty("logger.errorlog"))
		{
			std::cerr << "Should error logs to " << config().getString("logger.errorlog") << std::endl;
			Poco::AutoPtr<Poco::LevelFilterChannel> level = new Poco::LevelFilterChannel;
			level->setLevel(Message::PRIO_NOTICE);
			Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(*this);
			pf->setProperty("times", "local");
			Poco::AutoPtr<FormattingChannel> errorlog = new FormattingChannel(pf);
			error_log_file = new FileChannel;
			error_log_file->setProperty("path", Poco::Path(config().getString("logger.errorlog")).absolute().toString());
			error_log_file->setProperty("rotation", config().getRawString("logger.size", "100M"));
			error_log_file->setProperty("archive", "number");
			error_log_file->setProperty("compress", config().getRawString("logger.compress", "true"));
			error_log_file->setProperty("purgeCount", config().getRawString("logger.count", "1"));
			errorlog->setChannel(error_log_file);
			level->setChannel(errorlog);
			split->addChannel(level);
			errorlog->open();
		}

		if (config().getBool("logger.use_syslog", false) || config().getBool("dynamic_layer_selection", false))
		{
			Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(*this, OwnPatternFormatter::ADD_LAYER_TAG);
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
		Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(*this);
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


void Daemon::closeLogs()
{
	if (log_file)
		log_file->close();
	if (error_log_file)
		error_log_file->close();

	if (!log_file)
		logger().warning("Logging to console but received signal to close log file (ignoring).");
}

void Daemon::initialize(Application& self)
{
	/// В случае падения - сохраняем коры
	{
		struct rlimit rlim;
		if (getrlimit(RLIMIT_CORE, &rlim))
			throw Poco::Exception("Cannot getrlimit");
		rlim.rlim_cur = 1024 * 1024 * 1024;		/// 1 GiB. Если больше - они слишком долго пишутся на диск.
		if (setrlimit(RLIMIT_CORE, &rlim))
			throw Poco::Exception("Cannot setrlimit");
	}

	task_manager.reset(new Poco::TaskManager);
	ServerApplication::initialize(self);

	bool is_daemon = config().getBool("application.runAsDaemon", false);

	if (is_daemon)
	{
		/** При создании pid файла и поиске конфигурации, будем интерпретировать относительные пути
		  * от директории запуска программы.
		  */
		std::string path = config().getString("application.path");
		if (0 != chdir(Poco::Path(path).setFileName("").toString().c_str()))
			throw Poco::Exception("Cannot change directory to " + path);

		/// Создадим pid-file.
		if (is_daemon && config().has("pid"))
			pid.seed(config().getString("pid"));
	}

	/// Считаем конфигурацию
	reloadConfiguration();
	buildLoggers();

	/// Ставим terminate_handler
	std::set_terminate(terminate_handler);

	/// Ставим обработчики сигналов
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = fault_signal_handler;
	sa.sa_flags = SA_SIGINFO;

	{
		int signals[] = {SIGABRT, SIGSEGV, SIGILL, SIGBUS, SIGSYS, SIGFPE, SIGPIPE, 0};

		if (sigemptyset(&sa.sa_mask))
			throw Poco::Exception("Cannot set signal handler.");

		for (size_t i = 0; signals[i]; ++i)
			if (sigaddset(&sa.sa_mask, signals[i]))
				throw Poco::Exception("Cannot set signal handler.");

		for (size_t i = 0; signals[i]; ++i)
			if (sigaction(signals[i], &sa, 0))
				throw Poco::Exception("Cannot set signal handler.");
	}

	sa.sa_sigaction = close_logs_signal_handler;

	{
		int signals[] = {SIGHUP, SIGUSR1, 0};

		if (sigemptyset(&sa.sa_mask))
			throw Poco::Exception("Cannot set signal handler.");

		for (size_t i = 0; signals[i]; ++i)
			if (sigaddset(&sa.sa_mask, signals[i]))
				throw Poco::Exception("Cannot set signal handler.");

		for (size_t i = 0; signals[i]; ++i)
			if (sigaction(signals[i], &sa, 0))
				throw Poco::Exception("Cannot set signal handler.");
	}

	/// Ставим ErrorHandler для потоков
	static KillingErrorHandler killing_error_handler;
	Poco::ErrorHandler::set(&killing_error_handler);

	/// Выведем ревизию демона
	Logger::root().information("Starting daemon with revision " + Poco::NumberFormatter::format(Revision::get()));

	close_logs_listener.reset(new SignalListener);
	close_logs_thread.start(*close_logs_listener);

	if (is_daemon)
	{
		/// Сменим директорию на ту, куда надо писать core файлы.
		Poco::File opt_cores = "/opt/cores";
		std::string log_dir = config().getString("logger.log", "");
		size_t pos_of_last_slash = log_dir.rfind("/");
		if (pos_of_last_slash != std::string::npos)
			log_dir.resize(pos_of_last_slash);

		std::string core_path = config().getString("core_path",
			opt_cores.exists() && opt_cores.isDirectory()
				? "/opt/cores/"
				: (!log_dir.empty()
					? log_dir
					: "/opt/"));

		if (0 != chdir(core_path.c_str()))
			throw Poco::Exception("Cannot change directory to " + core_path);

		/** Переназначим stdout, stderr в отдельные файлы в директориях с логами.
		  * Некоторые библиотеки пишут в stderr в случае ошибок или в отладочном режиме,
		  *  и этот вывод иногда имеет смысл смотреть даже когда программа запущена в режиме демона.
		  */
		if (!log_dir.empty())
		{
			std::string stdout_path = log_dir + "/stdout";
			if (!freopen(stdout_path.c_str(), "a+", stdout))
				throw Poco::OpenFileException("Cannot attach stdout to " + stdout_path);

			std::string stderr_path = log_dir + "/stderr";
			if (!freopen(stderr_path.c_str(), "a+", stderr))
				throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);
		}
	}

	graphite_writer.reset(new GraphiteWriter("graphite"));
}


/// Заставляет демон завершаться, если хотя бы одна задача завершилась неудачно
void Daemon::exitOnTaskError()
{
	Observer<Daemon, Poco::TaskFailedNotification> obs(*this, &Daemon::handleNotification);
	getTaskManager().addObserver(obs);
}

/// Используется при exitOnTaskError()
void Daemon::handleNotification(Poco::TaskFailedNotification *_tfn)
{
	AutoPtr<Poco::TaskFailedNotification> fn(_tfn);
	Logger *lg = &(logger());
	LOG_ERROR(lg, "Task '" << fn->task()->name() << "' failed. Daemon is shutting down. Reason - " << fn->reason().displayText());
	ServerApplication::terminate();
}

void Daemon::defineOptions(Poco::Util::OptionSet& _options)
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


void Daemon::PID::seed(const std::string & file_)
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

void Daemon::PID::clear()
{
	if (!file.empty())
	{
		Poco::File(file).remove();
		file.clear();
	}
}
