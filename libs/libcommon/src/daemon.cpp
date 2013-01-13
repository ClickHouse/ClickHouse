#include <Yandex/daemon.h>

#include <signal.h>
#include <cxxabi.h>

#include <typeinfo>

#include <Yandex/logger_useful.h>
#include <Yandex/mkdir.h>
#include <Yandex/KillingErrorHandler.h>

#include <sys/time.h>
#include <sys/resource.h>
	      
#include <iostream>
#include <Poco/Observer.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/Ext/PatternFormatterWithOwnThreadNumber.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FileChannel.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Exception.h>
#include <Poco/ErrorHandler.h>
#include <Poco/NumberFormatter.h>

#include <DB/Core/Exception.h>

#include "revision.h"


/** Ждать столько то секунд перед тем, как начать слушать уведомления.
  * - простое исправление для ошибки address already in use, которая возникает при перезапуске.
  * (ошибка возникает, так как сокет не закрывается)
  */
#define SLEEP_BEFORE_LISTEN_TO_NOTIFICATIONS 10


using Poco::Logger;
using Poco::AutoPtr;
using Poco::Observer;
using Poco::PatternFormatterWithOwnThreadNumber;
using Poco::FormattingChannel;
using Poco::SplitterChannel;
using Poco::ConsoleChannel;
using Poco::FileChannel;
using Poco::Path;
using Poco::Message;
using Poco::Util::AbstractConfiguration;


/** Слушает определённый порт по UDP
  * При получении любой датаграммы, вызывает wakeup.
  */
class DatagramListener : public Poco::Runnable
{
private:
	UInt16 port;
	Logger * log;

public:
	DatagramListener(UInt16 port_) : port(port_), log(&Logger::get("DatagramListener")) {}

	void run()
	{
		::sleep(SLEEP_BEFORE_LISTEN_TO_NOTIFICATIONS);
		Poco::Net::DatagramSocket socket(Poco::Net::SocketAddress("[::]:" + Poco::NumberFormatter::format(port)), true);

		UInt8 message;
		while (1)
		{
			if (socket.receiveBytes(reinterpret_cast<void *>(&message), sizeof(message)))
			{
				LOG_DEBUG(log, "Received datagram. Waking up.");
				Daemon::instance().wakeup();
			}
		}
	}
};


/** Для использования с помощью std::set_terminate.
  * Выводит чуть больше информации, чем __gnu_cxx::__verbose_terminate_handler,
  *  и выводит её в лог, а не в stderr.
  * См. исходники в libstdc++-v3/libsupc++/vterminate.cc
  */
void terminate_handler()
{
	Logger * log = &Logger::get("Daemon");
	
	static bool terminating = false;
	if (terminating)
	{
		LOG_ERROR(log, "Terminate called recursively");
		abort();
	}

	terminating = true;

	std::type_info * t = abi::__cxa_current_exception_type();
	if (t)
	{
		// Note that "name" is the mangled name.
		char const * name = t->name();
		{
			int status = -1;
			char * dem = 0;

			dem = abi::__cxa_demangle(name, 0, 0, &status);

		    LOG_ERROR(log, "Terminate called after throwing an instance of " << (status == 0 ? dem : name));

			if (status == 0)
				free(dem);
		}

		// If the exception is derived from std::exception, we can
		// give more information.
		try
		{
			throw;
		}
		catch (DB::Exception & e)
		{
			LOG_ERROR(log, "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
			LOG_ERROR(log, "Stack trace:\n\n" << DB::StackTrace().toString());
		}
		catch (Poco::Exception & e)
		{
			LOG_ERROR(log, "Code: " << e.code() << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
			LOG_ERROR(log, "Stack trace:\n\n" << DB::StackTrace().toString());
		}
		catch (const std::exception & e)
		{
			LOG_ERROR(log, "what(): " << e.what());
			LOG_ERROR(log, "Stack trace:\n\n" << DB::StackTrace().toString());
		}
		catch (...)
		{
			LOG_ERROR(log, "Stack trace:\n\n" << DB::StackTrace().toString());
		}
	}
	else
	{
	    LOG_ERROR(log, "Terminate called without an active exception");
	}

    abort();
}


void Daemon::reloadConfiguration()
{
	/** Если программа запущена не в режиме демона, и не указан параметр config-file,
	  *  то будем использовать параметры из файла config.xml в текущей директории,
	  *  но игнорировать заданные в нём параметры логгирования.
	  * (Чтобы при запуске с минимумом параметров, лог выводился в консоль.)
	  * При этом, параметры логгирования, заданные в командной строке, не игнорируются.
	  */
	std::string log_command_line_option = config().getString("logger.log", "");
	loadConfiguration(config().getString("config-file", "config.xml"));
	log_to_console = !config().getBool("application.runAsDaemon", false) && log_command_line_option.empty();

	/// Сменим директорию для лога
	if (config().hasProperty("logger.log") && !log_to_console)
	{
		std::string path = Yandex::mkdir(config().getString("logger.log"));
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

	if (config().hasProperty("logger.errorlog"))
		Yandex::mkdir(config().getString("logger.errorlog"));

	buildLoggers();
}

/// Строит необходимые логгеры
void Daemon::buildLoggers()
{
	std::string format("%Y.%m.%d %H:%M:%S [ %I ] <%p> %s: %t");

	if (config().hasProperty("logger.log") && !log_to_console)
	{
		std::cerr << "Should logs to " << config().getString("logger.log") << std::endl;

		// splitter
		SplitterChannel *split = new SplitterChannel();

		// set up two channel chains
		PatternFormatterWithOwnThreadNumber *pf = new PatternFormatterWithOwnThreadNumber(format);
		pf->setProperty("times", "local");
		FormattingChannel *log = new FormattingChannel(pf);
		FileChannel *file = new FileChannel();
		file->setProperty("path", Poco::Path(config().getString("logger.log")).absolute().toString());
		file->setProperty("rotation", config().getRawString("logger.size", "100M"));
		file->setProperty("archive", "number");
		file->setProperty("compress", config().getRawString("logger.compress", "true"));
		file->setProperty("purgeCount", config().getRawString("logger.count", "1"));
		log->setChannel(file);
		split->addChannel(log);
		file->open();

		if (config().hasProperty("logger.errorlog"))
		{
			std::cerr << "Should error logs to " << config().getString("logger.errorlog") << std::endl;
			Poco::LevelFilterChannel *level = new Poco::LevelFilterChannel();
			level->setLevel(Message::PRIO_NOTICE);
			PatternFormatterWithOwnThreadNumber *pf = new PatternFormatterWithOwnThreadNumber(format);
			pf->setProperty("times", "local");
			FormattingChannel *errorlog = new FormattingChannel(pf);
			FileChannel *errorfile = new FileChannel();
			errorfile->setProperty("path", Poco::Path(config().getString("logger.errorlog")).absolute().toString());
			errorfile->setProperty("rotation", config().getRawString("logger.size", "100M"));
			errorfile->setProperty("archive", "number");
			file->setProperty("compress", config().getRawString("logger.compress", "true"));
			errorfile->setProperty("purgeCount", config().getRawString("logger.count", "1"));
			errorlog->setChannel(errorfile);
			level->setChannel(errorlog);
			split->addChannel(level);
			errorlog->open();
		}

		split->open();
		logger().close();
		logger().setChannel(split);
	}
	else
	{
		// Выводим на консоль
		ConsoleChannel * file = new ConsoleChannel();
		PatternFormatterWithOwnThreadNumber * pf = new PatternFormatterWithOwnThreadNumber(format);
		pf->setProperty("times", "local");
		FormattingChannel * log = new FormattingChannel(pf);
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


void Daemon::initialize(Application& self)
{
	/// В случае падения - сохраняем коры
	{
		struct rlimit rlim;
		if (getrlimit(RLIMIT_CORE, &rlim))
			throw Poco::Exception("Cannot getrlimit");
		rlim.rlim_cur = RLIM_INFINITY;
		if (setrlimit(RLIMIT_CORE, &rlim))
			throw Poco::Exception("Cannot setrlimit");
	}
	
	p_TaskManager = new TaskManager();
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
			m_Pid.seed(config().getString("pid"));
	}

	/// Считаем конфигурацию
	reloadConfiguration();

	/// Ставим terminate_handler
	std::set_terminate(terminate_handler);

	/// Ставим ErrorHandler для потоков
	Poco::ErrorHandler::set(new Yandex::KillingErrorHandler());
	
	/// Выведем ревизию демона
	Logger::root().information("Starting daemon with svn revision " + Poco::NumberFormatter::format(SVN_REVISION));

	/// Порт, при получении датаграммы на котором, будить демон.
	if (config().hasProperty("wakeup_port"))
	{
		listener = new DatagramListener(config().getInt("wakeup_port"));
		listening_thread.start(*listener);
	}

	/// Список подписчиков на изменение состояния - адресов, на которые посылается датаграмма.
	if (config().hasProperty("notify"))
	{
		AbstractConfiguration::Keys address_keys;
		config().keys("notify", address_keys);

		if (!address_keys.empty())
			for (AbstractConfiguration::Keys::iterator it = address_keys.begin(); it != address_keys.end(); ++it)
				subscribers.push_back(Poco::Net::SocketAddress(config().getString("notify." + *it)));
	}

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
	}
}

/// Заставляет демон завершаться, если хотя бы одна задача завершилась неудачно
void Daemon::exitOnTaskError()
{
	Observer<Daemon, TaskFailedNotification> obs(*this, &Daemon::handleNotification);
        getTaskManager().addObserver(obs);
}

/// Используется при exitOnTaskError()
void Daemon::handleNotification(TaskFailedNotification *_tfn)
{
	AutoPtr<TaskFailedNotification> fn(_tfn);
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


void Daemon::notifySubscribers()
{
	UInt8 message = 1;
	for (Subscribers::iterator it = subscribers.begin(); it != subscribers.end(); ++it)
	{
		LOG_TRACE((&Logger::get("Daemon")), "Notifying " << it->toString());
		Poco::Net::DatagramSocket(it->family()).sendTo(&message, sizeof(message), *it);
	}
}
