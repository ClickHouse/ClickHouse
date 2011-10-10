
#include <Yandex/daemon.h>

#include <signal.h>
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

#include "revision.h"


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
		Poco::Net::DatagramSocket socket(Poco::Net::SocketAddress("[::]:" + Poco::NumberFormatter::format(port)));

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
	
	// Сбросим генератор случайных чисел
	srandom(time(NULL));	

	p_TaskManager = new TaskManager();
	ServerApplication::initialize(self);

	// Создадим pid-file, если запущен, как демон
	if (config().getBool("application.runAsDaemon", false))
		m_Pid.seed(config().getString("pid", "pid"));

	// Считаем конфигурацию
	reloadConfiguration();

	// Ставим ErrorHandler для потоков
	Poco::ErrorHandler::set(new Yandex::KillingErrorHandler());
	
	// Выведем ревизию демона
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
}

void Daemon::uninitialize()
{
	ServerApplication::uninitialize();
	delete p_TaskManager;
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
	Poco::Net::DatagramSocket socket;
	UInt8 message = 1;
	for (Subscribers::iterator it = subscribers.begin(); it != subscribers.end(); ++it)
	{
		LOG_TRACE((&Logger::get("Daemon")), "Notifying " << it->toString());
		socket.sendTo(&message, sizeof(message), *it);
	}
}
