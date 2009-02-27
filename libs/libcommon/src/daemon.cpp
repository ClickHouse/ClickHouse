
#include <Yandex/daemon.h>

#include <signal.h>
#include <Yandex/Exception.h>
#include <Yandex/logger_useful.h>
#include <Yandex/mkdir.h>

#include <sys/time.h>
#include <sys/resource.h>
	      

#include <iostream>
#include <Poco/Observer.h>
#include <Poco/RWLock.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/PatternFormatter.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FileChannel.h>
#include <Poco/File.h>
#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Ext/MProfile.h>
#include <Poco/Exception.h>


#include "revision.h"

using Poco::Logger;
using Poco::AutoPtr;
using Poco::Observer;
using Poco::PatternFormatter;
using Poco::FormattingChannel;
using Poco::SplitterChannel;
using Poco::ConsoleChannel;
using Poco::FileChannel;
using Poco::Path;
using Poco::Message;
using Poco::Util::AbstractConfiguration;

void Daemon::reloadConfiguration()
{

	try
	{
		if( !config().hasProperty("config-file") ) throw Yandex::BException("config-file and pid-file should be specified");
		//config().remove(Poco::Util::Application::PRIO_DEFAULT);
		loadConfiguration(config().getString("config-file"));

		// Перейдём в каталожек, чтобы нормально писать логи и коры
		if( config().hasProperty("logger.log") && !config().hasProperty("logger.console") )
		{
			std::string path = Yandex::mkdir( config().getString("logger.log") );
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
		
		buildLoggers();
		Logger *log = &Logger::get( "Daemon.reloadConfiguration()" );

		// Виртуальная функция, чтобы пользователь мог обновить кеш конфигурации
		try
		{
			refreshConfigCache();
		}
		catch(const Poco::Exception& ex)
		{
			LOG_ERROR(log, "PoCo error while refresh config: " << ex.displayText());
			if( !is_Running ) throw;
		}
		catch (const std::exception& ex)
		{
			LOG_ERROR(log, "STD error while refresh config: " << ex.what());
			if( !is_Running ) throw;
		}
		catch (...)
		{
			LOG_ERROR(log, "UNKNOWN error while refresh config");
			if( !is_Running ) throw;
		}

	}
	catch(Poco::Exception& ex)
	{
		throw;
	}

	// Если уже на работающем демоне, кто-то меняет конфиг так, что вываливается исключение - это его
	// проблемы - будем всё равно работать	
	is_Running = true;
}

/// Строит необходимые логгеры
void Daemon::buildLoggers()
{
	Poco::ScopedRWLock lock(*this);

	try
	{
		if( config().hasProperty("logger.log") && !config().hasProperty("logger.console") )
		{
			std::cerr << "Should logs to " << config().getString("logger.log") << std::endl;
			
			// splitter
			SplitterChannel *split = new SplitterChannel();
			
			// format
			std::string format("%Y.%m.%d %H:%M:%S [ %I ] <%p> %s: %t");

			// set up two channel chains
			PatternFormatter *pf = new PatternFormatter(format);
			pf->setProperty("times", "local");
			FormattingChannel *log = new FormattingChannel(pf);
			FileChannel *file = new FileChannel();
			file->setProperty("path", config().getString("logger.log"));
			file->setProperty("rotation", config().getRawString("logger.size", "100M"));
			file->setProperty("archive", "number");
			file->setProperty("purgeCount", config().getRawString("logger.count", "1"));
			log->setChannel(file);
			split->addChannel(log);
			file->open();

			if( config().hasProperty("logger.errorlog") )
			{
				std::cerr << "Should error logs to " << config().getString("logger.errorlog") << std::endl;
				Poco::LevelFilterChannel *level = new Poco::LevelFilterChannel();
				level->setLevel(Message::PRIO_NOTICE);
				PatternFormatter *pf = new PatternFormatter(format);
				pf->setProperty("times", "local");
				FormattingChannel *errorlog = new FormattingChannel(pf);
				FileChannel *errorfile = new FileChannel();
				errorfile->setProperty("path", config().getString("logger.errorlog"));
				errorfile->setProperty("rotation", config().getRawString("logger.size", "100M"));
				errorfile->setProperty("archive", "number");
				errorfile->setProperty("purgeCount", config().getRawString("logger.count", "1"));
				errorlog->setChannel(errorfile);
				level->setChannel(errorlog);
				split->addChannel(level);
				errorlog->open();
			}

			split->open();
			logger().close();
			logger().setChannel( split );
		}
		else
		{
			// Выводим на консоль
			ConsoleChannel *file = new ConsoleChannel();
			logger().close();
			logger().setChannel( file );
			logger().warning("Log file isn't specified. Logging to console");
		}
	}
	catch(...)
	{
		// Выводим на консоль
		ConsoleChannel *file = new ConsoleChannel();
		logger().close();
		logger().setChannel( file );
		logger().warning("Can't log to file. Logging to console");
		throw;
	}

	// Уровни для всех
	logger().setLevel( config().getString("logger.level", "information") );
		
	// Прикрутим к корневому логгеру
	Logger::root().setLevel( logger().getLevel() );
	Logger::root().setChannel( logger().getChannel() );
	
	// Уровни для явно указанных логгеров
	AbstractConfiguration::Keys levels;
	config().keys("logger.levels", levels);
	if( !levels.empty() )
	{
		for(AbstractConfiguration::Keys::iterator it=levels.begin();it!=levels.end();++it)
		{
			Logger::get(*it).setLevel( config().getString("logger.levels." + *it, "info") );
		}
	}
}

void Daemon::initialize(Application& self)
{
	/// В случае падения - сохраняем коры
	{
		struct rlimit rlim;
		int res = getrlimit(RLIMIT_CORE, &rlim);
		assert(!res);
		rlim.rlim_cur = RLIM_INFINITY;
		res = setrlimit(RLIMIT_CORE, &rlim);
		assert(!res);
	}
	
	// Сбросим генератор случайных чисел
	srandom(time(NULL));	

	// Используется при загрузке конфигурации
	is_Running = false;

	p_TaskManager = new ("TaskManager") TaskManager();
	ServerApplication::initialize(self);

	// Создадим pid-file
	if( !config().hasProperty("pid") ) throw Yandex::BException("config-file and pid-file should be specified");
	m_Pid.seed( config().getString("pid") );

	// Считаем конфигурацию
	reloadConfiguration();
	
	// Выведем ревизию демона
	Logger::root().information("Starting daemon with svn revision " + Yandex::to_string(SVN_REVISION));
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

	_options.addOption(
		Poco::Util::Option ("console", "", "print logs on console only")
			.required (false)
			.repeatable (false)
			.binding("logger.console")
			);
}
