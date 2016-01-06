//
// Application.cpp
//
// $Id: //poco/1.4/Util/src/Application.cpp#6 $
//
// Library: Util
// Package: Application
// Module:  Application
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Application.h"
#include "Poco/Util/SystemConfiguration.h"
#include "Poco/Util/MapConfiguration.h"
#include "Poco/Util/PropertyFileConfiguration.h"
#include "Poco/Util/IniFileConfiguration.h"
#ifndef POCO_UTIL_NO_XMLCONFIGURATION
#include "Poco/Util/XMLConfiguration.h"
#endif
#ifndef POCO_UTIL_NO_JSONCONFIGURATION
#include "Poco/Util/JSONConfiguration.h"
#endif
#include "Poco/Util/LoggingSubsystem.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionProcessor.h"
#include "Poco/Util/Validator.h"
#include "Poco/Environment.h"
#include "Poco/Exception.h"
#include "Poco/NumberFormatter.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/String.h"
#include "Poco/ConsoleChannel.h"
#include "Poco/AutoPtr.h"
#if defined(POCO_OS_FAMILY_WINDOWS)
#include "Poco/UnWindows.h"
#endif
#if defined(POCO_OS_FAMILY_UNIX) && !defined(POCO_VXWORKS)
#include "Poco/SignalHandler.h"
#endif
#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
#include "Poco/UnicodeConverter.h"
#endif


using Poco::Logger;
using Poco::Path;
using Poco::File;
using Poco::Environment;
using Poco::SystemException;
using Poco::ConsoleChannel;
using Poco::NumberFormatter;
using Poco::AutoPtr;
using Poco::icompare;


namespace Poco {
namespace Util {


Application* Application::_pInstance = 0;


Application::Application():
	_pConfig(new LayeredConfiguration),
	_initialized(false),
	_unixOptions(true),
	_pLogger(&Logger::get("ApplicationStartup")),
	_stopOptionsProcessing(false)
{
	setup();
}


Application::Application(int argc, char* argv[]):
	_pConfig(new LayeredConfiguration),
	_initialized(false),
	_unixOptions(true),
	_pLogger(&Logger::get("ApplicationStartup")),
	_stopOptionsProcessing(false)
{
	setup();
	init(argc, argv);
}


Application::~Application()
{
	_pInstance = 0;
}


void Application::setup()
{
	poco_assert (_pInstance == 0);
	
	_pConfig->add(new SystemConfiguration, PRIO_SYSTEM, false, false);
	_pConfig->add(new MapConfiguration, PRIO_APPLICATION, true, false);
	
	addSubsystem(new LoggingSubsystem);
	
#if defined(POCO_OS_FAMILY_UNIX) && !defined(POCO_VXWORKS)
	_workingDirAtLaunch = Path::current();

	#if !defined(_DEBUG)
	Poco::SignalHandler::install();
	#endif
#else
	setUnixOptions(false);
#endif

	_pInstance = this;

	AutoPtr<ConsoleChannel> pCC = new ConsoleChannel;
	Logger::setChannel("", pCC);
}


void Application::addSubsystem(Subsystem* pSubsystem)
{
	poco_check_ptr (pSubsystem);

	_subsystems.push_back(pSubsystem);
}


void Application::init(int argc, char* argv[])
{
	setArgs(argc, argv);
	init();
}


#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
void Application::init(int argc, wchar_t* argv[])
{
	std::vector<std::string> args;
	for (int i = 0; i < argc; ++i)
	{
		std::string arg;
		Poco::UnicodeConverter::toUTF8(argv[i], arg);
		args.push_back(arg);
	}
	init(args);
}
#endif


void Application::init(const ArgVec& args)
{
	setArgs(args);
	init();
}


void Application::init()
{
	Path appPath;
	getApplicationPath(appPath);
	_pConfig->setString("application.path", appPath.toString());
	_pConfig->setString("application.name", appPath.getFileName());
	_pConfig->setString("application.baseName", appPath.getBaseName());
	_pConfig->setString("application.dir", appPath.parent().toString());
	_pConfig->setString("application.configDir", appPath.parent().toString());
	processOptions();
}


const char* Application::name() const
{
	return "Application";
}


void Application::initialize(Application& self)
{
	for (SubsystemVec::iterator it = _subsystems.begin(); it != _subsystems.end(); ++it)
	{
		_pLogger->debug(std::string("Initializing subsystem: ") + (*it)->name());
		(*it)->initialize(self);
	}
	_initialized = true;
}

	
void Application::uninitialize()
{
	if (_initialized)
	{
		for (SubsystemVec::reverse_iterator it = _subsystems.rbegin(); it != _subsystems.rend(); ++it)
		{
			_pLogger->debug(std::string("Uninitializing subsystem: ") + (*it)->name());
			(*it)->uninitialize();
		}
		_initialized = false;
	}
}


void Application::reinitialize(Application& self)
{
	for (SubsystemVec::iterator it = _subsystems.begin(); it != _subsystems.end(); ++it)
	{
		_pLogger->debug(std::string("Re-initializing subsystem: ") + (*it)->name());
		(*it)->reinitialize(self);
	}
}


void Application::setUnixOptions(bool flag)
{
	_unixOptions = flag;
}


int Application::loadConfiguration(int priority)
{
	int n = 0;
	Path appPath;
	getApplicationPath(appPath);
	Path confPath;
	if (findAppConfigFile(appPath.getBaseName(), "properties", confPath))
	{
		_pConfig->add(new PropertyFileConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#ifndef POCO_UTIL_NO_INIFILECONFIGURATION
	if (findAppConfigFile(appPath.getBaseName(), "ini", confPath))
	{
		_pConfig->add(new IniFileConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#endif
#ifndef POCO_UTIL_NO_JSONCONFIGURATION
	if (findAppConfigFile(appPath.getBaseName(), "json", confPath))
	{
		_pConfig->add(new JSONConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#endif
#ifndef POCO_UTIL_NO_XMLCONFIGURATION
	if (findAppConfigFile(appPath.getBaseName(), "xml", confPath))
	{
		_pConfig->add(new XMLConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#endif
	if (n > 0)
	{
		if (!confPath.isAbsolute())
			_pConfig->setString("application.configDir", confPath.absolute().parent().toString());
		else
			_pConfig->setString("application.configDir", confPath.parent().toString());
	}
	return n;
}


void Application::loadConfiguration(const std::string& path, int priority)
{
	int n = 0;
	Path confPath(path);
	std::string ext = confPath.getExtension();
	if (icompare(ext, "properties") == 0)
	{
		_pConfig->add(new PropertyFileConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#ifndef POCO_UTIL_NO_INIFILECONFIGURATION
	else if (icompare(ext, "ini") == 0)
	{
		_pConfig->add(new IniFileConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#endif
#ifndef POCO_UTIL_NO_JSONCONFIGURATION
	else if (icompare(ext, "json") == 0)
	{
		_pConfig->add(new JSONConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#endif
#ifndef POCO_UTIL_NO_XMLCONFIGURATION
	else if (icompare(ext, "xml") == 0)
	{
		_pConfig->add(new XMLConfiguration(confPath.toString()), priority, false, false);
		++n;
	}
#endif
	else throw Poco::InvalidArgumentException("Unsupported configuration file type", ext);

	if (n > 0 && !_pConfig->has("application.configDir"))
	{
		if (!confPath.isAbsolute())
			_pConfig->setString("application.configDir", confPath.absolute().parent().toString());
		else
			_pConfig->setString("application.configDir", confPath.parent().toString());
	}
}


std::string Application::commandName() const
{
	return _pConfig->getString("application.baseName");
}


std::string Application::commandPath() const
{
	return _pConfig->getString("application.path");
}


void Application::stopOptionsProcessing()
{
	_stopOptionsProcessing = true;
}


int Application::run()
{
	int rc = EXIT_CONFIG;
	initialize(*this);

	try
	{
		rc = EXIT_SOFTWARE;
		rc = main(_unprocessedArgs);
	}
	catch (Poco::Exception& exc)
	{
		logger().log(exc);
	}
	catch (std::exception& exc)
	{
		logger().error(exc.what());
	}
	catch (...)
	{
		logger().fatal("system exception");
	}

	uninitialize();
	return rc;
}


int Application::main(const ArgVec& args)
{
	return EXIT_OK;
}


void Application::setArgs(int argc, char* argv[])
{
	_command = argv[0];
	_pConfig->setInt("application.argc", argc);
	_unprocessedArgs.reserve(argc);
	std::string argvKey = "application.argv[";
	for (int i = 0; i < argc; ++i)
	{
		std::string arg(argv[i]);
		_pConfig->setString(argvKey + NumberFormatter::format(i) + "]", arg);
		_unprocessedArgs.push_back(arg);
	}
}


void Application::setArgs(const ArgVec& args)
{
	poco_assert (!args.empty());
	
	_command = args[0];
	_pConfig->setInt("application.argc", (int) args.size());
	_unprocessedArgs = args;
	std::string argvKey = "application.argv[";
	for (int i = 0; i < args.size(); ++i)
	{
		_pConfig->setString(argvKey + NumberFormatter::format(i) + "]", args[i]);
	}
}


void Application::processOptions()
{
	defineOptions(_options);
	OptionProcessor processor(_options);
	processor.setUnixStyle(_unixOptions);
	_argv = _unprocessedArgs;
	_unprocessedArgs.erase(_unprocessedArgs.begin());
	ArgVec::iterator it = _unprocessedArgs.begin();
	while (it != _unprocessedArgs.end() && !_stopOptionsProcessing)
	{
		std::string name;
		std::string value;
		if (processor.process(*it, name, value))
		{
			if (!name.empty()) // "--" option to end options processing or deferred argument
			{
				handleOption(name, value);
			}
			it = _unprocessedArgs.erase(it);
		}
		else ++it;
	}
	if (!_stopOptionsProcessing)
		processor.checkRequired();
}


void Application::getApplicationPath(Poco::Path& appPath) const
{
#if defined(POCO_OS_FAMILY_UNIX) && !defined(POCO_VXWORKS)
	if (_command.find('/') != std::string::npos)
	{
		Path path(_command);
		if (path.isAbsolute())
		{
			appPath = path;
		}
		else
		{
			appPath = _workingDirAtLaunch;
			appPath.append(path);
		}
	}
	else
	{
		if (!Path::find(Environment::get("PATH"), _command, appPath))
			appPath = Path(_workingDirAtLaunch, _command);
		appPath.makeAbsolute();
	}
#elif defined(POCO_OS_FAMILY_WINDOWS)
	#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
		wchar_t path[1024];
		int n = GetModuleFileNameW(0, path, sizeof(path)/sizeof(wchar_t));
		if (n > 0)
		{
			std::string p;
			Poco::UnicodeConverter::toUTF8(path, p);
			appPath = p;
		}
		else throw SystemException("Cannot get application file name.");
	#else
		char path[1024];
		int n = GetModuleFileNameA(0, path, sizeof(path));
		if (n > 0)
			appPath = path;
		else
			throw SystemException("Cannot get application file name.");
	#endif
#else
	appPath = _command;
#endif
}


bool Application::findFile(Poco::Path& path) const
{
	if (path.isAbsolute()) return true;
	
	Path appPath;
	getApplicationPath(appPath);
	Path base = appPath.parent();
	do
	{
		Path p(base, path);
		File f(p);
		if (f.exists())
		{
			path = p;
			return true;
		}
		if (base.depth() > 0) base.popDirectory();
	}
	while (base.depth() > 0);
	return false;
}


bool Application::findAppConfigFile(const std::string& appName, const std::string& extension, Path& path) const
{
	poco_assert (!appName.empty());

	Path p(appName);
	p.setExtension(extension);
	bool found = findFile(p);
	if (!found)
	{
#if defined(_DEBUG)
		if (appName[appName.length() - 1] == 'd')
		{
			p.setBaseName(appName.substr(0, appName.length() - 1));
			found = findFile(p);
		}
#endif
	}
	if (found)
		path = p;
	return found;
}


void Application::defineOptions(OptionSet& options)
{
	for (SubsystemVec::iterator it = _subsystems.begin(); it != _subsystems.end(); ++it)
	{
		(*it)->defineOptions(options);
	}
}


void Application::handleOption(const std::string& name, const std::string& value)
{
	const Option& option = _options.getOption(name);
	if (option.validator())
	{
		option.validator()->validate(option, value);
	}
	if (!option.binding().empty())
	{
		AbstractConfiguration* pConfig = option.config();
		if (!pConfig) pConfig = &config();
		pConfig->setString(option.binding(), value);
	}
	if (option.callback())
	{
		option.callback()->invoke(name, value);
	}
}


void Application::setLogger(Logger& logger)
{
	_pLogger = &logger;
}


} } // namespace Poco::Util
