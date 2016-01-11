//
// ServerApplication.cpp
//
// $Id: //poco/1.4/Util/src/ServerApplication.cpp#6 $
//
// Library: Util
// Package: Application
// Module:  ServerApplication
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/OptionException.h"
#include "Poco/Exception.h"
#if !defined(POCO_VXWORKS)
#include "Poco/Process.h"
#include "Poco/NamedEvent.h"
#endif
#include "Poco/NumberFormatter.h"
#include "Poco/Logger.h"
#include "Poco/String.h"
#if defined(POCO_OS_FAMILY_UNIX) && !defined(POCO_VXWORKS)
#include "Poco/TemporaryFile.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <sys/stat.h>
#include <fstream>
#elif defined(POCO_OS_FAMILY_WINDOWS)
#if !defined(_WIN32_WCE)
#include "Poco/Util/WinService.h"
#include "Poco/Util/WinRegistryKey.h"
#endif
#include "Poco/UnWindows.h"
#include <cstring>
#endif
#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
#include "Poco/UnicodeConverter.h"
#endif


using Poco::NumberFormatter;
using Poco::Exception;
using Poco::SystemException;


namespace Poco {
namespace Util {


#if defined(POCO_OS_FAMILY_WINDOWS)
Poco::NamedEvent      ServerApplication::_terminate(Poco::ProcessImpl::terminationEventName(Poco::Process::id()));
#if !defined(_WIN32_WCE)
Poco::Event           ServerApplication::_terminated;
SERVICE_STATUS        ServerApplication::_serviceStatus; 
SERVICE_STATUS_HANDLE ServerApplication::_serviceStatusHandle = 0; 
#endif
#endif
#if defined(POCO_VXWORKS) || defined(POCO_ANDROID)
Poco::Event ServerApplication::_terminate;
#endif


ServerApplication::ServerApplication()
{
#if defined(POCO_OS_FAMILY_WINDOWS)
#if !defined(_WIN32_WCE)
	_action = SRV_RUN;
	std::memset(&_serviceStatus, 0, sizeof(_serviceStatus));
#endif
#endif
}


ServerApplication::~ServerApplication()
{
}


bool ServerApplication::isInteractive() const
{
	bool runsInBackground = config().getBool("application.runAsDaemon", false) || config().getBool("application.runAsService", false);
	return !runsInBackground;
}


int ServerApplication::run()
{
	return Application::run();
}


void ServerApplication::terminate()
{
#if defined(POCO_OS_FAMILY_WINDOWS)
	_terminate.set();
#elif defined(POCO_VXWORKS) || defined(POCO_ANDROID)
	_terminate.set();
#else
	Poco::Process::requestTermination(Process::id());
#endif
}


#if defined(POCO_OS_FAMILY_WINDOWS)
#if !defined(_WIN32_WCE)


//
// Windows specific code
//
BOOL ServerApplication::ConsoleCtrlHandler(DWORD ctrlType)
{
	switch (ctrlType) 
	{ 
	case CTRL_C_EVENT: 
	case CTRL_CLOSE_EVENT: 
	case CTRL_BREAK_EVENT:
		terminate();
		return _terminated.tryWait(10000) ? TRUE : FALSE;
	default: 
		return FALSE; 
	}
}


void ServerApplication::ServiceControlHandler(DWORD control)
{
	switch (control) 
	{ 
	case SERVICE_CONTROL_STOP:
	case SERVICE_CONTROL_SHUTDOWN:
		terminate();
		_serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
		break;
	case SERVICE_CONTROL_INTERROGATE: 
		break; 
	} 
	SetServiceStatus(_serviceStatusHandle,  &_serviceStatus);
}


#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
void ServerApplication::ServiceMain(DWORD argc, LPWSTR* argv)
#else
void ServerApplication::ServiceMain(DWORD argc, LPTSTR* argv)
#endif
{
	ServerApplication& app = static_cast<ServerApplication&>(Application::instance());

	app.config().setBool("application.runAsService", true);

#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
	_serviceStatusHandle = RegisterServiceCtrlHandlerW(L"", ServiceControlHandler);
#else
	_serviceStatusHandle = RegisterServiceCtrlHandlerA("", ServiceControlHandler);
#endif
	if (!_serviceStatusHandle)
		throw SystemException("cannot register service control handler");

	_serviceStatus.dwServiceType             = SERVICE_WIN32; 
	_serviceStatus.dwCurrentState            = SERVICE_START_PENDING; 
	_serviceStatus.dwControlsAccepted        = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
	_serviceStatus.dwWin32ExitCode           = 0; 
	_serviceStatus.dwServiceSpecificExitCode = 0; 
	_serviceStatus.dwCheckPoint              = 0; 
	_serviceStatus.dwWaitHint                = 0; 
	SetServiceStatus(_serviceStatusHandle, &_serviceStatus);

	try
	{
#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
		std::vector<std::string> args;
		for (DWORD i = 0; i < argc; ++i)
		{
			std::string arg;
			Poco::UnicodeConverter::toUTF8(argv[i], arg);
			args.push_back(arg);
		}
		app.init(args);
#else
		app.init(argc, argv);
#endif
		_serviceStatus.dwCurrentState = SERVICE_RUNNING; 
		SetServiceStatus(_serviceStatusHandle, &_serviceStatus);
		int rc = app.run();
		_serviceStatus.dwWin32ExitCode           = rc ? ERROR_SERVICE_SPECIFIC_ERROR : 0;
		_serviceStatus.dwServiceSpecificExitCode = rc; 
	}
	catch (Exception& exc)
	{
		app.logger().log(exc);
		_serviceStatus.dwWin32ExitCode           = ERROR_SERVICE_SPECIFIC_ERROR;
		_serviceStatus.dwServiceSpecificExitCode = EXIT_CONFIG; 
	}
	catch (...)
	{
		app.logger().error("fatal error - aborting");
		_serviceStatus.dwWin32ExitCode           = ERROR_SERVICE_SPECIFIC_ERROR;
		_serviceStatus.dwServiceSpecificExitCode = EXIT_SOFTWARE; 
	}
	_serviceStatus.dwCurrentState = SERVICE_STOPPED;
	SetServiceStatus(_serviceStatusHandle, &_serviceStatus);
}


void ServerApplication::waitForTerminationRequest()
{
	SetConsoleCtrlHandler(ConsoleCtrlHandler, TRUE);
	_terminate.wait();
	_terminated.set();
}


int ServerApplication::run(int argc, char** argv)
{
	if (!hasConsole() && isService())
	{
		return 0;
	}
	else 
	{
		int rc = EXIT_OK;
		try
		{
			init(argc, argv);
			switch (_action)
			{
			case SRV_REGISTER:
				registerService();
				rc = EXIT_OK;
				break;
			case SRV_UNREGISTER:
				unregisterService();
				rc = EXIT_OK;
				break;
			default:
				rc = run();
			}
		}
		catch (Exception& exc)
		{
			logger().log(exc);
			rc = EXIT_SOFTWARE;
		}
		return rc;
	}
}


int ServerApplication::run(const std::vector<std::string>& args)
{
	if (!hasConsole() && isService())
	{
		return 0;
	}
	else 
	{
		int rc = EXIT_OK;
		try
		{
			init(args);
			switch (_action)
			{
			case SRV_REGISTER:
				registerService();
				rc = EXIT_OK;
				break;
			case SRV_UNREGISTER:
				unregisterService();
				rc = EXIT_OK;
				break;
			default:
				rc = run();
			}
		}
		catch (Exception& exc)
		{
			logger().log(exc);
			rc = EXIT_SOFTWARE;
		}
		return rc;
	}
}


#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
int ServerApplication::run(int argc, wchar_t** argv)
{
	if (!hasConsole() && isService())
	{
		return 0;
	}
	else 
	{
		int rc = EXIT_OK;
		try
		{
			init(argc, argv);
			switch (_action)
			{
			case SRV_REGISTER:
				registerService();
				rc = EXIT_OK;
				break;
			case SRV_UNREGISTER:
				unregisterService();
				rc = EXIT_OK;
				break;
			default:
				rc = run();
			}
		}
		catch (Exception& exc)
		{
			logger().log(exc);
			rc = EXIT_SOFTWARE;
		}
		return rc;
	}
}
#endif


bool ServerApplication::isService()
{
#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
	SERVICE_TABLE_ENTRYW svcDispatchTable[2];
	svcDispatchTable[0].lpServiceName = L"";
	svcDispatchTable[0].lpServiceProc = ServiceMain;
	svcDispatchTable[1].lpServiceName = NULL;
	svcDispatchTable[1].lpServiceProc = NULL; 
	return StartServiceCtrlDispatcherW(svcDispatchTable) != 0; 
#else
	SERVICE_TABLE_ENTRY svcDispatchTable[2];
	svcDispatchTable[0].lpServiceName = "";
	svcDispatchTable[0].lpServiceProc = ServiceMain;
	svcDispatchTable[1].lpServiceName = NULL;
	svcDispatchTable[1].lpServiceProc = NULL; 
	return StartServiceCtrlDispatcherA(svcDispatchTable) != 0; 
#endif
}


bool ServerApplication::hasConsole()
{
	HANDLE hStdOut = GetStdHandle(STD_OUTPUT_HANDLE);
	return hStdOut != INVALID_HANDLE_VALUE && hStdOut != NULL;
}


void ServerApplication::registerService()
{
	std::string name = config().getString("application.baseName");
	std::string path = config().getString("application.path");
	
	WinService service(name);
	if (_displayName.empty())
		service.registerService(path);
	else
		service.registerService(path, _displayName);
	if (_startup == "auto")
		service.setStartup(WinService::SVC_AUTO_START);
	else if (_startup == "manual")
		service.setStartup(WinService::SVC_MANUAL_START);
	if (!_description.empty())
		service.setDescription(_description);
	logger().information("The application has been successfully registered as a service.");
}


void ServerApplication::unregisterService()
{
	std::string name = config().getString("application.baseName");
	
	WinService service(name);
	service.unregisterService();
	logger().information("The service has been successfully unregistered.");
}


void ServerApplication::defineOptions(OptionSet& options)
{
	Application::defineOptions(options);

	options.addOption(
		Option("registerService", "", "Register the application as a service.")
			.required(false)
			.repeatable(false)
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleRegisterService)));

	options.addOption(
		Option("unregisterService", "", "Unregister the application as a service.")
			.required(false)
			.repeatable(false)
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleUnregisterService)));

	options.addOption(
		Option("displayName", "", "Specify a display name for the service (only with /registerService).")
			.required(false)
			.repeatable(false)
			.argument("name")
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleDisplayName)));

	options.addOption(
		Option("description", "", "Specify a description for the service (only with /registerService).")
			.required(false)
			.repeatable(false)
			.argument("text")
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleDescription)));

	options.addOption(
		Option("startup", "", "Specify the startup mode for the service (only with /registerService).")
			.required(false)
			.repeatable(false)
			.argument("automatic|manual")
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleStartup)));
}


void ServerApplication::handleRegisterService(const std::string& name, const std::string& value)
{
	_action = SRV_REGISTER;
}


void ServerApplication::handleUnregisterService(const std::string& name, const std::string& value)
{
	_action = SRV_UNREGISTER;
}


void ServerApplication::handleDisplayName(const std::string& name, const std::string& value)
{
	_displayName = value;
}


void ServerApplication::handleDescription(const std::string& name, const std::string& value)
{
	_description = value;
}


void ServerApplication::handleStartup(const std::string& name, const std::string& value)
{
	if (Poco::icompare(value, 4, std::string("auto")) == 0)
		_startup = "auto";
	else if (Poco::icompare(value, std::string("manual")) == 0)
		_startup = "manual";
	else
		throw InvalidArgumentException("argument to startup option must be 'auto[matic]' or 'manual'");
}


#else // _WIN32_WCE
void ServerApplication::waitForTerminationRequest()
{
	_terminate.wait();
}


int ServerApplication::run(int argc, char** argv)
{
	try
	{
		init(argc, argv);
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


int ServerApplication::run(const std::vector<std::string>& args)
{
	try
	{
		init(args);
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
int ServerApplication::run(int argc, wchar_t** argv)
{
	try
	{
		init(argc, argv);
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}
#endif


#endif // _WIN32_WCE
#elif defined(POCO_VXWORKS)
//
// VxWorks specific code
//
void ServerApplication::waitForTerminationRequest()
{
	_terminate.wait();
}


int ServerApplication::run(int argc, char** argv)
{
	try
	{
		init(argc, argv);
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


int ServerApplication::run(const std::vector<std::string>& args)
{
	try
	{
		init(args);
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


void ServerApplication::defineOptions(OptionSet& options)
{
	Application::defineOptions(options);
}


#elif defined(POCO_OS_FAMILY_UNIX)


//
// Unix specific code
//
void ServerApplication::waitForTerminationRequest()
{
#ifndef POCO_ANDROID
	sigset_t sset;
	sigemptyset(&sset);
	if (!std::getenv("POCO_ENABLE_DEBUGGER"))
	{
		sigaddset(&sset, SIGINT);
	}
	sigaddset(&sset, SIGQUIT);
	sigaddset(&sset, SIGTERM);
	sigprocmask(SIG_BLOCK, &sset, NULL);
	int sig;
	sigwait(&sset, &sig);
#else // POCO_ANDROID
	_terminate.wait();
#endif
}


int ServerApplication::run(int argc, char** argv)
{
	bool runAsDaemon = isDaemon(argc, argv);
	if (runAsDaemon)
	{
		beDaemon();
	}
	try
	{
		init(argc, argv);
		if (runAsDaemon)
		{
			int rc = chdir("/");
			if (rc != 0) return EXIT_OSERR;
		}
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


int ServerApplication::run(const std::vector<std::string>& args)
{
	bool runAsDaemon = false;
	for (std::vector<std::string>::const_iterator it = args.begin(); it != args.end(); ++it)
	{
		if (*it == "--daemon")
		{
			runAsDaemon = true;
			break;
		}
	}		
	if (runAsDaemon)
	{
		beDaemon();
	}
	try
	{
		init(args);
		if (runAsDaemon)
		{
			int rc = chdir("/");
			if (rc != 0) return EXIT_OSERR;
		}
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


bool ServerApplication::isDaemon(int argc, char** argv)
{
	std::string option("--daemon");
	for (int i = 1; i < argc; ++i)
	{
		if (option == argv[i])
			return true;
	}
	return false;
}


void ServerApplication::beDaemon()
{
	pid_t pid;
	if ((pid = fork()) < 0)
		throw SystemException("cannot fork daemon process");
	else if (pid != 0)
		exit(0);
	
	setsid();
	umask(0);
	
	// attach stdin, stdout, stderr to /dev/null
	// instead of just closing them. This avoids
	// issues with third party/legacy code writing
	// stuff to stdout/stderr.
	FILE* fin  = freopen("/dev/null", "r+", stdin);
	if (!fin) throw Poco::OpenFileException("Cannot attach stdin to /dev/null");
	FILE* fout = freopen("/dev/null", "r+", stdout);
	if (!fout) throw Poco::OpenFileException("Cannot attach stdout to /dev/null");
	FILE* ferr = freopen("/dev/null", "r+", stderr);
	if (!ferr) throw Poco::OpenFileException("Cannot attach stderr to /dev/null");
}


void ServerApplication::defineOptions(OptionSet& options)
{
	Application::defineOptions(options);

	options.addOption(
		Option("daemon", "", "Run application as a daemon.")
			.required(false)
			.repeatable(false)
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleDaemon)));

	options.addOption(
		Option("pidfile", "", "Write the process ID of the application to given file.")
			.required(false)
			.repeatable(false)
			.argument("path")
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handlePidFile)));
}


void ServerApplication::handleDaemon(const std::string& name, const std::string& value)
{
	config().setBool("application.runAsDaemon", true);
}


void ServerApplication::handlePidFile(const std::string& name, const std::string& value)
{
	std::ofstream ostr(value.c_str());
	if (ostr.good())
		ostr << Poco::Process::id() << std::endl;
	else
		throw Poco::CreateFileException("Cannot write PID to file", value);
	Poco::TemporaryFile::registerForDeletion(value);
}


#elif defined(POCO_OS_FAMILY_VMS)


//
// VMS specific code
//
namespace
{
	static void handleSignal(int sig)
	{
		ServerApplication::terminate();
	}
}


void ServerApplication::waitForTerminationRequest()
{
	struct sigaction handler;
	handler.sa_handler = handleSignal;
	handler.sa_flags   = 0;
	sigemptyset(&handler.sa_mask);
	sigaction(SIGINT, &handler, NULL);
	sigaction(SIGQUIT, &handler, NULL);                                       

	long ctrlY = LIB$M_CLI_CTRLY;
	unsigned short ioChan;
	$DESCRIPTOR(ttDsc, "TT:");

	lib$disable_ctrl(&ctrlY);
	sys$assign(&ttDsc, &ioChan, 0, 0);
	sys$qiow(0, ioChan, IO$_SETMODE | IO$M_CTRLYAST, 0, 0, 0, terminate, 0, 0, 0, 0, 0);
	sys$qiow(0, ioChan, IO$_SETMODE | IO$M_CTRLCAST, 0, 0, 0, terminate, 0, 0, 0, 0, 0);

	std::string evName("POCOTRM");
	NumberFormatter::appendHex(evName, Poco::Process::id(), 8);
	Poco::NamedEvent ev(evName);
	try
	{
		ev.wait();
    }
	catch (...)
	{
		// CTRL-C will cause an exception to be raised
	}
	sys$dassgn(ioChan);
	lib$enable_ctrl(&ctrlY);
}


int ServerApplication::run(int argc, char** argv)
{
	try
	{
		init(argc, argv);
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


int ServerApplication::run(const std::vector<std::string>& args)
{
	try
	{
		init(args);
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


void ServerApplication::defineOptions(OptionSet& options)
{
	Application::defineOptions(options);
}


#endif


} } // namespace Poco::Util
