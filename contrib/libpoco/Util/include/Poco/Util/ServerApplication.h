//
// ServerApplication.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/ServerApplication.h#3 $
//
// Library: Util
// Package: Application
// Module:  ServerApplication
//
// Definition of the ServerApplication class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_ServerApplication_INCLUDED
#define Util_ServerApplication_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/Application.h"
#include "Poco/Event.h"
#if defined(POCO_OS_FAMILY_WINDOWS)
#include "Poco/NamedEvent.h"
#endif


namespace Poco {
namespace Util {


class Util_API ServerApplication: public Application
	/// A subclass of the Application class that is used
	/// for implementing server applications.
	///
	/// A ServerApplication allows for the application
	/// to run as a Windows service or as a Unix daemon
	/// without the need to add extra code.
	///
	/// For a ServerApplication to work both from the command line
	/// and as a daemon or service, a few rules must be met:
	///   - Subsystems must be registered in the constructor.
	///   - All non-trivial initializations must be made in the
	///     initialize() method.
	///   - At the end of the main() method, waitForTerminationRequest()
	///     should be called.
	///   - New threads must only be created in initialize() or main() or
	///     methods called from there, but not in the application class'
	///     constructor or in the constructor of instance variables.
	///     The reason for this is that fork() will be called in order to
	///     create the daemon process, and threads created prior to calling
	///     fork() won't be taken over to the daemon process.
	///   - The main(argc, argv) function must look as follows:
	///
	///   int main(int argc, char** argv)
	///   {
	///       MyServerApplication app;
	///       return app.run(argc, argv);
	///   }
	///
	/// The POCO_SERVER_MAIN macro can be used to implement main(argc, argv).
	/// If POCO has been built with POCO_WIN32_UTF8, POCO_SERVER_MAIN supports
	/// Unicode command line arguments.
	///
	/// On Windows platforms, an application built on top of the
	/// ServerApplication class can be run both from the command line
	/// or as a service.
	///
	/// To run an application as a Windows service, it must be registered
	/// with the Windows Service Control Manager (SCM). To do this, the application
	/// can be started from the command line, with the /registerService option
	/// specified. This causes the application to register itself with the
	/// SCM, and then exit. Similarly, an application registered as a service can
	/// be unregistered, by specifying the /unregisterService option.
	/// The file name of the application executable (excluding the .exe suffix)
	/// is used as the service name. Additionally, a more user-friendly name can be
	/// specified, using the /displayName option (e.g., /displayName="Demo Service")
	/// and a service description can be added with the /description option.
	/// The startup mode (automatic or manual) for the service can be specified
	/// with the /startup option.
	///
	/// An application can determine whether it is running as a service by checking
	/// for the "application.runAsService" configuration property.
	/// 
	///     if (config().getBool("application.runAsService", false))
	///     {
	///         // do service specific things
	///     }
	///
	/// Note that the working directory for an application running as a service
	/// is the Windows system directory (e.g., C:\Windows\system32). Take this
	/// into account when working with relative filesystem paths. Also, services
	/// run under a different user account, so an application that works when
	/// started from the command line may fail to run as a service if it depends
	/// on a certain environment (e.g., the PATH environment variable).
	///
	/// An application registered as a Windows service can be started
	/// with the NET START <name> command and stopped with the NET STOP <name>
	/// command. Alternatively, the Services MMC applet can be used.
	///
	/// On Unix platforms, an application built on top of the ServerApplication
	/// class can be optionally run as a daemon by giving the --daemon
	/// command line option. A daemon, when launched, immediately
	/// forks off a background process that does the actual work. After launching
	/// the background process, the foreground process exits.
	/// 
	/// After the initialization is complete, but before entering the main() method,
	/// the current working directory for the daemon process is changed to the root
	/// directory ("/"), as it is common practice for daemon processes. Therefore, be
	/// careful when working with files, as relative paths may not point to where
	/// you expect them point to.
	///
	/// An application can determine whether it is running as a daemon by checking
	/// for the "application.runAsDaemon" configuration property.
	///
	///     if (config().getBool("application.runAsDaemon", false))
	///     {
	///         // do daemon specific things
	///     }
	///
	/// When running as a daemon, specifying the --pidfile option (e.g.,
	/// --pidfile=/var/run/sample.pid) may be useful to record the process ID of 
	/// the daemon in a file. The PID file will be removed when the daemon process 
	/// terminates (but not, if it crashes).
{
public:
	ServerApplication();
		/// Creates the ServerApplication.

	~ServerApplication();
		/// Destroys the ServerApplication.
		
	bool isInteractive() const;
		/// Returns true if the application runs from the command line.
		/// Returns false if the application runs as a Unix daemon
		/// or Windows service.

	int run(int argc, char** argv);
		/// Runs the application by performing additional initializations
		/// and calling the main() method.
		
	int run(const std::vector<std::string>& args);
		/// Runs the application by performing additional initializations
		/// and calling the main() method.

#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
	int run(int argc, wchar_t** argv);
		/// Runs the application by performing additional initializations
		/// and calling the main() method.
		///
		/// This Windows-specific version of init is used for passing
		/// Unicode command line arguments from wmain().
#endif

	static void terminate();
		/// Sends a friendly termination request to the application.
		/// If the application's main thread is waiting in 
		/// waitForTerminationRequest(), this method will return
		/// and the application can shut down.
		
protected:
	int run();
	void waitForTerminationRequest();
#if !defined(_WIN32_WCE)
	void defineOptions(OptionSet& options);
#endif

private:
#if defined(POCO_VXWORKS)
	static Poco::Event _terminate;
#elif defined(POCO_OS_FAMILY_UNIX)
	void handleDaemon(const std::string& name, const std::string& value);
	void handlePidFile(const std::string& name, const std::string& value);
	bool isDaemon(int argc, char** argv);
	void beDaemon();
#if defined(POCO_ANDROID)
	static Poco::Event _terminate;
#endif
#elif defined(POCO_OS_FAMILY_WINDOWS)
#if !defined(_WIN32_WCE)
	enum Action
	{
		SRV_RUN,
		SRV_REGISTER,
		SRV_UNREGISTER
	};
	static BOOL __stdcall ConsoleCtrlHandler(DWORD ctrlType);
	static void __stdcall ServiceControlHandler(DWORD control);
#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
	static void __stdcall ServiceMain(DWORD argc, LPWSTR* argv);
#else
	static void __stdcall ServiceMain(DWORD argc, LPTSTR* argv);
#endif

	bool hasConsole();
	bool isService();
	void beService();
	void registerService();
	void unregisterService();
	void handleRegisterService(const std::string& name, const std::string& value);
	void handleUnregisterService(const std::string& name, const std::string& value);
	void handleDisplayName(const std::string& name, const std::string& value);
	void handleDescription(const std::string& name, const std::string& value);
	void handleStartup(const std::string& name, const std::string& value);	
	
	Action      _action;
	std::string _displayName;
	std::string _description;
	std::string _startup;

	static Poco::Event           _terminated;
	static SERVICE_STATUS        _serviceStatus; 
	static SERVICE_STATUS_HANDLE _serviceStatusHandle; 
#endif // _WIN32_WCE
	static Poco::NamedEvent      _terminate;
#endif
};


} } // namespace Poco::Util


//
// Macro to implement main()
//
#if defined(_WIN32) && defined(POCO_WIN32_UTF8)
	#define POCO_SERVER_MAIN(App) \
	int wmain(int argc, wchar_t** argv)	\
	{									\
		try 							\
		{								\
			App app;					\
			return app.run(argc, argv);	\
		}								\
		catch (Poco::Exception& exc)	\
		{								\
			std::cerr << exc.displayText() << std::endl;	\
			return Poco::Util::Application::EXIT_SOFTWARE; 	\
		}								\
	}
#elif defined(POCO_VXWORKS)
	#define POCO_SERVER_MAIN(App) \
	int pocoSrvMain(const char* appName, ...) 				\
	{ 														\
		std::vector<std::string> args; 						\
		args.push_back(std::string(appName)); 				\
		va_list vargs; 										\
		va_start(vargs, appName); 							\
		const char* arg = va_arg(vargs, const char*); 		\
		while (arg) 										\
		{ 													\
			args.push_back(std::string(arg));				\
			arg = va_arg(vargs, const char*); 				\
		} 													\
		va_end(vargs); 										\
		try													\
		{ 													\
			App app;										\
			return app.run(args); 							\
		} 													\
		catch (Poco::Exception& exc)						\
		{													\
			std::cerr << exc.displayText() << std::endl;	\
			return Poco::Util::Application::EXIT_SOFTWARE; 	\
		}													\
	}
#else
	#define POCO_SERVER_MAIN(App) \
	int main(int argc, char** argv)		\
	{									\
		try 							\
		{								\
			App app;					\
			return app.run(argc, argv);	\
		}								\
		catch (Poco::Exception& exc)	\
		{								\
			std::cerr << exc.displayText() << std::endl;	\
			return Poco::Util::Application::EXIT_SOFTWARE; 	\
		}								\
	}
#endif


#endif // Util_ServerApplication_INCLUDED
