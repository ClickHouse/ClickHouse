//
// ServerApplication.cpp
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
#include "Poco/FileStream.h"
#include "Poco/Exception.h"
#include "Poco/Process.h"
#include "Poco/NamedEvent.h"
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
#include <sys/socket.h>
#include <sys/wait.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <fstream>
#endif


using Poco::NumberFormatter;
using Poco::Exception;
using Poco::SystemException;


namespace Poco {
namespace Util {


#if defined(POCO_VXWORKS) || POCO_OS == POCO_OS_ANDROID
Poco::Event ServerApplication::_terminate;
#endif


ServerApplication::ServerApplication()
{
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
#if   defined(POCO_VXWORKS) || POCO_OS == POCO_OS_ANDROID
	_terminate.set();
#else
	Poco::Process::requestTermination(Process::id());
#endif
}


#if   defined(POCO_OS_FAMILY_UNIX)


//
// Unix specific code
//
void ServerApplication::waitForTerminationRequest()
{
#if POCO_OS != POCO_OS_ANDROID
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
#else // POCO_OS != POCO_OS_ANDROID
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


namespace
{
	/// The state shared by beDaemon() and detachFromDaemonLauncher(), in the process of the daemon.
	struct DaemonLauncherState
	{
		/// The socket that the launcher waits on.
		int fd = -1;
		bool detached = true;
		/// The original stdout and stderr, which are replaced by /dev/null when detaching.
		bool replaceStdout = false;
		bool replaceStderr = false;
		struct stat originalStdout{};
		struct stat originalStderr{};
	};

	DaemonLauncherState daemonLauncherState;

	/// A regular file is kept: it holds nothing of the caller, and the caller redirected the output there to read it.
	bool rememberOriginalStream(int fd, struct stat & original)
	{
		return fstat(fd, &original) == 0 && !S_ISREG(original.st_mode);
	}

	void replaceOriginalStreamByDevNull(int fd, const struct stat & original)
	{
		struct stat current;
		/// The stream was redirected (for example, to a log file) after the start: then it is not the one of the caller.
		if (fstat(fd, &current) != 0 || current.st_dev != original.st_dev || current.st_ino != original.st_ino)
			return;
		int nullFd = open("/dev/null", O_RDWR | O_CLOEXEC);
		if (nullFd < 0)
			throw SystemException("cannot open /dev/null");
		int rc = dup2(nullFd, fd);
		close(nullFd);
		if (rc < 0)
			throw SystemException("cannot attach the standard stream to /dev/null");
	}

	/// The process that was started: waits until the daemon initializes, and exits with its status.
	[[noreturn]] void runDaemonLauncher(pid_t pid, int fd)
	{
		char byte = 0;
		ssize_t res;
		do
			res = read(fd, &byte, 1);
		while (res < 0 && errno == EINTR);
		/// Initialized.
		if (res == 1)
			_exit(Application::EXIT_OK);

		/// No process can tell anymore that the daemon has initialized: the daemon has exited before that.
		int status = 0;
		pid_t waited;
		do
			waited = waitpid(pid, &status, 0);
		while (waited < 0 && errno == EINTR);
		if (waited < 0)
		{
			fprintf(stderr, "Cannot wait for the daemon process %d: %s\n", static_cast<int>(pid), strerror(errno));
			_exit(Application::EXIT_OSERR);
		}
		if (WIFEXITED(status))
		{
			int code = WEXITSTATUS(status);
			if (code != 0)
				fprintf(stderr, "The daemon process exited with code %d before it finished initialization\n", code);
			_exit(code);
		}
		if (WIFSIGNALED(status))
		{
			int sig = WTERMSIG(status);
			fprintf(stderr, "The daemon process was terminated by signal %d (%s) before it finished initialization\n", sig, strsignal(sig));
			_exit(128 + sig);
		}
		_exit(Application::EXIT_SOFTWARE);
	}
}


void ServerApplication::beDaemon()
{
#if !defined(POCO_NO_FORK_EXEC)
	/// A socket, not a pipe: a write to it can be done without SIGPIPE if the launcher has gone.
	int fds[2];
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
		throw SystemException("cannot create a socket pair for the daemon process");
	if (fcntl(fds[0], F_SETFD, FD_CLOEXEC) != 0 || fcntl(fds[1], F_SETFD, FD_CLOEXEC) != 0)
	{
		close(fds[0]);
		close(fds[1]);
		throw SystemException("cannot set FD_CLOEXEC on the socket pair for the daemon process");
	}
#if defined(SO_NOSIGPIPE)
	int noSigPipe = 1;
	if (setsockopt(fds[1], SOL_SOCKET, SO_NOSIGPIPE, &noSigPipe, sizeof(noSigPipe)) != 0)
	{
		close(fds[0]);
		close(fds[1]);
		throw SystemException("cannot set SO_NOSIGPIPE on the socket pair for the daemon process");
	}
#endif

	pid_t pid;
	if ((pid = fork()) < 0)
	{
		close(fds[0]);
		close(fds[1]);
		throw SystemException("cannot fork daemon process");
	}
	else if (pid != 0)
	{
		close(fds[1]);
		runDaemonLauncher(pid, fds[0]);
	}
	close(fds[0]);

	setsid();
	umask(027);

	// attach stdin to /dev/null instead of just closing it.
	// stdout and stderr stay attached to the ones of the launcher
	// until the initialization is done, so that the errors of the
	// startup are visible, and are attached to /dev/null after that,
	// see detachFromDaemonLauncher().
	FILE* fin  = freopen("/dev/null", "r+", stdin);
	if (!fin) throw Poco::OpenFileException("Cannot attach stdin to /dev/null");

	daemonLauncherState.fd = fds[1];
	daemonLauncherState.detached = false;
	daemonLauncherState.replaceStdout = rememberOriginalStream(STDOUT_FILENO, daemonLauncherState.originalStdout);
	daemonLauncherState.replaceStderr = rememberOriginalStream(STDERR_FILENO, daemonLauncherState.originalStderr);
#else
	throw Poco::NotImplementedException("platform does not allow fork/exec");
#endif
}


void ServerApplication::detachFromDaemonLauncher(bool initialized)
{
	DaemonLauncherState & state = daemonLauncherState;
	if (state.detached)
		return;
	state.detached = true;

	fflush(stdout);
	fflush(stderr);
	if (state.replaceStdout)
		replaceOriginalStreamByDevNull(STDOUT_FILENO, state.originalStdout);
	if (state.replaceStderr)
		replaceOriginalStreamByDevNull(STDERR_FILENO, state.originalStderr);

	if (state.fd >= 0)
	{
		if (initialized)
		{
			char byte = 0;
#if defined(MSG_NOSIGNAL)
			const int flags = MSG_NOSIGNAL;
#else
			const int flags = 0;
#endif
			ssize_t res;
			do
				res = send(state.fd, &byte, 1, flags);
			while (res < 0 && errno == EINTR);
			/// An error means that the launcher has gone (killed by the user), and nobody is waiting for it.
		}
		close(state.fd);
		state.fd = -1;
	}
}


int ServerApplication::getDaemonLauncherDescriptor()
{
	return daemonLauncherState.fd;
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
		Option("umask", "", "Set the daemon's umask (octal, e.g. 027).")
			.required(false)
			.repeatable(false)
			.argument("mask")
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleUMask)));

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


void ServerApplication::handleUMask(const std::string& name, const std::string& value)
{
	int mask = 0;
	for (std::string::const_iterator it = value.begin(); it != value.end(); ++it)
	{
		mask *= 8;
		if (*it >= '0' && *it <= '7')
			mask += *it - '0';
		else
			throw Poco::InvalidArgumentException("umask contains non-octal characters", value);
	}
	umask(mask);
}


void ServerApplication::handlePidFile(const std::string& name, const std::string& value)
{
	Poco::FileOutputStream ostr(value);
	if (ostr.good())
		ostr << Poco::Process::id() << std::endl;
	else
		throw Poco::CreateFileException("Cannot write PID to file", value);
	Poco::TemporaryFile::registerForDeletion(value);
}


#endif


} } // namespace Poco::Util
