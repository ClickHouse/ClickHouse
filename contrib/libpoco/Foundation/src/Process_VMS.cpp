//
// Process_VMS.cpp
//
// $Id: //poco/1.4/Foundation/src/Process_VMS.cpp#3 $
//
// Library: Foundation
// Package: Processes
// Module:  Process
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Process_VMS.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NamedEvent.h"
#include <sstream>
#include <times.h>
#include <time.h>


namespace Poco {


//
// ProcessHandleImpl
//
ProcessHandleImpl::ProcessHandleImpl(pid_t pid):
	_pid(pid)
{
}


ProcessHandleImpl::~ProcessHandleImpl()
{
}


pid_t ProcessHandleImpl::id() const
{
	return _pid;
}


int ProcessHandleImpl::wait() const
{
	int status;
	if (waitpid(_pid, &status, 0) != _pid)
		throw SystemException("Cannot wait for process", NumberFormatter::format(_pid));
	return WEXITSTATUS(status);
}


//
// ProcessImpl
//
ProcessImpl::PIDImpl ProcessImpl::idImpl()
{
	return getpid();
}


void ProcessImpl::timesImpl(long& userTime, long& kernelTime)
{
	struct tms buffer;
	times(&buffer)*1000/CLOCKS_PER_SEC;
	userTime   = buffer.tms_utime/CLOCKS_PER_SEC;
	kernelTime = buffer.tms_stime/CLOCKS_PER_SEC;
}


ProcessHandleImpl* ProcessImpl::launchImpl(const std::string& command, const ArgsImpl& args, const std::string& initialDirectory, Pipe* inPipe, Pipe* outPipe, Pipe* errPipe, const EnvImpl& env)
{
	char** argv = new char*[args.size() + 2];
	int i = 0;
	argv[i++] = const_cast<char*>(command.c_str());
	for (ArgsImpl::const_iterator it = args.begin(); it != args.end(); ++it) 
		argv[i++] = const_cast<char*>(it->c_str());
	argv[i] = NULL;
	try
	{
		int pid = vfork();
		if (pid < 0)
		{
			throw SystemException("Cannot fork process for", command);		
		}
		else if (pid == 0)
		{
			if (!initialDirectory.empty())
			{
				if (chdir(initialDirectory.c_str()) != 0)
				{
					std::stringstream str;
					str << "Cannot set initial directory to '" << initialDirectory << "' when forking process for";
					throw SystemException(str.str(), command);		
				}
			}
			setEnvironmentVariables(environment_variables);

			if (execvp(command.c_str(), argv) == -1)
				throw SystemException("Cannot execute command", command);
		}
		else 
		{
			delete [] argv;
			return new ProcessHandleImpl(pid);
		}
	}
	catch (...)
	{
		delete [] argv;
		throw;
	}
}


void ProcessImpl::killImpl(ProcessHandleImpl& handle)
{
	killImpl(handle.id());
}


void ProcessImpl::killImpl(PIDImpl pid)
{
	if (kill(pid, SIGKILL) != 0)
	{
		switch (errno)
		{
		case ESRCH:
			throw NotFoundException("cannot kill process");
		case EPERM:
			throw NoPermissionException("cannot kill process");
		default:
			throw SystemException("cannot kill process");
		}
	}
}


bool ProcessImpl::isRunningImpl(const ProcessHandleImpl& handle) 
{
	throw Poco::NotImplementedException("Process::is_running()");
}


bool ProcessImpl::isRunningImpl(PIDImpl pid) 
{
	throw Poco::NotImplementedException("Process::is_running()");
}


void ProcessImpl::requestTerminationImpl(PIDImpl pid)
{
	std::string evName("POCOTRM");
	evName.append(NumberFormatter::formatHex(pid, 8));
	NamedEvent ev(evName);
	ev.set();
}


} // namespace Poco
