//
// Process_VX.cpp
//
// $Id: //poco/1.4/Foundation/src/Process_VX.cpp#3 $
//
// Library: Foundation
// Package: Processes
// Module:  Process
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Process_VX.h"
#include "Poco/Exception.h"


namespace Poco {


//
// ProcessHandleImpl
//
ProcessHandleImpl::ProcessHandleImpl(int pid):
	_pid(pid)
{
}


ProcessHandleImpl::~ProcessHandleImpl()
{
}


int ProcessHandleImpl::id() const
{
	return _pid;
}


int ProcessHandleImpl::wait() const
{
	throw Poco::NotImplementedException("Process::wait()");
}


//
// ProcessImpl
//
ProcessImpl::PIDImpl ProcessImpl::idImpl()
{
	return 0;
}


void ProcessImpl::timesImpl(long& userTime, long& kernelTime)
{
	userTime   = 0;
	kernelTime = 0;
}


ProcessHandleImpl* ProcessImpl::launchImpl(const std::string& command, const ArgsImpl& args, const std::string& initialDirectory,Pipe* inPipe, Pipe* outPipe, Pipe* errPipe, const EnvImpl& env)
{
	throw Poco::NotImplementedException("Process::launch()");
}


void ProcessImpl::killImpl(ProcessHandleImpl& handle)
{
	throw Poco::NotImplementedException("Process::kill()");
}


void ProcessImpl::killImpl(PIDImpl pid)
{
	throw Poco::NotImplementedException("Process::kill()");
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
	throw Poco::NotImplementedException("Process::requestTermination()");
}


} // namespace Poco
