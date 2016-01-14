//
// Process_VMS.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Process_VMS.h#3 $
//
// Library: Foundation
// Package: Processes
// Module:  Process
//
// Definition of the ProcessImpl class for OpenVMS.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Process_VMS_INCLUDED
#define Foundation_Process_VMS_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/RefCountedObject.h"
#include <vector>
#include <map>
#include <unistd.h>


namespace Poco {


class Pipe;


class ProcessHandleImpl: public RefCountedObject
{
public:
	ProcessHandleImpl(pid_t pid);
	~ProcessHandleImpl();
	
	pid_t id() const;
	int wait() const;
	
private:
	pid_t _pid;
};


class ProcessImpl
{
public:
	typedef pid_t PIDImpl;
	typedef std::vector<std::string> ArgsImpl;
	typedef std::map<std::string, std::string> EnvImpl;
	
	static PIDImpl idImpl();
	static void timesImpl(long& userTime, long& kernelTime);
	static ProcessHandleImpl* launchImpl(
		const std::string& command, 
		const ArgsImpl& args, 
		const std::string& initialDirectory,
		Pipe* inPipe, 
		Pipe* outPipe, 
		Pipe* errPipe,
		const EnvImpl& env);
	static int waitImpl(PIDImpl pid);
	static void killImpl(ProcessHandleImpl& handle);
	static void killImpl(PIDImpl pid);
	static bool isRunningImpl(const ProcessHandleImpl& handle);
	static bool isRunningImpl(PIDImpl pid);
	static void requestTerminationImpl(PIDImpl pid);
};


} // namespace Poco


#endif // Foundation_Process_VMS_INCLUDED
