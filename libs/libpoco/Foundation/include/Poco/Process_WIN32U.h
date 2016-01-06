//
// Process_WIN32U.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Process_WIN32U.h#3 $
//
// Library: Foundation
// Package: Processes
// Module:  Process
//
// Definition of the ProcessImpl class for WIN32.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Process_WIN32U_INCLUDED
#define Foundation_Process_WIN32U_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/RefCountedObject.h"
#include <vector>
#include <map>
#include "Poco/UnWindows.h"


namespace Poco {


class Pipe;


class Foundation_API ProcessHandleImpl: public RefCountedObject
{
public:
	ProcessHandleImpl(HANDLE _hProcess, UInt32 pid);
	~ProcessHandleImpl();
	
	UInt32 id() const;
	HANDLE process() const;
	int wait() const;
	void closeHandle();

private:
	HANDLE _hProcess;
	UInt32 _pid;

	ProcessHandleImpl(const ProcessHandleImpl&);
	ProcessHandleImpl& operator = (const ProcessHandleImpl&);
};


class Foundation_API ProcessImpl
{
public:
	typedef UInt32 PIDImpl;
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
	static void killImpl(ProcessHandleImpl& handle);
	static void killImpl(PIDImpl pid);
	static bool isRunningImpl(const ProcessHandleImpl& handle);
	static bool isRunningImpl(PIDImpl pid);
	static void requestTerminationImpl(PIDImpl pid);
	static std::string terminationEventName(PIDImpl pid);
};


} // namespace Poco


#endif // Foundation_Process_WIN32U_INCLUDED
