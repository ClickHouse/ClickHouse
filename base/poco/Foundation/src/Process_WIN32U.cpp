//
// Process_WIN32U.cpp
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


#include "Poco/Process_WIN32U.h"
#include "Poco/Exception.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NamedEvent.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/Pipe.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/String.h"


namespace Poco {


//
// ProcessHandleImpl
//
ProcessHandleImpl::ProcessHandleImpl(HANDLE hProcess, UInt32 pid) :
	_hProcess(hProcess),
	_pid(pid)
{
}


ProcessHandleImpl::~ProcessHandleImpl()
{
	closeHandle();
}


void ProcessHandleImpl::closeHandle()
{
	if (_hProcess)
	{
		CloseHandle(_hProcess);
		_hProcess = NULL;
	}
}


UInt32 ProcessHandleImpl::id() const
{
	return _pid;
}


HANDLE ProcessHandleImpl::process() const
{
	return _hProcess;
}


int ProcessHandleImpl::wait() const
{
	DWORD rc = WaitForSingleObject(_hProcess, INFINITE);
	if (rc != WAIT_OBJECT_0)
		throw SystemException("Wait failed for process", NumberFormatter::format(_pid));

	DWORD exitCode;
	if (GetExitCodeProcess(_hProcess, &exitCode) == 0)
		throw SystemException("Cannot get exit code for process", NumberFormatter::format(_pid));

	return exitCode;
}


//
// ProcessImpl
//
ProcessImpl::PIDImpl ProcessImpl::idImpl()
{
	return GetCurrentProcessId();
}


void ProcessImpl::timesImpl(long& userTime, long& kernelTime)
{
	FILETIME ftCreation;
	FILETIME ftExit;
	FILETIME ftKernel;
	FILETIME ftUser;

	if (GetProcessTimes(GetCurrentProcess(), &ftCreation, &ftExit, &ftKernel, &ftUser) != 0)
	{
		ULARGE_INTEGER time;
		time.LowPart = ftKernel.dwLowDateTime;
		time.HighPart = ftKernel.dwHighDateTime;
		kernelTime = long(time.QuadPart / 10000000L);
		time.LowPart = ftUser.dwLowDateTime;
		time.HighPart = ftUser.dwHighDateTime;
		userTime = long(time.QuadPart / 10000000L);
	} 
	else
	{
		userTime = kernelTime = -1;
	}
}


static bool argNeedsEscaping(const std::string& arg)
{
	bool containsQuotableChar = std::string::npos != arg.find_first_of(" \t\n\v\"");
	// Assume args that start and end with quotes are already quoted and do not require further quoting.
	// There is probably code out there written before launch() escaped the arguments that does its own
	// escaping of arguments. This ensures we do not interfere with those arguments.
	bool isAlreadyQuoted = arg.size() > 1 && '\"' == arg[0] && '\"' == arg[arg.size() - 1];
	return containsQuotableChar && !isAlreadyQuoted;
}


// Based on code from https://blogs.msdn.microsoft.com/twistylittlepassagesallalike/2011/04/23/everyone-quotes-command-line-arguments-the-wrong-way/
static std::string escapeArg(const std::string& arg)
{
	if (argNeedsEscaping(arg))
	{
		std::string quotedArg("\"");
		for (std::string::const_iterator it = arg.begin(); ; ++it)
		{
			unsigned backslashCount = 0;
			while (it != arg.end() && '\\' == *it)
			{
				++it;
				++backslashCount;
			}

			if (it == arg.end())
			{
				quotedArg.append(2 * backslashCount, '\\');
				break;
			} 
			else if ('"' == *it)
			{
				quotedArg.append(2 * backslashCount + 1, '\\');
				quotedArg.push_back('"');
			} 
			else
			{
				quotedArg.append(backslashCount, '\\');
				quotedArg.push_back(*it);
			}
		}
		quotedArg.push_back('"');
		return quotedArg;
	} 
	else
	{
		return arg;
	}
}


ProcessHandleImpl* ProcessImpl::launchImpl(const std::string& command, const ArgsImpl& args, const std::string& initialDirectory, Pipe* inPipe, Pipe* outPipe, Pipe* errPipe, const EnvImpl& env)
{
	std::string commandLine = escapeArg(command);
	for (ArgsImpl::const_iterator it = args.begin(); it != args.end(); ++it)
	{
		commandLine.append(" ");
		commandLine.append(escapeArg(*it));
	}

	std::wstring ucommandLine;
	UnicodeConverter::toUTF16(commandLine, ucommandLine);

	const wchar_t* applicationName = 0;
	std::wstring uapplicationName;
	if (command.size() > MAX_PATH)
	{
		Poco::Path p(command);
		if (p.isAbsolute())
		{
			UnicodeConverter::toUTF16(command, uapplicationName);
			if (p.getExtension().empty()) uapplicationName += L".EXE";
			applicationName = uapplicationName.c_str();
		}
	}

	STARTUPINFOW startupInfo;
	GetStartupInfoW(&startupInfo); // take defaults from current process
	startupInfo.cb = sizeof(STARTUPINFOW);
	startupInfo.lpReserved = NULL;
	startupInfo.lpDesktop = NULL;
	startupInfo.lpTitle = NULL;
	startupInfo.dwFlags = STARTF_FORCEOFFFEEDBACK;
	startupInfo.cbReserved2 = 0;
	startupInfo.lpReserved2 = NULL;

	HANDLE hProc = GetCurrentProcess();
	bool mustInheritHandles = false;
	if (inPipe)
	{
		DuplicateHandle(hProc, inPipe->readHandle(), hProc, &startupInfo.hStdInput, 0, TRUE, DUPLICATE_SAME_ACCESS);
		mustInheritHandles = true;
		inPipe->close(Pipe::CLOSE_READ);
	} 
	else if (GetStdHandle(STD_INPUT_HANDLE))
	{
		DuplicateHandle(hProc, GetStdHandle(STD_INPUT_HANDLE), hProc, &startupInfo.hStdInput, 0, TRUE, DUPLICATE_SAME_ACCESS);
		mustInheritHandles = true;
	} 
	else
	{
		startupInfo.hStdInput = 0;
	}
	// outPipe may be the same as errPipe, so we duplicate first and close later.
	if (outPipe)
	{
		DuplicateHandle(hProc, outPipe->writeHandle(), hProc, &startupInfo.hStdOutput, 0, TRUE, DUPLICATE_SAME_ACCESS);
		mustInheritHandles = true;
	} 
	else if (GetStdHandle(STD_OUTPUT_HANDLE))
	{
		DuplicateHandle(hProc, GetStdHandle(STD_OUTPUT_HANDLE), hProc, &startupInfo.hStdOutput, 0, TRUE, DUPLICATE_SAME_ACCESS);
		mustInheritHandles = true;
	} 
	else
	{
		startupInfo.hStdOutput = 0;
	}
	if (errPipe)
	{
		DuplicateHandle(hProc, errPipe->writeHandle(), hProc, &startupInfo.hStdError, 0, TRUE, DUPLICATE_SAME_ACCESS);
		mustInheritHandles = true;
	} 
	else if (GetStdHandle(STD_ERROR_HANDLE))
	{
		DuplicateHandle(hProc, GetStdHandle(STD_ERROR_HANDLE), hProc, &startupInfo.hStdError, 0, TRUE, DUPLICATE_SAME_ACCESS);
		mustInheritHandles = true;
	} 
	else
	{
		startupInfo.hStdError = 0;
	}
	if (outPipe) outPipe->close(Pipe::CLOSE_WRITE);
	if (errPipe) errPipe->close(Pipe::CLOSE_WRITE);

	if (mustInheritHandles)
	{
		startupInfo.dwFlags |= STARTF_USESTDHANDLES;
	}

	std::wstring uinitialDirectory;
	UnicodeConverter::toUTF16(initialDirectory, uinitialDirectory);
	const wchar_t* workingDirectory = uinitialDirectory.empty() ? 0 : uinitialDirectory.c_str();

	const char* pEnv = 0;
	std::vector<char> envChars;
	if (!env.empty())
	{
		envChars = getEnvironmentVariablesBuffer(env);
		pEnv = &envChars[0];
	}

	PROCESS_INFORMATION processInfo;
	DWORD creationFlags = GetConsoleWindow() ? 0 : CREATE_NO_WINDOW;
	BOOL rc = CreateProcessW(
		applicationName,
		const_cast<wchar_t*>(ucommandLine.c_str()),
		NULL, // processAttributes
		NULL, // threadAttributes
		mustInheritHandles,
		creationFlags,
		(LPVOID)pEnv,
		workingDirectory,
		&startupInfo,
		&processInfo
	);
	if (startupInfo.hStdInput) CloseHandle(startupInfo.hStdInput);
	if (startupInfo.hStdOutput) CloseHandle(startupInfo.hStdOutput);
	if (startupInfo.hStdError) CloseHandle(startupInfo.hStdError);
	if (rc)
	{
		CloseHandle(processInfo.hThread);
		return new ProcessHandleImpl(processInfo.hProcess, processInfo.dwProcessId);
	} 
	else throw SystemException("Cannot launch process", command);
}


void ProcessImpl::killImpl(ProcessHandleImpl& handle)
{
	if (handle.process())
	{
		if (TerminateProcess(handle.process(), 0) == 0)
		{
			handle.closeHandle();
			throw SystemException("cannot kill process");
		}
		handle.closeHandle();
	}
}

void ProcessImpl::killImpl(PIDImpl pid)
{
	HANDLE hProc = OpenProcess(PROCESS_TERMINATE, FALSE, pid);
	if (hProc)
	{
		if (TerminateProcess(hProc, 0) == 0)
		{
			CloseHandle(hProc);
			throw SystemException("cannot kill process");
		}
		CloseHandle(hProc);
	} 
	else
	{
		switch (GetLastError())
		{
		case ERROR_ACCESS_DENIED:
			throw NoPermissionException("cannot kill process");
		case ERROR_NOT_FOUND: 
			throw NotFoundException("cannot kill process");
		case ERROR_INVALID_PARAMETER:
			throw NotFoundException("cannot kill process");
		default:
			throw SystemException("cannot kill process");
		}
	}
}


bool ProcessImpl::isRunningImpl(const ProcessHandleImpl& handle)
{
	bool result = true;
	DWORD exitCode;
	BOOL rc = GetExitCodeProcess(handle.process(), &exitCode);
	if (!rc || exitCode != STILL_ACTIVE) result = false;
	return result;
}


bool ProcessImpl::isRunningImpl(PIDImpl pid)
{
	HANDLE hProc = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
	bool result = true;
	DWORD exitCode;
	BOOL rc = GetExitCodeProcess(hProc, &exitCode);
	if (!rc || exitCode != STILL_ACTIVE) result = false;
	return result;
}


void ProcessImpl::requestTerminationImpl(PIDImpl pid)
{
	NamedEvent ev(terminationEventName(pid));
	ev.set();
}


std::string ProcessImpl::terminationEventName(PIDImpl pid)
{
	std::string evName("POCOTRM");
	NumberFormatter::appendHex(evName, pid, 8);
	return evName;
}


} // namespace Poco
