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
#include <cwchar>
#include <map>


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
	// An empty argument has to be quoted too, or it is serialized as just another separator and the
	// child does not see it at all.
	bool containsQuotableChar = arg.empty() || std::string::npos != arg.find_first_of(" \t\n\v\"");
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

	// `GetStartupInfoW` filled these with the current process's own handles. Clear them, so that
	// only handles duplicated below are ever passed to the child or closed afterwards.
	startupInfo.hStdInput = 0;
	startupInfo.hStdOutput = 0;
	startupInfo.hStdError = 0;

	HANDLE hProc = GetCurrentProcess();
	bool mustInheritHandles = false;
	auto closeStdHandles = [&startupInfo]()
	{
		if (startupInfo.hStdInput) CloseHandle(startupInfo.hStdInput);
		if (startupInfo.hStdOutput) CloseHandle(startupInfo.hStdOutput);
		if (startupInfo.hStdError) CloseHandle(startupInfo.hStdError);
	};
	auto inheritHandle = [&](HANDLE source, HANDLE& target)
	{
		if (!DuplicateHandle(hProc, source, hProc, &target, 0, 1, DUPLICATE_SAME_ACCESS))
		{
			target = 0;
			closeStdHandles();
			throw SystemException("Cannot duplicate a standard handle for the child process", command);
		}
		mustInheritHandles = true;
	};
	// `GetStdHandle` returns `INVALID_HANDLE_VALUE` on failure and NULL when there is no such handle.
	auto stdHandle = [](DWORD which)
	{
		HANDLE h = GetStdHandle(which);
		return h == INVALID_HANDLE_VALUE ? HANDLE(0) : h;
	};

	if (inPipe)
	{
		inheritHandle(inPipe->readHandle(), startupInfo.hStdInput);
		inPipe->close(Pipe::CLOSE_READ);
	}
	else if (HANDLE h = stdHandle(STD_INPUT_HANDLE))
	{
		inheritHandle(h, startupInfo.hStdInput);
	}
	// outPipe may be the same as errPipe, so we duplicate first and close later.
	if (outPipe)
		inheritHandle(outPipe->writeHandle(), startupInfo.hStdOutput);
	else if (HANDLE h = stdHandle(STD_OUTPUT_HANDLE))
		inheritHandle(h, startupInfo.hStdOutput);
	if (errPipe)
		inheritHandle(errPipe->writeHandle(), startupInfo.hStdError);
	else if (HANDLE h = stdHandle(STD_ERROR_HANDLE))
		inheritHandle(h, startupInfo.hStdError);
	if (outPipe) outPipe->close(Pipe::CLOSE_WRITE);
	if (errPipe) errPipe->close(Pipe::CLOSE_WRITE);

	if (mustInheritHandles)
	{
		startupInfo.dwFlags |= STARTF_USESTDHANDLES;
	}

	std::wstring uinitialDirectory;
	UnicodeConverter::toUTF16(initialDirectory, uinitialDirectory);
	const wchar_t* workingDirectory = uinitialDirectory.empty() ? 0 : uinitialDirectory.c_str();

	/// `POCO_WIN32_UTF8` makes `Process::Env` UTF-8, so the environment block has to be handed to
	/// `CreateProcessW` as UTF-16 together with `CREATE_UNICODE_ENVIRONMENT`. A narrow block would
	/// be decoded in the active ANSI code page and mangle every non-ASCII name or value.
	///
	/// A block passed to `CreateProcessW` replaces the whole environment of the child, while the
	/// POSIX implementation inherits the current one and applies `env` on top of it. To keep that
	/// contract, the block starts from the current environment. Variable names are case-insensitive
	/// on Windows, and `CreateProcessW` wants the block sorted by name the same way.
	std::wstring uenv;
	if (!env.empty())
	{
		struct NameLess
		{
			bool operator()(const std::wstring& lhs, const std::wstring& rhs) const
			{
				return CompareStringOrdinal(lhs.c_str(), static_cast<int>(lhs.size()), rhs.c_str(), static_cast<int>(rhs.size()), 1) == CSTR_LESS_THAN;
			}
		};
		std::map<std::wstring, std::wstring, NameLess> variables;

		if (wchar_t* current = GetEnvironmentStringsW())
		{
			for (const wchar_t* entry = current; *entry; entry += wcslen(entry) + 1)
			{
				// Skip the first character when looking for the separator: the hidden per-drive
				// current directories are stored as variables whose names start with `=`.
				const wchar_t* separator = wcschr(entry + 1, L'=');
				if (!separator)
					continue;
				variables.emplace(std::wstring(entry, separator), std::wstring(separator + 1));
			}
			FreeEnvironmentStringsW(current);
		}
		else
		{
			throw SystemException("Cannot get the environment of the current process", command);
		}

		for (Poco::Process::Env::const_iterator it = env.begin(); it != env.end(); ++it)
		{
			std::wstring uname;
			std::wstring uvalue;
			UnicodeConverter::toUTF16(it->first, uname);
			UnicodeConverter::toUTF16(it->second, uvalue);
			variables[uname] = uvalue;
		}

		for (const auto& [uname, uvalue] : variables)
		{
			uenv.append(uname);
			uenv.append(1, L'=');
			uenv.append(uvalue);
			uenv.append(1, L'\0');
		}
		uenv.append(1, L'\0');
	}

	PROCESS_INFORMATION processInfo;
	DWORD creationFlags = GetConsoleWindow() ? 0 : CREATE_NO_WINDOW;
	if (!uenv.empty()) creationFlags |= CREATE_UNICODE_ENVIRONMENT;
	BOOL rc = CreateProcessW(
		applicationName,
		const_cast<wchar_t*>(ucommandLine.c_str()),
		NULL, // processAttributes
		NULL, // threadAttributes
		mustInheritHandles,
		creationFlags,
		uenv.empty() ? NULL : (LPVOID)uenv.data(),
		workingDirectory,
		&startupInfo,
		&processInfo
	);
	closeStdHandles();
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
	HANDLE hProc = OpenProcess(PROCESS_TERMINATE, 0, pid);
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
	HANDLE hProc = OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid);
	if (!hProc) return false;
	bool result = true;
	DWORD exitCode;
	BOOL rc = GetExitCodeProcess(hProc, &exitCode);
	if (!rc || exitCode != STILL_ACTIVE) result = false;
	/// The handle is ours - not closing it here would leak a kernel handle on every poll.
	CloseHandle(hProc);
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
