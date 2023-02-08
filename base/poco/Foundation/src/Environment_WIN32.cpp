//
// Environment_WIN32.cpp
//
// Library: Foundation
// Package: Core
// Module:  Environment
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Environment_WIN32.h"
#include "Poco/Exception.h"
#include <sstream>
#include <cstring>
#include "Poco/UnWindows.h"
#include <winsock2.h>
#include <wincrypt.h>
#include <ws2ipdef.h>
#include <iphlpapi.h>


namespace Poco {


std::string EnvironmentImpl::getImpl(const std::string& name)
{
	DWORD len = GetEnvironmentVariableA(name.c_str(), 0, 0);
	if (len == 0) throw NotFoundException(name);
	char* buffer = new char[len];
	GetEnvironmentVariableA(name.c_str(), buffer, len);
	std::string result(buffer);
	delete [] buffer;
	return result;
}


bool EnvironmentImpl::hasImpl(const std::string& name)
{
	DWORD len = GetEnvironmentVariableA(name.c_str(), 0, 0);
	return len > 0;
}


void EnvironmentImpl::setImpl(const std::string& name, const std::string& value)
{
	if (SetEnvironmentVariableA(name.c_str(), value.c_str()) == 0)
	{
		std::string msg = "cannot set environment variable: ";
		msg.append(name);
		throw SystemException(msg);
	}
}


std::string EnvironmentImpl::osNameImpl()
{
	OSVERSIONINFO vi;
	vi.dwOSVersionInfoSize = sizeof(vi);
	if (GetVersionEx(&vi) == 0) throw SystemException("Cannot get OS version information");
	switch (vi.dwPlatformId)
	{
	case VER_PLATFORM_WIN32s:
		return "Windows 3.x";
	case VER_PLATFORM_WIN32_WINDOWS:
		return vi.dwMinorVersion == 0 ? "Windows 95" : "Windows 98";
	case VER_PLATFORM_WIN32_NT:
		return "Windows NT";
	default:
		return "Unknown";
	}
}


std::string EnvironmentImpl::osDisplayNameImpl()
{
	OSVERSIONINFOEX vi;	// OSVERSIONINFOEX is supported starting at Windows 2000 
	vi.dwOSVersionInfoSize = sizeof(vi);
	if (GetVersionEx((OSVERSIONINFO*) &vi) == 0) throw SystemException("Cannot get OS version information");
	switch (vi.dwMajorVersion)
	{
	case 10:
		switch (vi.dwMinorVersion)
		{
		case 0:
			return vi.wProductType == VER_NT_WORKSTATION ? "Windows 10" : "Windows Server 2016";
		}
	case 6:
		switch (vi.dwMinorVersion)
		{
		case 0:
			return vi.wProductType == VER_NT_WORKSTATION ? "Windows Vista" : "Windows Server 2008";
		case 1:
			return vi.wProductType == VER_NT_WORKSTATION ? "Windows 7" : "Windows Server 2008 R2";
		case 2:
			return vi.wProductType == VER_NT_WORKSTATION ? "Windows 8" : "Windows Server 2012";
		case 3:
			return vi.wProductType == VER_NT_WORKSTATION ? "Windows 8.1" : "Windows Server 2012 R2";
		default:
			return "Unknown";
		}
	case 5:
		switch (vi.dwMinorVersion)
		{
		case 0:
			return "Windows 2000";
		case 1:
			return "Windows XP";
		case 2:
			return "Windows Server 2003/Windows Server 2003 R2";
		default:
			return "Unknown";
		}
	default:
		return "Unknown";
	}
}


std::string EnvironmentImpl::osVersionImpl()
{
	OSVERSIONINFO vi;
	vi.dwOSVersionInfoSize = sizeof(vi);
	if (GetVersionEx(&vi) == 0) throw SystemException("Cannot get OS version information");
	std::ostringstream str;
	str << vi.dwMajorVersion << "." << vi.dwMinorVersion << " (Build " << (vi.dwBuildNumber & 0xFFFF);
	if (vi.szCSDVersion[0]) str << ": " << vi.szCSDVersion;
	str << ")";
	return str.str();
}


std::string EnvironmentImpl::osArchitectureImpl()
{
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	switch (si.wProcessorArchitecture)
	{
	case PROCESSOR_ARCHITECTURE_INTEL:
		return "IA32";
	case PROCESSOR_ARCHITECTURE_MIPS:
		return "MIPS";
	case PROCESSOR_ARCHITECTURE_ALPHA:
		return "ALPHA";
	case PROCESSOR_ARCHITECTURE_PPC:
		return "PPC";
	case PROCESSOR_ARCHITECTURE_IA64:
		return "IA64";
#ifdef PROCESSOR_ARCHITECTURE_IA32_ON_WIN64
	case PROCESSOR_ARCHITECTURE_IA32_ON_WIN64:
		return "IA64/32";
#endif
#ifdef PROCESSOR_ARCHITECTURE_AMD64
	case PROCESSOR_ARCHITECTURE_AMD64:
		return "AMD64";
#endif
	default:
		return "Unknown";
	}
}


std::string EnvironmentImpl::nodeNameImpl()
{
	char name[MAX_COMPUTERNAME_LENGTH + 1];
	DWORD size = sizeof(name);
	if (GetComputerNameA(name, &size) == 0) throw SystemException("Cannot get computer name");
	return std::string(name);
}


void EnvironmentImpl::nodeIdImpl(NodeId& id)
{
	std::memset(&id, 0, sizeof(id));

	PIP_ADAPTER_INFO pAdapterInfo;
	PIP_ADAPTER_INFO pAdapter = 0;
	ULONG len    = sizeof(IP_ADAPTER_INFO);
	pAdapterInfo = reinterpret_cast<IP_ADAPTER_INFO*>(new char[len]);
	// Make an initial call to GetAdaptersInfo to get
	// the necessary size into len
	DWORD rc = GetAdaptersInfo(pAdapterInfo, &len);
	if (rc == ERROR_BUFFER_OVERFLOW) 
	{
		delete [] reinterpret_cast<char*>(pAdapterInfo);
		pAdapterInfo = reinterpret_cast<IP_ADAPTER_INFO*>(new char[len]);
	}
	else if (rc != ERROR_SUCCESS)
	{
		return;
	}
	if (GetAdaptersInfo(pAdapterInfo, &len) == NO_ERROR) 
	{
		pAdapter = pAdapterInfo;
		bool found = false;
		while (pAdapter && !found) 
		{
			if (pAdapter->Type == MIB_IF_TYPE_ETHERNET && pAdapter->AddressLength == sizeof(id))
			{
				found = true;
				std::memcpy(&id, pAdapter->Address, pAdapter->AddressLength);
			}
			pAdapter = pAdapter->Next;
		}
	}
	delete [] reinterpret_cast<char*>(pAdapterInfo);
}


unsigned EnvironmentImpl::processorCountImpl()
{
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	return si.dwNumberOfProcessors;
}


} // namespace Poco
