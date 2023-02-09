//
// Environment_WINCE.cpp
//
// Library: Foundation
// Package: Core
// Module:  Environment
//
// Copyright (c) 2009-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Environment_WINCE.h"
#include "Poco/Exception.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/String.h"
#include "Poco/Path.h"
#include "Poco/NumberFormatter.h"
#include <sstream>
#include <cstring>
#include <windows.h>
#include <iphlpapi.h>


namespace Poco {


const std::string EnvironmentImpl::TEMP("TEMP");
const std::string EnvironmentImpl::TMP("TMP");
const std::string EnvironmentImpl::HOMEPATH("HOMEPATH");
const std::string EnvironmentImpl::COMPUTERNAME("COMPUTERNAME");
const std::string EnvironmentImpl::OS("OS");
const std::string EnvironmentImpl::NUMBER_OF_PROCESSORS("NUMBER_OF_PROCESSORS");
const std::string EnvironmentImpl::PROCESSOR_ARCHITECTURE("PROCESSOR_ARCHITECTURE");


std::string EnvironmentImpl::getImpl(const std::string& name)
{
	std::string value;
	if (!envVar(name, &value)) throw NotFoundException(name);
	return value;
}


bool EnvironmentImpl::hasImpl(const std::string& name)
{
	return envVar(name, 0);
}


void EnvironmentImpl::setImpl(const std::string& name, const std::string& value)
{
	throw NotImplementedException("Cannot set environment variables on Windows CE");
}


std::string EnvironmentImpl::osNameImpl()
{
	return "Windows CE";
}


std::string EnvironmentImpl::osDisplayNameImpl()
{
	return osNameImpl();
}


std::string EnvironmentImpl::osVersionImpl()
{
	OSVERSIONINFOW vi;
	vi.dwOSVersionInfoSize = sizeof(vi);
	if (GetVersionExW(&vi) == 0) throw SystemException("Cannot get OS version information");
	std::ostringstream str;
	str << vi.dwMajorVersion << "." << vi.dwMinorVersion << " (Build " << (vi.dwBuildNumber & 0xFFFF);
	std::string version;
	UnicodeConverter::toUTF8(vi.szCSDVersion, version);
	if (!version.empty()) str << ": " << version;
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
	case PROCESSOR_ARCHITECTURE_SHX:
		return "SHX";
	case PROCESSOR_ARCHITECTURE_ARM:
		return "ARM";
	default:
		return "Unknown";
	}
}


std::string EnvironmentImpl::nodeNameImpl()
{
    HKEY hKey;
	DWORD dwDisposition;
    if (RegCreateKeyExW(HKEY_LOCAL_MACHINE, L"\\Ident", 0, 0, 0, 0, 0, &hKey, &dwDisposition) != ERROR_SUCCESS) 
		throw SystemException("Cannot get node name", "registry key not found");

	std::string value;
    DWORD dwType;
    BYTE bData[1026];
    DWORD dwData = sizeof(bData);
	if (RegQueryValueExW(hKey, L"Name", 0, &dwType, bData, &dwData) == ERROR_SUCCESS)
	{
		switch (dwType) 
		{
		case REG_SZ:
			UnicodeConverter::toUTF8(reinterpret_cast<wchar_t*>(bData), value);
			break;

		default:
			RegCloseKey(hKey);
			throw SystemException("Cannot get node name", "registry value has wrong type");
		}
	}
	else
	{
	    RegCloseKey(hKey);
		throw SystemException("Cannot get node name", "registry value not found");
	}
    RegCloseKey(hKey);
	return value;
}


void EnvironmentImpl::nodeIdImpl(NodeId& id)
{
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
		throw SystemException("cannot get network adapter list");
	}
	try
	{
		bool found = false;
		if (GetAdaptersInfo(pAdapterInfo, &len) == NO_ERROR) 
		{
			pAdapter = pAdapterInfo;
			while (pAdapter && !found) 
			{
				if (pAdapter->Type == MIB_IF_TYPE_ETHERNET && pAdapter->AddressLength == sizeof(id))
				{
					std::memcpy(&id, pAdapter->Address, pAdapter->AddressLength);
					found = true;
				}
				pAdapter = pAdapter->Next;
			}
		}
		else throw SystemException("cannot get network adapter list");
		if (!found) throw SystemException("no Ethernet adapter found");
	}
	catch (Exception&)
	{
		delete [] reinterpret_cast<char*>(pAdapterInfo);
		throw;
	}
	delete [] reinterpret_cast<char*>(pAdapterInfo);
}


unsigned EnvironmentImpl::processorCountImpl()
{
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	return si.dwNumberOfProcessors;
}


bool EnvironmentImpl::envVar(const std::string& name, std::string* value)
{
	if (icompare(name, TEMP) == 0)
	{
		if (value) *value = Path::temp();
	}
	else if (icompare(name, TMP) == 0)
	{
		if (value) *value = Path::temp();
	}
	else if (icompare(name, HOMEPATH) == 0)
	{
		if (value) *value = Path::home();
	}
	else if (icompare(name, COMPUTERNAME) == 0)
	{
		if (value) *value = nodeNameImpl();
	}
	else if (icompare(name, OS) == 0)
	{
		if (value) *value = osNameImpl();
	}
	else if (icompare(name, NUMBER_OF_PROCESSORS) == 0)
	{
		if (value) *value = NumberFormatter::format(processorCountImpl());
	}
	else if (icompare(name, PROCESSOR_ARCHITECTURE) == 0)
	{
		if (value) *value = osArchitectureImpl();
	}
	else return false;
	return true;
}


} // namespace Poco
