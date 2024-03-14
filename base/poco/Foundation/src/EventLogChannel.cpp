//
// EventLogChannel.cpp
//
// Library: Foundation
// Package: Logging
// Module:  EventLogChannel
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/EventLogChannel.h"
#include "Poco/Message.h"
#include "Poco/String.h"
#include "pocomsg.h"


namespace Poco {


const std::string EventLogChannel::PROP_NAME    = "name";
const std::string EventLogChannel::PROP_HOST    = "host";
const std::string EventLogChannel::PROP_LOGHOST = "loghost";
const std::string EventLogChannel::PROP_LOGFILE = "logfile";


EventLogChannel::EventLogChannel(): 
	_logFile("Application"),
	_h(0)
{
	const DWORD maxPathLen = MAX_PATH + 1;
	char name[maxPathLen];
	int n = GetModuleFileNameA(NULL, name, maxPathLen);
	if (n > 0)
	{
		char* end = name + n - 1;
		while (end > name && *end != '\\') --end;
		if (*end == '\\') ++end;
		_name = end;
	}
}


EventLogChannel::EventLogChannel(const std::string& name): 
	_name(name), 
	_logFile("Application"),
	_h(0)
{
}


EventLogChannel::EventLogChannel(const std::string& name, const std::string& host): 
	_name(name), 
	_host(host),
	_logFile("Application"),
	_h(0)
{
}


EventLogChannel::~EventLogChannel()
{
	try
	{
		close();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void EventLogChannel::open()
{
	setUpRegistry();
	_h = RegisterEventSource(_host.empty() ? NULL : _host.c_str(), _name.c_str());
	if (!_h) throw SystemException("cannot register event source");
}


void EventLogChannel::close()
{
	if (_h) DeregisterEventSource(_h);
	_h = 0;
}


void EventLogChannel::log(const Message& msg)
{
	if (!_h) open();
	const char* pMsg = msg.getText().c_str();
	ReportEvent(_h, getType(msg), getCategory(msg), POCO_MSG_LOG, NULL, 1, 0, &pMsg, NULL); 
}


void EventLogChannel::setProperty(const std::string& name, const std::string& value)
{
	if (icompare(name, PROP_NAME) == 0)
		_name = value;
	else if (icompare(name, PROP_HOST) == 0)
		_host = value;
	else if (icompare(name, PROP_LOGHOST) == 0)
		_host = value;
	else if (icompare(name, PROP_LOGFILE) == 0)
		_logFile = value;
	else
		Channel::setProperty(name, value);
}


std::string EventLogChannel::getProperty(const std::string& name) const
{
	if (icompare(name, PROP_NAME) == 0)
		return _name;
	else if (icompare(name, PROP_HOST) == 0)
		return _host;
	else if (icompare(name, PROP_LOGHOST) == 0)
		return _host;
	else if (icompare(name, PROP_LOGFILE) == 0)
		return _logFile;
	else
		return Channel::getProperty(name);
}


int EventLogChannel::getType(const Message& msg)
{
	switch (msg.getPriority())
	{
	case Message::PRIO_TRACE:
	case Message::PRIO_DEBUG:
	case Message::PRIO_INFORMATION:
		return EVENTLOG_INFORMATION_TYPE;
	case Message::PRIO_NOTICE:
	case Message::PRIO_WARNING:
		return EVENTLOG_WARNING_TYPE;
	default:
		return EVENTLOG_ERROR_TYPE;
	}
}


int EventLogChannel::getCategory(const Message& msg)
{
	switch (msg.getPriority())
	{
	case Message::PRIO_TRACE:
		return POCO_CTG_TRACE;
	case Message::PRIO_DEBUG:
		return POCO_CTG_DEBUG;
	case Message::PRIO_INFORMATION:
		return POCO_CTG_INFORMATION;
	case Message::PRIO_NOTICE:
		return POCO_CTG_NOTICE;
	case Message::PRIO_WARNING:
		return POCO_CTG_WARNING;
	case Message::PRIO_ERROR:
		return POCO_CTG_ERROR;
	case Message::PRIO_CRITICAL:
		return POCO_CTG_CRITICAL;
	case Message::PRIO_FATAL:
		return POCO_CTG_FATAL;
	default:
		return 0;
	}
}


void EventLogChannel::setUpRegistry() const
{
	std::string key = "SYSTEM\\CurrentControlSet\\Services\\EventLog\\";
	key.append(_logFile);
	key.append("\\");
	key.append(_name);
	HKEY hKey;
	DWORD disp;
	DWORD rc = RegCreateKeyEx(HKEY_LOCAL_MACHINE, key.c_str(), 0, NULL, REG_OPTION_NON_VOLATILE, KEY_ALL_ACCESS, NULL, &hKey, &disp);
	if (rc != ERROR_SUCCESS) return;
	
	if (disp == REG_CREATED_NEW_KEY)
	{
		std::string path;
		
		if (path.empty())
			path = findLibrary("PocoMsg.dll");
		
		if (!path.empty())
		{
			DWORD count = 8;
			DWORD types = 7;
			RegSetValueEx(hKey, "CategoryMessageFile", 0, REG_SZ, (const BYTE*) path.c_str(), static_cast<DWORD>(path.size() + 1));
			RegSetValueEx(hKey, "EventMessageFile", 0, REG_SZ, (const BYTE*) path.c_str(), static_cast<DWORD>(path.size() + 1));
			RegSetValueEx(hKey, "CategoryCount", 0, REG_DWORD, (const BYTE*) &count, static_cast<DWORD>(sizeof(count)));
			RegSetValueEx(hKey, "TypesSupported", 0, REG_DWORD, (const BYTE*) &types, static_cast<DWORD>(sizeof(types)));
		}
	}
	RegCloseKey(hKey);
}


std::string EventLogChannel::findLibrary(const char* name)
{
	std::string path;
	HMODULE dll = LoadLibraryA(name);
	if (dll)
	{
		const DWORD maxPathLen = MAX_PATH + 1;
		char name[maxPathLen];
		int n = GetModuleFileNameA(dll, name, maxPathLen);
		if (n > 0) path = name;
		FreeLibrary(dll);
	}
	return path;
}


} // namespace Poco
