//
// LoggingRegistry.cpp
//
// Library: Foundation
// Package: Logging
// Module:  LoggingRegistry
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/LoggingRegistry.h"
#include "Poco/SingletonHolder.h"


namespace Poco {


LoggingRegistry::LoggingRegistry()
{
}


LoggingRegistry::~LoggingRegistry()
{
}


Channel* LoggingRegistry::channelForName(const std::string& name) const
{
	FastMutex::ScopedLock lock(_mutex);
	
	ChannelMap::const_iterator it = _channelMap.find(name);
	if (it != _channelMap.end())
		return const_cast<Channel*>(it->second.get());
	else
		throw NotFoundException("logging channel", name);
}


Formatter* LoggingRegistry::formatterForName(const std::string& name) const
{
	FastMutex::ScopedLock lock(_mutex);

	FormatterMap::const_iterator it = _formatterMap.find(name);
	if (it != _formatterMap.end())
		return const_cast<Formatter*>(it->second.get());
	else
		throw NotFoundException("logging formatter", name);
}


void LoggingRegistry::registerChannel(const std::string& name, Channel* pChannel)
{
	FastMutex::ScopedLock lock(_mutex);

	_channelMap[name] = ChannelPtr(pChannel, true);
}

	
void LoggingRegistry::registerFormatter(const std::string& name, Formatter* pFormatter)
{
	FastMutex::ScopedLock lock(_mutex);

	_formatterMap[name] = FormatterPtr(pFormatter, true);
}


void LoggingRegistry::unregisterChannel(const std::string& name)
{
	FastMutex::ScopedLock lock(_mutex);

	ChannelMap::iterator it = _channelMap.find(name);
	if (it != _channelMap.end())
		_channelMap.erase(it);
	else
		throw NotFoundException("logging channel", name);
}


void LoggingRegistry::unregisterFormatter(const std::string& name)
{
	FastMutex::ScopedLock lock(_mutex);

	FormatterMap::iterator it = _formatterMap.find(name);
	if (it != _formatterMap.end())
		_formatterMap.erase(it);
	else
		throw NotFoundException("logging formatter", name);
}


void LoggingRegistry::clear()
{
	FastMutex::ScopedLock lock(_mutex);

	_channelMap.clear();
	_formatterMap.clear();
}


namespace
{
	static SingletonHolder<LoggingRegistry> sh;
}


LoggingRegistry& LoggingRegistry::defaultRegistry()
{
	return *sh.get();
}


} // namespace Poco
