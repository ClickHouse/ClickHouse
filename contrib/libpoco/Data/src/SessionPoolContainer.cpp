//
// SessionPoolContainer.cpp
//
// $Id: //poco/Main/Data/src/SessionPoolContainer.cpp#3 $
//
// Library: Data
// Package: SessionPoolContainering
// Module:  SessionPoolContainer
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SessionPoolContainer.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Data/DataException.h"
#include "Poco/URI.h"
#include "Poco/String.h"
#include "Poco/Exception.h"
#include <algorithm>


using Poco::FastMutex;


namespace Poco {
namespace Data {


SessionPoolContainer::SessionPoolContainer()
{
}


SessionPoolContainer::~SessionPoolContainer()
{
}


void SessionPoolContainer::add(SessionPool* pPool)
{
	poco_check_ptr (pPool);

	FastMutex::ScopedLock lock(_mutex);
	if (_sessionPools.find(pPool->name()) != _sessionPools.end())
		throw SessionPoolExistsException("Session pool already exists: " + pPool->name());

	pPool->duplicate();
	_sessionPools.insert(SessionPoolMap::value_type(pPool->name(), pPool));
}


Session SessionPoolContainer::add(const std::string& sessionKey, 
	const std::string& connectionString,
	int minSessions, 
	int maxSessions, 
	int idleTime)
{
	std::string name = SessionPool::name(sessionKey, connectionString);

	FastMutex::ScopedLock lock(_mutex);
	SessionPoolMap::iterator it = _sessionPools.find(name);

	// pool already exists, silently return a session from it
	if (it != _sessionPools.end()) return it->second->get();

	SessionPool* pSP = 
		new SessionPool(sessionKey, connectionString, minSessions, maxSessions, idleTime);

	std::pair<SessionPoolMap::iterator, bool> ins = 
		_sessionPools.insert(SessionPoolMap::value_type(name, pSP));

	return ins.first->second->get();
}


bool SessionPoolContainer::isActive(const std::string& sessionKey,
		const std::string& connectionString) const
{
	std::string name = connectionString.empty() ? 
		sessionKey : SessionPool::name(sessionKey, connectionString);

	SessionPoolMap::const_iterator it = _sessionPools.find(name);
	if (it != _sessionPools.end() && it->second->isActive())
	{
		return true;
	}

	return false;
}


Session SessionPoolContainer::get(const std::string& name)
{
	return getPool(name).get();
}


SessionPool& SessionPoolContainer::getPool(const std::string& name)
{
	URI uri(name);
	std::string path = uri.getPath();
	poco_assert (!path.empty());
	std::string n = Session::uri(uri.getScheme(), path.substr(1));

	FastMutex::ScopedLock lock(_mutex);
	SessionPoolMap::iterator it = _sessionPools.find(n);
	if (_sessionPools.end() == it) throw NotFoundException(n);
	return *it->second;
}


void SessionPoolContainer::shutdown()
{
	SessionPoolMap::iterator it = _sessionPools.begin();
	SessionPoolMap::iterator end = _sessionPools.end();
	for (; it != end; ++it) it->second->shutdown();
}


} } // namespace Poco::Data
