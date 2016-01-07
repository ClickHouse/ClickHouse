//
// SessionFactory.cpp
//
// $Id: //poco/Main/Data/src/SessionFactory.cpp#6 $
//
// Library: Data
// Package: DataCore
// Module:  SessionFactory
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SessionFactory.h"
#include "Poco/URI.h"
#include "Poco/String.h"


namespace Poco {
namespace Data {


SessionFactory::SessionFactory()
{
}


SessionFactory::~SessionFactory()
{
}


SessionFactory& SessionFactory::instance()
{
	static SessionFactory sf;
	return sf;
}


void SessionFactory::add(Connector* pIn)
{
	Poco::FastMutex::ScopedLock lock(_mutex);
	SessionInfo info(pIn);
	std::pair<Connectors::iterator, bool> res =
		_connectors.insert(std::make_pair(pIn->name(), info));
	if (!res.second) res.first->second.cnt++;
}


void SessionFactory::remove(const std::string& key)
{
	Poco::FastMutex::ScopedLock lock(_mutex);
	Connectors::iterator it = _connectors.find(key);
	poco_assert (_connectors.end() != it);

	--(it->second.cnt);
	if (it->second.cnt == 0) _connectors.erase(it);
}


Session SessionFactory::create(const std::string& key,
	const std::string& connectionString,
	std::size_t timeout)
{
	Poco::FastMutex::ScopedLock lock(_mutex);
	Connectors::iterator it = _connectors.find(key);
	poco_assert (_connectors.end() != it);

	return Session(it->second.ptrSI->createSession(connectionString, timeout));
}


Session SessionFactory::create(const std::string& uri,
	std::size_t timeout)
{
	URI u(uri);
	poco_assert (!u.getPath().empty());
	return create(u.getScheme(), u.getPath().substr(1), timeout);
}


SessionFactory::SessionInfo::SessionInfo(Connector* pSI): 
	cnt(1), 
	ptrSI(pSI)
{
}


} } // namespace Poco::Data
