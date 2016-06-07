//
// Connector.cpp
//
// $Id: //poco/Main/Data/SQLite/src/Connector.cpp#2 $
//
// Library: SQLite
// Package: SQLite
// Module:  Connector
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Data/SQLite/SessionImpl.h"
#include "Poco/Data/SessionFactory.h"
#if defined(POCO_UNBUNDLED)
#include <sqlite3.h>
#else
#include "sqlite3.h"
#endif


const SQLiteConnectorRegistrator pocoSQLiteConnectorRegistrator;


namespace Poco {
namespace Data {
namespace SQLite {


const std::string Connector::KEY(POCO_DATA_SQLITE_CONNECTOR_NAME);


Connector::Connector()
{
}


Connector::~Connector()
{
}


Poco::AutoPtr<Poco::Data::SessionImpl> Connector::createSession(const std::string& connectionString,
	std::size_t timeout)
{
	return Poco::AutoPtr<Poco::Data::SessionImpl>(new SessionImpl(connectionString, timeout));
}


void Connector::registerConnector()
{
	Poco::Data::SessionFactory::instance().add(new Connector());
}


void Connector::unregisterConnector()
{
	Poco::Data::SessionFactory::instance().remove(POCO_DATA_SQLITE_CONNECTOR_NAME);
}


void Connector::enableSharedCache(bool flag)
{
	sqlite3_enable_shared_cache(flag ? 1 : 0);
}


void Connector::enableSoftHeapLimit(int limit)
{
	sqlite3_soft_heap_limit(limit);
}


} } } // namespace Poco::Data::SQLite
