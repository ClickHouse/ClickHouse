//
// MySQLException.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/Connector.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  Connector
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/Connector.h"
#include "Poco/Data/MySQL/SessionImpl.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/Exception.h"
#include <mysql.h>


const MySQLConnectorRegistrator pocoMySQLConnectorRegistrator;


namespace Poco {
namespace Data {
namespace MySQL {


std::string Connector::KEY(POCO_DATA_MYSQL_CONNECTOR_NAME);


Connector::Connector()
{
}


Connector::~Connector()
{
}

const std::string& Connector::name() const
{
	static const std::string n(POCO_DATA_MYSQL_CONNECTOR_NAME);
	return n;
}

Poco::AutoPtr<Poco::Data::SessionImpl> Connector::createSession(const std::string& connectionString,
	std::size_t timeout)
{
	return Poco::AutoPtr<Poco::Data::SessionImpl>(new SessionImpl(connectionString, timeout));	
}


void Connector::registerConnector()
{
	if (mysql_library_init(0, 0, 0) != 0)
	{
		throw Exception("mysql_library_init error");
	}

	Poco::Data::SessionFactory::instance().add(new Connector());
}


void Connector::unregisterConnector()
{
	Poco::Data::SessionFactory::instance().remove(POCO_DATA_MYSQL_CONNECTOR_NAME);
	mysql_library_end();
}


} } } // namespace Poco::Data::MySQL

