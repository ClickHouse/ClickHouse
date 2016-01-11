//
// Connector.cpp
//
// $Id: //poco/Main/Data/ODBC/src/Connector.cpp#2 $
//
// Library: ODBC
// Package: ODBC
// Module:  Connector
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/ODBC/Connector.h"
#include "Poco/Data/ODBC/SessionImpl.h"
#include "Poco/Data/SessionFactory.h"


const ODBCConnectorRegistrator pocoODBCConnectorRegistrator;


namespace Poco {
namespace Data {
namespace ODBC {


const std::string Connector::KEY(POCO_DATA_ODBC_CONNECTOR_NAME);


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
	Poco::Data::SessionFactory::instance().remove(POCO_DATA_ODBC_CONNECTOR_NAME);
}


} } } // namespace Poco::Data::ODBC
