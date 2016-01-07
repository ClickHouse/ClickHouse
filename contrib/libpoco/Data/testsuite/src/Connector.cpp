//
// Connector.cpp
//
// $Id: //poco/Main/Data/testsuite/src/Connector.cpp#2 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Connector.h"
#include "SessionImpl.h"
#include "Poco/Data/SessionFactory.h"


namespace Poco {
namespace Data {
namespace Test {


const std::string Connector::KEY("test");


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


void Connector::addToFactory()
{
	Poco::Data::SessionFactory::instance().add(new Connector());
}


void Connector::removeFromFactory()
{
	Poco::Data::SessionFactory::instance().remove(KEY);
}


} } } // namespace Poco::Data::Test
