//
// SessionImpl.cpp
//
// $Id: //poco/Main/Data/testsuite/src/SessionImpl.cpp#4 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "SessionImpl.h"
#include "TestStatementImpl.h"
#include "Connector.h"


namespace Poco {
namespace Data {
namespace Test {


SessionImpl::SessionImpl(const std::string& init, std::size_t timeout):
	Poco::Data::AbstractSessionImpl<SessionImpl>(init, timeout),
	_f(false),
	_connected(true)
{
	addFeature("f1", &SessionImpl::setF, &SessionImpl::getF);
	addFeature("f2", 0, &SessionImpl::getF);
	addFeature("f3", &SessionImpl::setF, 0);
	addFeature("connected", &SessionImpl::setConnected, &SessionImpl::getConnected);
	addProperty("p1", &SessionImpl::setP, &SessionImpl::getP);
	addProperty("p2", 0, &SessionImpl::getP);
	addProperty("p3", &SessionImpl::setP, &SessionImpl::getP);
}


SessionImpl::~SessionImpl()
{
}


void SessionImpl::open(const std::string& connectionString)
{
	_connected = true;
}


void SessionImpl::close()
{
	_connected = false;
}


bool SessionImpl::isConnected()
{
	return _connected;
}


void SessionImpl::setConnectionTimeout(std::size_t timeout)
{
}


std::size_t SessionImpl::getConnectionTimeout()
{
	return 0;
}


Poco::Data::StatementImpl* SessionImpl::createStatementImpl()
{
	return new TestStatementImpl(*this);
}


void SessionImpl::begin()
{
}


void SessionImpl::commit()
{
}


void SessionImpl::rollback()
{
}


bool SessionImpl::canTransact()
{
	return false;
}


bool SessionImpl::isTransaction()
{
	return false;
}


void SessionImpl::setTransactionIsolation(Poco::UInt32)
{
}


Poco::UInt32 SessionImpl::getTransactionIsolation()
{
	return 0;
}


bool SessionImpl::hasTransactionIsolation(Poco::UInt32)
{
	return false;
}


bool SessionImpl::isTransactionIsolation(Poco::UInt32)
{
	return false;
}


const std::string& SessionImpl::connectorName() const
{
	return Connector::KEY;
}


bool SessionImpl::getConnected(const std::string& name)
{
	return _connected;
}


void SessionImpl::setConnected(const std::string& name, bool value)
{
	_connected = value;
}


void SessionImpl::setF(const std::string& name, bool value)
{
	_f = value;
}


bool SessionImpl::getF(const std::string& name)
{
	return _f;
}


void SessionImpl::setP(const std::string& name, const Poco::Any& value)
{
	_p = value;
}


Poco::Any SessionImpl::getP(const std::string& name)
{
	return _p;
}


} } } // namespace Poco::Data::Test
