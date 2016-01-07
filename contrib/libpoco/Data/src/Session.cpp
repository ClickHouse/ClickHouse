//
// Session.cpp
//
// $Id: //poco/Main/Data/src/Session.cpp#5 $
//
// Library: Data
// Package: DataCore
// Module:  Session
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/Session.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/String.h"
#include "Poco/URI.h"
#include <algorithm>


namespace Poco {
namespace Data {


Session::Session(Poco::AutoPtr<SessionImpl> pImpl):
	_pImpl(pImpl),
	_statementCreator(pImpl)
{
	poco_check_ptr (pImpl.get());
}


Session::Session(const std::string& connector,
	const std::string& connectionString,
	std::size_t timeout)
{
	Session newSession(SessionFactory::instance().create(connector, connectionString, timeout));
	swap(newSession);
}


Session::Session(const std::string& connection,
	std::size_t timeout)
{
	Session newSession(SessionFactory::instance().create(connection, timeout));
	swap(newSession);
}


Session::Session(const Session& other):	_pImpl(other._pImpl),
	_statementCreator(other._pImpl)
{
}


Session::~Session()
{
}


Session& Session::operator = (const Session& other)
{
	Session tmp(other);
	swap(tmp);
	return *this;
}


void Session::swap(Session& other)
{
	using std::swap;
	swap(_statementCreator, other._statementCreator);
	swap(_pImpl, other._pImpl);
}


} } // namespace Poco::Data
