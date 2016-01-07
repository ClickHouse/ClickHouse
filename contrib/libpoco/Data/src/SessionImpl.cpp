//
// SessionImpl.cpp
//
// $Id: //poco/Main/Data/src/SessionImpl.cpp#3 $
//
// Library: Data
// Package: DataCore
// Module:  SessionImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SessionImpl.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Data {


SessionImpl::SessionImpl(const std::string& connectionString, std::size_t timeout): 
	_connectionString(connectionString),
	_loginTimeout(timeout)
{
}


SessionImpl::~SessionImpl()
{
}


void SessionImpl::reconnect()
{
	close();
	open();
}


void SessionImpl::setConnectionString(const std::string& connectionString)
{
	if (isConnected())
		throw Poco::InvalidAccessException("Can not change connection string on connected session."
			" Close the session first.");

	_connectionString = connectionString;
}


} } // namespace Poco::Data
