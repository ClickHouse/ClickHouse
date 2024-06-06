//
// HTTPSessionInstantiator.cpp
//
// Library: Net
// Package: HTTPClient
// Module:  HTTPSessionInstantiator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPSessionInstantiator.h"
#include "Poco/Net/HTTPSessionFactory.h"
#include "Poco/Net/HTTPClientSession.h"


using Poco::URI;


namespace Poco {
namespace Net {


HTTPSessionInstantiator::HTTPSessionInstantiator():
	_proxyPort(0)
{
}


HTTPSessionInstantiator::~HTTPSessionInstantiator()
{
}


HTTPClientSession* HTTPSessionInstantiator::createClientSession(const Poco::URI& uri)
{
	poco_assert (uri.getScheme() == "http");
	HTTPClientSession* pSession = new HTTPClientSession(uri.getHost(), uri.getPort());
	if (!proxyHost().empty())
	{
		pSession->setProxy(proxyHost(), proxyPort());
		pSession->setProxyCredentials(proxyUsername(), proxyPassword());
	}
	return pSession;
}


void HTTPSessionInstantiator::registerInstantiator()
{
	HTTPSessionFactory::defaultFactory().registerProtocol("http", new HTTPSessionInstantiator);
}


void HTTPSessionInstantiator::unregisterInstantiator()
{
	HTTPSessionFactory::defaultFactory().unregisterProtocol("http");
}


void HTTPSessionInstantiator::setProxy(const std::string& host, Poco::UInt16 port)
{
	_proxyHost = host;
	_proxyPort = port;
}


void HTTPSessionInstantiator::setProxyCredentials(const std::string& username, const std::string& password)
{
	_proxyUsername = username;
	_proxyPassword = password;
}


} } // namespace Poco::Net
