//
// OAuth20Credentials.cpp
//
// Library: Net
// Package: OAuth
// Module:	OAuth20Credentials
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/OAuth20Credentials.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/NetException.h"
#include "Poco/String.h"


namespace Poco {
namespace Net {


const std::string OAuth20Credentials::SCHEME = "Bearer";


OAuth20Credentials::OAuth20Credentials():
	_scheme(SCHEME)
{
}


OAuth20Credentials::OAuth20Credentials(const std::string& bearerToken):
	_bearerToken(bearerToken),
	_scheme(SCHEME)
{
}


OAuth20Credentials::OAuth20Credentials(const std::string& bearerToken, const std::string& scheme):
	_bearerToken(bearerToken),
	_scheme(scheme)
{
}


OAuth20Credentials::OAuth20Credentials(const HTTPRequest& request):
	_scheme(SCHEME)
{
	extractBearerToken(request);
}


OAuth20Credentials::OAuth20Credentials(const HTTPRequest& request, const std::string& scheme):
	_scheme(scheme)
{
	extractBearerToken(request);
}


OAuth20Credentials::~OAuth20Credentials()
{
}


void OAuth20Credentials::setBearerToken(const std::string& bearerToken)
{
	_bearerToken = bearerToken;
}


void OAuth20Credentials::setScheme(const std::string& scheme)
{
	_scheme = scheme;
}

	
void OAuth20Credentials::authenticate(HTTPRequest& request)
{
	std::string auth(_scheme);
	auth += ' ';
	auth += _bearerToken;
	request.set(HTTPRequest::AUTHORIZATION, auth);
}


void OAuth20Credentials::extractBearerToken(const HTTPRequest& request)
{
	if (request.hasCredentials())
	{
		std::string authScheme;
		std::string authInfo;
		request.getCredentials(authScheme, authInfo);
		if (icompare(authScheme, _scheme) == 0)
		{
			_bearerToken = authInfo;
		}
		else throw NotAuthenticatedException("No bearer token in Authorization header", authScheme);
	}
	else throw NotAuthenticatedException("No Authorization header found");
}


} } // namespace Poco::Net
