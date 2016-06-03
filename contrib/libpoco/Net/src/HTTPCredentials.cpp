//
// HTTPCredentials.cpp
//
// $Id: //poco/1.4/Net/src/HTTPCredentials.cpp#3 $
//
// Library: Net
// Package: HTTP
// Module:	HTTPCredentials
//
// Copyright (c) 2011, Anton V. Yabchinskiy (arn at bestmx dot ru).
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPAuthenticationParams.h"
#include "Poco/Net/HTTPBasicCredentials.h"
#include "Poco/Net/HTTPCredentials.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/NetException.h"
#include "Poco/String.h"
#include "Poco/Ascii.h"
#include "Poco/URI.h"


using Poco::icompare;


namespace Poco {
namespace Net {


HTTPCredentials::HTTPCredentials()
{
}


HTTPCredentials::HTTPCredentials(const std::string& username, const std::string& password):
	_digest(username, password)
{
}


HTTPCredentials::~HTTPCredentials()
{
}


void HTTPCredentials::fromUserInfo(const std::string& userInfo)
{
	std::string username;
	std::string password;

	extractCredentials(userInfo, username, password);
	setUsername(username);
	setPassword(password);
	_digest.reset();
}


void HTTPCredentials::fromURI(const URI& uri)
{
	std::string username;
	std::string password;

	extractCredentials(uri, username, password);
	setUsername(username);
	setPassword(password);
	_digest.reset();
}


void HTTPCredentials::authenticate(HTTPRequest& request, const HTTPResponse& response)
{
	for (HTTPResponse::ConstIterator iter = response.find(HTTPAuthenticationParams::WWW_AUTHENTICATE); iter != response.end(); ++iter)
	{
		if (isBasicCredentials(iter->second)) 
		{
			HTTPBasicCredentials(_digest.getUsername(), _digest.getPassword()).authenticate(request);
			return;
		} 
		else if (isDigestCredentials(iter->second)) 
		{
			_digest.authenticate(request, HTTPAuthenticationParams(iter->second.substr(7)));
			return;
		}
	}
}


void HTTPCredentials::updateAuthInfo(HTTPRequest& request)
{
	if (request.has(HTTPRequest::AUTHORIZATION)) 
	{
		const std::string& authorization = request.get(HTTPRequest::AUTHORIZATION);

		if (isBasicCredentials(authorization)) 
		{
			HTTPBasicCredentials(_digest.getUsername(), _digest.getPassword()).authenticate(request);
		} 
		else if (isDigestCredentials(authorization)) 
		{
			_digest.updateAuthInfo(request);
		}
	}
}


void HTTPCredentials::proxyAuthenticate(HTTPRequest& request, const HTTPResponse& response)
{
	for (HTTPResponse::ConstIterator iter = response.find(HTTPAuthenticationParams::PROXY_AUTHENTICATE); iter != response.end(); ++iter)
	{
		if (isBasicCredentials(iter->second)) 
		{
			HTTPBasicCredentials(_digest.getUsername(), _digest.getPassword()).proxyAuthenticate(request);
			return;
		} 
		else if (isDigestCredentials(iter->second)) 
		{
			_digest.proxyAuthenticate(request, HTTPAuthenticationParams(iter->second.substr(7)));
			return;
		}
	}
}


void HTTPCredentials::updateProxyAuthInfo(HTTPRequest& request)
{
	if (request.has(HTTPRequest::PROXY_AUTHORIZATION)) 
	{
		const std::string& authorization = request.get(HTTPRequest::PROXY_AUTHORIZATION);

		if (isBasicCredentials(authorization)) 
		{
			HTTPBasicCredentials(_digest.getUsername(), _digest.getPassword()).proxyAuthenticate(request);
		} 
		else if (isDigestCredentials(authorization)) 
		{
			_digest.updateProxyAuthInfo(request);
		}
	}
}


bool HTTPCredentials::isBasicCredentials(const std::string& header)
{
	return icompare(header, 0, 5, "Basic") == 0 && (header.size() > 5 ? Poco::Ascii::isSpace(header[5]) : true);
}


bool HTTPCredentials::isDigestCredentials(const std::string& header)
{
	return icompare(header, 0, 6, "Digest") == 0 && (header.size() > 6 ? Poco::Ascii::isSpace(header[6]) : true);
}


bool HTTPCredentials::hasBasicCredentials(const HTTPRequest& request)
{
	return request.has(HTTPRequest::AUTHORIZATION) && isBasicCredentials(request.get(HTTPRequest::AUTHORIZATION));
}


bool HTTPCredentials::hasDigestCredentials(const HTTPRequest& request)
{
	return request.has(HTTPRequest::AUTHORIZATION) && isDigestCredentials(request.get(HTTPRequest::AUTHORIZATION));
}


bool HTTPCredentials::hasProxyBasicCredentials(const HTTPRequest& request)
{
	return request.has(HTTPRequest::PROXY_AUTHORIZATION) && isBasicCredentials(request.get(HTTPRequest::PROXY_AUTHORIZATION));
}


bool HTTPCredentials::hasProxyDigestCredentials(const HTTPRequest& request)
{
	return request.has(HTTPRequest::PROXY_AUTHORIZATION) && isDigestCredentials(request.get(HTTPRequest::PROXY_AUTHORIZATION));
}


void HTTPCredentials::extractCredentials(const std::string& userInfo, std::string& username, std::string& password)
{
	const std::string::size_type p = userInfo.find(':');

	if (p != std::string::npos) 
	{
		username.assign(userInfo, 0, p);
		password.assign(userInfo, p + 1, std::string::npos);
	} 
	else 
	{
		username.assign(userInfo);
		password.clear();
	}
}


void HTTPCredentials::extractCredentials(const Poco::URI& uri, std::string& username, std::string& password)
{
	if (!uri.getUserInfo().empty()) 
	{
		extractCredentials(uri.getUserInfo(), username, password);
	}
}


} } // namespace Poco::Net
