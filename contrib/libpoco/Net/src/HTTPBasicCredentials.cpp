//
// HTTPBasicCredentials.cpp
//
// $Id: //poco/1.4/Net/src/HTTPBasicCredentials.cpp#3 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPBasicCredentials
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPBasicCredentials.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/NetException.h"
#include "Poco/Base64Encoder.h"
#include "Poco/Base64Decoder.h"
#include "Poco/String.h"
#include <sstream>


using Poco::Base64Decoder;
using Poco::Base64Encoder;
using Poco::icompare;


namespace Poco {
namespace Net {


const std::string HTTPBasicCredentials::SCHEME = "Basic";


HTTPBasicCredentials::HTTPBasicCredentials()
{
}

	
HTTPBasicCredentials::HTTPBasicCredentials(const std::string& username, const std::string& password):
	_username(username),
	_password(password)
{
}


HTTPBasicCredentials::HTTPBasicCredentials(const HTTPRequest& request)
{
	std::string scheme;
	std::string authInfo;
	request.getCredentials(scheme, authInfo);
	if (icompare(scheme, SCHEME) == 0)
	{
		parseAuthInfo(authInfo);
	}
	else throw NotAuthenticatedException("Basic authentication expected");
}


HTTPBasicCredentials::HTTPBasicCredentials(const std::string& authInfo)
{
	parseAuthInfo(authInfo);
}


HTTPBasicCredentials::~HTTPBasicCredentials()
{
}


void HTTPBasicCredentials::setUsername(const std::string& username)
{
	_username = username;
}
	
	
void HTTPBasicCredentials::setPassword(const std::string& password)
{
	_password = password;
}
	
	
void HTTPBasicCredentials::authenticate(HTTPRequest& request) const
{
	std::ostringstream ostr;
	Base64Encoder encoder(ostr);
	encoder.rdbuf()->setLineLength(0);
	encoder << _username << ":" << _password;
	encoder.close();
	request.setCredentials(SCHEME, ostr.str());
}


void HTTPBasicCredentials::proxyAuthenticate(HTTPRequest& request) const
{
	std::ostringstream ostr;
	Base64Encoder encoder(ostr);
	encoder.rdbuf()->setLineLength(0);
	encoder << _username << ":" << _password;
	encoder.close();
	request.setProxyCredentials(SCHEME, ostr.str());
}


void HTTPBasicCredentials::parseAuthInfo(const std::string& authInfo)
{
	static const int eof = std::char_traits<char>::eof();

	std::istringstream istr(authInfo);
	Base64Decoder decoder(istr);
	int ch = decoder.get();
	while (ch != eof && ch != ':')
	{
		_username += (char) ch;
		ch = decoder.get();
	}
	if (ch == ':') ch = decoder.get();
	while (ch != eof)
	{
		_password += (char) ch;
		ch = decoder.get();
	}
}


} } // namespace Poco::Net
