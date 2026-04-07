//
// HTTPRequest.cpp
//
// Library: Net
// Package: HTTP
// Module:  HTTPRequest
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/NameValueCollection.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Ascii.h"
#include "Poco/String.h"


using Poco::NumberFormatter;


namespace Poco {
namespace Net {


const std::string HTTPRequest::HTTP_GET            = "GET";
const std::string HTTPRequest::HTTP_HEAD           = "HEAD";
const std::string HTTPRequest::HTTP_PUT            = "PUT";
const std::string HTTPRequest::HTTP_POST           = "POST";
const std::string HTTPRequest::HTTP_OPTIONS        = "OPTIONS";
const std::string HTTPRequest::HTTP_DELETE         = "DELETE";
const std::string HTTPRequest::HTTP_TRACE          = "TRACE";
const std::string HTTPRequest::HTTP_CONNECT        = "CONNECT";
const std::string HTTPRequest::HTTP_PATCH          = "PATCH";
const std::string HTTPRequest::HOST                = "Host";
const std::string HTTPRequest::COOKIE              = "Cookie";
const std::string HTTPRequest::AUTHORIZATION       = "Authorization";
const std::string HTTPRequest::PROXY_AUTHORIZATION = "Proxy-Authorization";
const std::string HTTPRequest::UPGRADE             = "Upgrade";
const std::string HTTPRequest::EXPECT              = "Expect";


HTTPRequest::HTTPRequest():
	_method(HTTP_GET),
	_uri("/")
{
}

	
HTTPRequest::HTTPRequest(const std::string& version):
	HTTPMessage(version),
	_method(HTTP_GET),
	_uri("/")
{
}

	
HTTPRequest::HTTPRequest(const std::string& method, const std::string& uri):
	_method(method),
	_uri(uri)
{
}


HTTPRequest::HTTPRequest(const std::string& method, const std::string& uri, const std::string& version):
	HTTPMessage(version),
	_method(method),
	_uri(uri)
{
}


HTTPRequest::~HTTPRequest()
{
}


void HTTPRequest::setMethod(const std::string& method)
{
	_method = method;
}


void HTTPRequest::setURI(const std::string& uri)
{
	_uri = uri;
}


void HTTPRequest::setHost(const std::string& host)
{
	set(HOST, host);
}

	
void HTTPRequest::setHost(const std::string& host, Poco::UInt16 port)
{
	std::string value;
	if (host.find(':') != std::string::npos)
	{
		// IPv6 address
		value.append("[");
		value.append(host);
		value.append("]");
	}
	else
	{
		value.append(host);
	}    

	if (port != 80 && port != 443)
	{
		value.append(":");
		NumberFormatter::append(value, port);
	}
	setHost(value);
}

	
const std::string& HTTPRequest::getHost() const
{
	return get(HOST);
}


void HTTPRequest::setCookies(const NameValueCollection& cookies)
{
	std::string cookie;
	cookie.reserve(64);
	for (NameValueCollection::ConstIterator it = cookies.begin(); it != cookies.end(); ++it)
	{
		if (it != cookies.begin())
			cookie.append("; ");
		cookie.append(it->first);
		cookie.append("=");
		cookie.append(it->second);
	}
	add(COOKIE, cookie);
}

	
void HTTPRequest::getCookies(NameValueCollection& cookies) const
{
	NameValueCollection::ConstIterator it = find(COOKIE);
	while (it != end() && Poco::icompare(it->first, COOKIE) == 0)
	{
		splitParameters(it->second.begin(), it->second.end(), cookies);
		++it;
	}
}


bool HTTPRequest::hasCredentials() const
{
	return has(AUTHORIZATION);
}

	
void HTTPRequest::getCredentials(std::string& scheme, std::string& authInfo) const
{
	getCredentials(AUTHORIZATION, scheme, authInfo);
}

	
void HTTPRequest::setCredentials(const std::string& scheme, const std::string& authInfo)
{
	setCredentials(AUTHORIZATION, scheme, authInfo);
}


bool HTTPRequest::hasProxyCredentials() const
{
	return has(PROXY_AUTHORIZATION);
}

	
void HTTPRequest::getProxyCredentials(std::string& scheme, std::string& authInfo) const
{
	getCredentials(PROXY_AUTHORIZATION, scheme, authInfo);
}

	
void HTTPRequest::setProxyCredentials(const std::string& scheme, const std::string& authInfo)
{
	setCredentials(PROXY_AUTHORIZATION, scheme, authInfo);
}


void HTTPRequest::write(std::ostream& ostr) const
{
	ostr << _method << " " << _uri << " " << getVersion() << "\r\n";
	HTTPMessage::write(ostr);
	ostr << "\r\n";
}


void HTTPRequest::read(std::istream& istr)
{
	static const int eof = std::char_traits<char>::eof();

	std::string method;
	std::string uri;
	std::string version;
	method.reserve(16);
	uri.reserve(64);
	version.reserve(16);
	int ch = istr.get();
	if (istr.bad()) throw NetException("Error reading HTTP request header");
	if (ch == eof) throw NoMessageException();
	while (Poco::Ascii::isSpace(ch)) ch = istr.get();
	if (ch == eof) throw MessageException("No HTTP request header");
	while (!Poco::Ascii::isSpace(ch) && ch != eof && method.length() < MAX_METHOD_LENGTH) { method += (char) ch; ch = istr.get(); }
	if (!Poco::Ascii::isSpace(ch)) throw MessageException("HTTP request method invalid or too long");
	while (Poco::Ascii::isSpace(ch)) ch = istr.get();
	while (!Poco::Ascii::isSpace(ch) && ch != eof && uri.length() < MAX_URI_LENGTH) { uri += (char) ch; ch = istr.get(); }
	if (!Poco::Ascii::isSpace(ch)) throw MessageException("HTTP request URI invalid or too long");
	while (Poco::Ascii::isSpace(ch)) ch = istr.get();
	while (!Poco::Ascii::isSpace(ch) && ch != eof && version.length() < MAX_VERSION_LENGTH) { version += (char) ch; ch = istr.get(); }
	if (!Poco::Ascii::isSpace(ch)) throw MessageException("Invalid HTTP version string");
	while (ch != '\n' && ch != eof) { ch = istr.get(); }
	HTTPMessage::read(istr);
	ch = istr.get();
	while (ch != '\n' && ch != eof) { ch = istr.get(); }
	setMethod(method);
	setURI(uri);
	setVersion(version);
}


void HTTPRequest::getCredentials(const std::string& header, std::string& scheme, std::string& authInfo) const
{
	scheme.clear();
	authInfo.clear();
	if (has(header))
	{
		const std::string& auth = get(header);
		std::string::const_iterator it  = auth.begin();
		std::string::const_iterator end = auth.end();
		while (it != end && Poco::Ascii::isSpace(*it)) ++it;
		while (it != end && !Poco::Ascii::isSpace(*it)) scheme += *it++;
		while (it != end && Poco::Ascii::isSpace(*it)) ++it;
		while (it != end) authInfo += *it++;
	}
	else throw NotAuthenticatedException();
}

	
void HTTPRequest::setCredentials(const std::string& header, const std::string& scheme, const std::string& authInfo)
{
	std::string auth(scheme);
	auth.append(" ");
	auth.append(authInfo);
	set(header, auth);
}


bool HTTPRequest::getExpectContinue() const
{
	const std::string& expect = get(EXPECT, EMPTY);
	return !expect.empty() && icompare(expect, "100-continue") == 0;
}


void HTTPRequest::setExpectContinue(bool expectContinue)
{
	if (expectContinue)
		set(EXPECT, "100-continue");
	else
		erase(EXPECT);
}


} } // namespace Poco::Net
