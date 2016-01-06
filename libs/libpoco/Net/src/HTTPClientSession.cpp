//
// HTTPClientSession.cpp
//
// $Id: //poco/1.4/Net/src/HTTPClientSession.cpp#15 $
//
// Library: Net
// Package: HTTPClient
// Module:  HTTPClientSession
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPClientSession.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPHeaderStream.h"
#include "Poco/Net/HTTPStream.h"
#include "Poco/Net/HTTPFixedLengthStream.h"
#include "Poco/Net/HTTPChunkedStream.h"
#include "Poco/Net/HTTPBasicCredentials.h"
#include "Poco/Net/NetException.h"
#include "Poco/NumberFormatter.h"
#include "Poco/CountingStream.h"
#include "Poco/RegularExpression.h"
#include <sstream>


using Poco::NumberFormatter;
using Poco::IllegalStateException;


namespace Poco {
namespace Net {


HTTPClientSession::ProxyConfig HTTPClientSession::_globalProxyConfig;


HTTPClientSession::HTTPClientSession():
	_port(HTTPSession::HTTP_PORT),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false)
{
}


HTTPClientSession::HTTPClientSession(const StreamSocket& socket):
	HTTPSession(socket),
	_port(HTTPSession::HTTP_PORT),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false)
{
}


HTTPClientSession::HTTPClientSession(const SocketAddress& address):
	_host(address.host().toString()),
	_port(address.port()),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false)
{
}


HTTPClientSession::HTTPClientSession(const std::string& host, Poco::UInt16 port):
	_host(host),
	_port(port),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false)
{
}


HTTPClientSession::HTTPClientSession(const std::string& host, Poco::UInt16 port, const ProxyConfig& proxyConfig):
	_host(host),
	_port(port),
	_proxyConfig(proxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false)
{
}


HTTPClientSession::~HTTPClientSession()
{
}


void HTTPClientSession::setHost(const std::string& host)
{
	if (!connected())
		_host = host;
	else
		throw IllegalStateException("Cannot set the host for an already connected session");
}


void HTTPClientSession::setPort(Poco::UInt16 port)
{
	if (!connected())
		_port = port;
	else
		throw IllegalStateException("Cannot set the port number for an already connected session");
}


void HTTPClientSession::setProxy(const std::string& host, Poco::UInt16 port)
{
	if (!connected())
	{
		_proxyConfig.host = host;
		_proxyConfig.port = port;
	}
	else throw IllegalStateException("Cannot set the proxy host and port for an already connected session");
}


void HTTPClientSession::setProxyHost(const std::string& host)
{
	if (!connected())
		_proxyConfig.host = host;
	else
		throw IllegalStateException("Cannot set the proxy host for an already connected session");
}


void HTTPClientSession::setProxyPort(Poco::UInt16 port)
{
	if (!connected())
		_proxyConfig.port = port;
	else
		throw IllegalStateException("Cannot set the proxy port number for an already connected session");
}


void HTTPClientSession::setProxyCredentials(const std::string& username, const std::string& password)
{
	_proxyConfig.username = username;
	_proxyConfig.password = password;
}


void HTTPClientSession::setProxyUsername(const std::string& username)
{
	_proxyConfig.username = username;
}
	

void HTTPClientSession::setProxyPassword(const std::string& password)
{
	_proxyConfig.password = password;
}


void HTTPClientSession::setProxyConfig(const ProxyConfig& config)
{
	_proxyConfig = config;
}


void HTTPClientSession::setGlobalProxyConfig(const ProxyConfig& config)
{
	_globalProxyConfig = config;
}


void HTTPClientSession::setKeepAliveTimeout(const Poco::Timespan& timeout)
{
	_keepAliveTimeout = timeout;
}


std::ostream& HTTPClientSession::sendRequest(HTTPRequest& request)
{
	clearException();
	_pResponseStream = 0;

	bool keepAlive = getKeepAlive();
	if ((connected() && !keepAlive) || mustReconnect())
	{
		close();
		_mustReconnect = false;
	}
	try
	{
		if (!connected())
			reconnect();
		if (!keepAlive)
			request.setKeepAlive(false);
		if (!request.has(HTTPRequest::HOST))
			request.setHost(_host, _port);
		if (!_proxyConfig.host.empty() && !bypassProxy())
		{
			request.setURI(proxyRequestPrefix() + request.getURI());
			proxyAuthenticate(request);
		}
		_reconnect = keepAlive;
		_expectResponseBody = request.getMethod() != HTTPRequest::HTTP_HEAD;
		if (request.getChunkedTransferEncoding())
		{
			HTTPHeaderOutputStream hos(*this);
			request.write(hos);
			_pRequestStream = new HTTPChunkedOutputStream(*this);
		}
		else if (request.hasContentLength())
		{
			Poco::CountingOutputStream cs;
			request.write(cs);
#if POCO_HAVE_INT64
			_pRequestStream = new HTTPFixedLengthOutputStream(*this, request.getContentLength64() + cs.chars());
#else
			_pRequestStream = new HTTPFixedLengthOutputStream(*this, request.getContentLength() + cs.chars());
#endif
			request.write(*_pRequestStream);
		}
		else if ((request.getMethod() != HTTPRequest::HTTP_PUT && request.getMethod() != HTTPRequest::HTTP_POST) || request.has(HTTPRequest::UPGRADE))
		{
			Poco::CountingOutputStream cs;
			request.write(cs);
			_pRequestStream = new HTTPFixedLengthOutputStream(*this, cs.chars());
			request.write(*_pRequestStream);
		}
		else
		{
			_pRequestStream = new HTTPOutputStream(*this);
			request.write(*_pRequestStream);
		}	
		_lastRequest.update();
		return *_pRequestStream;
	}
	catch (Exception&)
	{
		close();
		throw;
	}
}


std::istream& HTTPClientSession::receiveResponse(HTTPResponse& response)
{
	_pRequestStream = 0;
	if (networkException()) networkException()->rethrow();

	do
	{
		response.clear();
		HTTPHeaderInputStream his(*this);
		try
		{
			response.read(his);
		}
		catch (Exception&)
		{
			close();
			if (networkException())
				networkException()->rethrow();
			else
				throw;
			throw;
		}
	}
	while (response.getStatus() == HTTPResponse::HTTP_CONTINUE);

	_mustReconnect = getKeepAlive() && !response.getKeepAlive();

	if (!_expectResponseBody || response.getStatus() < 200 || response.getStatus() == HTTPResponse::HTTP_NO_CONTENT || response.getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
		_pResponseStream = new HTTPFixedLengthInputStream(*this, 0);
	else if (response.getChunkedTransferEncoding())
		_pResponseStream = new HTTPChunkedInputStream(*this);
	else if (response.hasContentLength())
#if defined(POCO_HAVE_INT64)
		_pResponseStream = new HTTPFixedLengthInputStream(*this, response.getContentLength64());
#else
		_pResponseStream = new HTTPFixedLengthInputStream(*this, response.getContentLength());
#endif
	else
		_pResponseStream = new HTTPInputStream(*this);
		
	return *_pResponseStream;
}


void HTTPClientSession::reset()
{
	close();
}


bool HTTPClientSession::secure() const
{
	return false;
}


int HTTPClientSession::write(const char* buffer, std::streamsize length)
{
	try
	{
		int rc = HTTPSession::write(buffer, length);
		_reconnect = false;
		return rc;
	}
	catch (NetException&)
	{
		if (_reconnect)
		{
			close();
			reconnect();
			int rc = HTTPSession::write(buffer, length);
			_reconnect = false;
			return rc;
		}
		else throw;
	}
}


void HTTPClientSession::reconnect()
{
	if (_proxyConfig.host.empty() || bypassProxy())
	{
		SocketAddress addr(_host, _port);
		connect(addr);
	}
	else
	{
		SocketAddress addr(_proxyConfig.host, _proxyConfig.port);
		connect(addr);
	}
}


std::string HTTPClientSession::proxyRequestPrefix() const
{
	std::string result("http://");
	result.append(_host);
	result.append(":");
	NumberFormatter::append(result, _port);
	return result;
}


bool HTTPClientSession::mustReconnect() const
{
	if (!_mustReconnect)
	{
		Poco::Timestamp now;
		return _keepAliveTimeout <= now - _lastRequest;
	}
	else return true;
}


void HTTPClientSession::proxyAuthenticate(HTTPRequest& request)
{
	proxyAuthenticateImpl(request);
}


void HTTPClientSession::proxyAuthenticateImpl(HTTPRequest& request)
{
	if (!_proxyConfig.username.empty())
	{
		HTTPBasicCredentials creds(_proxyConfig.username, _proxyConfig.password);
		creds.proxyAuthenticate(request);
	}
}


StreamSocket HTTPClientSession::proxyConnect()
{
	ProxyConfig emptyProxyConfig;
	HTTPClientSession proxySession(getProxyHost(), getProxyPort(), emptyProxyConfig);
	proxySession.setTimeout(getTimeout());
	std::string targetAddress(_host);
	targetAddress.append(":");
	NumberFormatter::append(targetAddress, _port);
	HTTPRequest proxyRequest(HTTPRequest::HTTP_CONNECT, targetAddress, HTTPMessage::HTTP_1_1);
	HTTPResponse proxyResponse;
	proxyRequest.set("Proxy-Connection", "keep-alive");
	proxyRequest.set("Host", getHost());
	proxyAuthenticateImpl(proxyRequest);
	proxySession.setKeepAlive(true);
	proxySession.sendRequest(proxyRequest);
	proxySession.receiveResponse(proxyResponse);
	if (proxyResponse.getStatus() != HTTPResponse::HTTP_OK)
		throw HTTPException("Cannot establish proxy connection", proxyResponse.getReason());
	return proxySession.detachSocket();
}


void HTTPClientSession::proxyTunnel()
{
	StreamSocket ss = proxyConnect();
	attachSocket(ss);
}


bool HTTPClientSession::bypassProxy() const
{
	if (!_proxyConfig.nonProxyHosts.empty())
	{
		return RegularExpression::match(_host, _proxyConfig.nonProxyHosts, RegularExpression::RE_CASELESS | RegularExpression::RE_ANCHORED);
	}
	else return false;
}


} } // namespace Poco::Net
