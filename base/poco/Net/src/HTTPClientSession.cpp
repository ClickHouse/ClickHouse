//
// HTTPClientSession.cpp
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
#include "Poco/Net/HTTPSessionInstantiator.h"
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
const double HTTPClientSession::_defaultKeepAliveReliabilityLevel = 0.9;


HTTPClientSession::HTTPClientSession():
	_port(HTTPSession::HTTP_PORT),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false),
	_responseReceived(false)
{
	_proxySessionFactory.registerProtocol("http", new HTTPSessionInstantiator);
}


HTTPClientSession::HTTPClientSession(const StreamSocket& socket):
	HTTPSession(socket),
	_port(HTTPSession::HTTP_PORT),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false),
	_responseReceived(false)
{
	_proxySessionFactory.registerProtocol("http", new HTTPSessionInstantiator);
}


HTTPClientSession::HTTPClientSession(const SocketAddress& address):
	_host(address.host().toString()),
	_port(address.port()),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false),
	_responseReceived(false)
{
	_proxySessionFactory.registerProtocol("http", new HTTPSessionInstantiator);
}


HTTPClientSession::HTTPClientSession(const std::string& host, Poco::UInt16 port):
	_host(host),
	_port(port),
	_proxyConfig(_globalProxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false),
	_responseReceived(false)
{
	_proxySessionFactory.registerProtocol("http", new HTTPSessionInstantiator);
}


HTTPClientSession::HTTPClientSession(const std::string& host, Poco::UInt16 port, const ProxyConfig& proxyConfig):
	_host(host),
	_port(port),
	_proxyConfig(proxyConfig),
	_keepAliveTimeout(DEFAULT_KEEP_ALIVE_TIMEOUT, 0),
	_reconnect(false),
	_mustReconnect(false),
	_expectResponseBody(false),
	_responseReceived(false)
{
	_proxySessionFactory.registerProtocol("http", new HTTPSessionInstantiator);
}


HTTPClientSession::~HTTPClientSession()
{
	_proxySessionFactory.unregisterProtocol("http");
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


void HTTPClientSession::setProxy(const std::string& host, Poco::UInt16 port, const std::string& protocol, bool tunnel)
{
	if (protocol != "http" && protocol != "https")
		throw IllegalStateException("Protocol must be either http or https");

	if (!connected())
	{
		_proxyConfig.host = host;
		_proxyConfig.port = port;
		_proxyConfig.protocol = protocol;
		_proxyConfig.tunnel = tunnel;
	}
	else throw IllegalStateException("Cannot set the proxy host, port and protocol for an already connected session");
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


void HTTPClientSession::setProxyProtocol(const std::string& protocol)
{
	if (protocol != "http" && protocol != "https")
		throw IllegalStateException("Protocol must be either http or https");

	if (!connected())
		_proxyConfig.protocol = protocol;
	else
		throw IllegalStateException("Cannot set the proxy port number for an already connected session");
}


void HTTPClientSession::setProxyTunnel(bool tunnel)
{
	if (!connected())
		_proxyConfig.tunnel = tunnel;
	else
		throw IllegalStateException("Cannot set the proxy tunnel for an already connected session");
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
    if (connected())
    {
        throw Poco::IllegalStateException("cannot change keep alive timeout on initiated connection, "
                                          "That value is managed privately after connection is established.");
    }
    _keepAliveTimeout = timeout;
}


void HTTPClientSession::setKeepAliveMaxRequests(int max_requests)
{
    if (connected())
    {
        throw Poco::IllegalStateException("cannot change keep alive max requests on initiated connection, "
                                          "That value is managed privately after connection is established.");
    }
    _keepAliveMaxRequests = max_requests;
}


void HTTPClientSession::setKeepAliveRequest(int request)
{
    _keepAliveCurrentRequest = request;
}



void HTTPClientSession::setLastRequest(Poco::Timestamp time)
{
    if (connected())
    {
        throw Poco::IllegalStateException("cannot change last request on initiated connection, "
                                          "That value is managed privately after connection is established.");
    }
    _lastRequest = time;
}


std::ostream& HTTPClientSession::sendRequest(HTTPRequest& request)
{
	_pRequestStream = 0;
    _pResponseStream = 0;
	clearException();
	_responseReceived = false;

    _keepAliveCurrentRequest += 1;

	bool keepAlive = getKeepAlive();
	if (((connected() && !keepAlive) || mustReconnect()) && !_host.empty())
	{
		close();
		_mustReconnect = false;
	}
	try
	{
		if (!connected())
			reconnect();
        if (!request.has(HTTPMessage::CONNECTION))
            request.setKeepAlive(keepAlive);
        if (keepAlive && !request.has(HTTPMessage::CONNECTION_KEEP_ALIVE) && _keepAliveTimeout.totalSeconds() > 0)
            request.setKeepAliveTimeout(_keepAliveTimeout.totalSeconds(), _keepAliveMaxRequests);
		if (!request.has(HTTPRequest::HOST) && !_host.empty())
			request.setHost(_host, _port);
		if (!_proxyConfig.host.empty() && !bypassProxy())
		{
			request.setURI(proxyRequestPrefix() + request.getURI());
			proxyAuthenticate(request);
		}
		_reconnect = keepAlive;
		_expectResponseBody = request.getMethod() != HTTPRequest::HTTP_HEAD;
		const std::string& method = request.getMethod();
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
			_pRequestStream = new HTTPFixedLengthOutputStream(*this, request.getContentLength64() + cs.chars());
			request.write(*_pRequestStream);
		}
		else if ((method != HTTPRequest::HTTP_PUT && method != HTTPRequest::HTTP_POST && method != HTTPRequest::HTTP_PATCH) || request.has(HTTPRequest::UPGRADE))
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


void HTTPClientSession::flushRequest()
{
	_pRequestStream = 0;
	if (networkException()) networkException()->rethrow();
}


std::istream& HTTPClientSession::receiveResponse(HTTPResponse& response)
{
	flushRequest();
	if (!_responseReceived)
	{
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
	}

	_mustReconnect = getKeepAlive() && !response.getKeepAlive();

    if (!_mustReconnect)
    {
        /// when server sends its keep alive timeout, client has to follow that value
        auto timeout = response.getKeepAliveTimeout();
        if (timeout > 0)
            _keepAliveTimeout = std::min(_keepAliveTimeout, Poco::Timespan(timeout, 0));
        auto max_requests = response.getKeepAliveMaxRequests();
        if (max_requests > 0)
            _keepAliveMaxRequests = std::min(_keepAliveMaxRequests, max_requests);
    }

	if (!_expectResponseBody || response.getStatus() < 200 || response.getStatus() == HTTPResponse::HTTP_NO_CONTENT || response.getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
		_pResponseStream = new HTTPFixedLengthInputStream(*this, 0);
	else if (response.getChunkedTransferEncoding())
		_pResponseStream = new HTTPChunkedInputStream(*this);
	else if (response.hasContentLength())
		_pResponseStream = new HTTPFixedLengthInputStream(*this, response.getContentLength64());
	else
		_pResponseStream = new HTTPInputStream(*this);

	return *_pResponseStream;
}


bool HTTPClientSession::peekResponse(HTTPResponse& response)
{
	poco_assert (!_responseReceived);

	_pRequestStream->flush();

	if (networkException()) networkException()->rethrow();

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
	_responseReceived = response.getStatus() != HTTPResponse::HTTP_CONTINUE;
	return !_responseReceived;
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
	catch (Poco::Exception&)
	{
		if (_reconnect)
		{
			close();
			reconnect();
			int rc = HTTPSession::write(buffer, length);
			clearException();
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
		SocketAddress addr(_resolved_host.empty() ? _host : _resolved_host, _port);
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
	std::string result(_proxyConfig.originalRequestProtocol + "://");
	result.append(_host);
	/// Do not append default by default, since this may break some servers.
	/// One example of such server is GCS (Google Cloud Storage).
	if (_port != HTTPSession::HTTP_PORT)
	{
		result.append(":");
		NumberFormatter::append(result, _port);
	}
	return result;
}

bool HTTPClientSession::isKeepAliveExpired(double reliability) const
{
    Poco::Timestamp now;
    return Timespan(Timestamp::TimeDiff(reliability *_keepAliveTimeout.totalMicroseconds())) <= now - _lastRequest
            || _keepAliveCurrentRequest > _keepAliveMaxRequests;
}

bool HTTPClientSession::mustReconnect() const
{
	if (!_mustReconnect)
        return isKeepAliveExpired(_defaultKeepAliveReliabilityLevel);
    return true;
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
	URI proxyUri;
	proxyUri.setScheme(getProxyProtocol());
	proxyUri.setHost(getProxyHost());
	proxyUri.setPort(getProxyPort());

	SharedPtr<HTTPClientSession> proxySession (_proxySessionFactory.createClientSession(proxyUri));

	proxySession->setTimeout(getTimeout());
	std::string targetAddress(_host);
	targetAddress.append(":");
	NumberFormatter::append(targetAddress, _port);
	HTTPRequest proxyRequest(HTTPRequest::HTTP_CONNECT, targetAddress, HTTPMessage::HTTP_1_1);
	HTTPResponse proxyResponse;
	proxyRequest.set("Proxy-Connection", "keep-alive");
	proxyRequest.set("Host", targetAddress);
	proxyAuthenticateImpl(proxyRequest);
	proxySession->setKeepAlive(true);
	proxySession->sendRequest(proxyRequest);
	proxySession->receiveResponse(proxyResponse);
	if (proxyResponse.getStatus() != HTTPResponse::HTTP_OK)
		throw HTTPException("Cannot establish proxy connection", proxyResponse.getReason());
	return proxySession->detachSocket();
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

void HTTPClientSession::assign(Poco::Net::HTTPClientSession & session)
{
    poco_assert (this != &session);

    if (session.buffered())
        throw Poco::LogicException("assign a session with not empty buffered data");

    if (buffered())
        throw Poco::LogicException("assign to a session with not empty buffered data");

    poco_assert(!connected());

    setResolvedHost(session.getResolvedHost());
    setProxyConfig(session.getProxyConfig());

    setTimeout(session.getConnectionTimeout(), session.getSendTimeout(), session.getReceiveTimeout());
    setKeepAlive(session.getKeepAlive());

    setLastRequest(session.getLastRequest());
    setKeepAliveTimeout(session.getKeepAliveTimeout());

    _keepAliveMaxRequests = session._keepAliveMaxRequests;
    _keepAliveCurrentRequest = session._keepAliveCurrentRequest;

    attachSocket(session.detachSocket());

    session.reset();
}

} } // namespace Poco::Net
