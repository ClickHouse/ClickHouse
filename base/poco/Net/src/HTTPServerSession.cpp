//
// HTTPServerSession.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerSession
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPServerSession.h"


namespace Poco {
namespace Net {


HTTPServerSession::HTTPServerSession(const StreamSocket & socket, HTTPServerParams::Ptr pParams)
    : HTTPSession(socket, pParams->getKeepAlive())
    , _firstRequest(true)
    , _keepAliveTimeout(pParams->getKeepAliveTimeout())
    , _maxKeepAliveRequests(pParams->getMaxKeepAliveRequests())
{
	setTimeout(pParams->getTimeout());
}


HTTPServerSession::~HTTPServerSession()
{
}

void HTTPServerSession::setKeepAliveTimeout(Poco::Timespan keepAliveTimeout)
{
    _keepAliveTimeout = keepAliveTimeout;
}

void HTTPServerSession::setStopCallback(std::function<bool()> stopCallback)
{
    _stopCallback = std::move(stopCallback);
}

bool HTTPServerSession::waitForRequest(Poco::Timespan timeout)
{
    if (!_stopCallback)
        return socket().poll(timeout, Socket::SELECT_READ);

    // Poll in bounded slices so that a connection idle in keep-alive does not
    // block server shutdown for the whole timeout: the stop callback is checked
    // between slices and aborts the wait as soon as the server stops.
    static const Poco::Timespan slice(1, 0); // 1 second
    Poco::Timespan remaining = timeout;
    while (remaining > 0)
    {
        if (_stopCallback())
            return false;
        Poco::Timespan step = remaining < slice ? remaining : slice;
        if (socket().poll(step, Socket::SELECT_READ))
            return true;
        remaining -= step;
    }
    return false;
}


bool HTTPServerSession::hasMoreRequests()
{
	if (!socket().impl()->initialized()) return false;

	if (_firstRequest)
	{
		_firstRequest = false;
		--_maxKeepAliveRequests;
		return waitForRequest(getTimeout());
	}
	else if (canKeepAlive())
	{
        if (_maxKeepAliveRequests > 0)
            --_maxKeepAliveRequests;
        return buffered() > 0 || waitForRequest(_keepAliveTimeout);
    }
	else
		return false;
}


SocketAddress HTTPServerSession::clientAddress()
{
	return socket().peerAddress();
}


SocketAddress HTTPServerSession::serverAddress()
{
	return socket().address();
}


} } // namespace Poco::Net
