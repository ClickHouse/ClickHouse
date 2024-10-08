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



bool HTTPServerSession::hasMoreRequests()
{
	if (!socket().impl()->initialized()) return false;

	if (_firstRequest)
	{
		_firstRequest = false;
		--_maxKeepAliveRequests;
		return socket().poll(getTimeout(), Socket::SELECT_READ);
	}
	else if (_maxKeepAliveRequests != 0 && getKeepAlive())
	{
        if (_maxKeepAliveRequests > 0)
            --_maxKeepAliveRequests;
        return buffered() > 0 || socket().poll(_keepAliveTimeout, Socket::SELECT_READ);
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
