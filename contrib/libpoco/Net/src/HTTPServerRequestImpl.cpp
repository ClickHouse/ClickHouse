//
// HTTPServerRequestImpl.cpp
//
// $Id: //poco/1.4/Net/src/HTTPServerRequestImpl.cpp#1 $
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerRequestImpl
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/HTTPServerRequestImpl.h"
#include "Poco/Net/HTTPServerResponseImpl.h"
#include "Poco/Net/HTTPServerSession.h"
#include "Poco/Net/HTTPHeaderStream.h"
#include "Poco/Net/HTTPStream.h"
#include "Poco/Net/HTTPFixedLengthStream.h"
#include "Poco/Net/HTTPChunkedStream.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/String.h"


using Poco::icompare;


namespace Poco {
namespace Net {


const std::string HTTPServerRequestImpl::EXPECT("Expect");


HTTPServerRequestImpl::HTTPServerRequestImpl(HTTPServerResponseImpl& response, HTTPServerSession& session, HTTPServerParams* pParams):
	_response(response),
	_session(session),
	_pStream(0),
	_pParams(pParams, true)
{
	response.attachRequest(this);

	HTTPHeaderInputStream hs(session);
	read(hs);
	
	// Now that we know socket is still connected, obtain addresses
	_clientAddress = session.clientAddress();
	_serverAddress = session.serverAddress();
	
	if (getChunkedTransferEncoding())
		_pStream = new HTTPChunkedInputStream(session);
	else if (hasContentLength())
#if defined(POCO_HAVE_INT64)
		_pStream = new HTTPFixedLengthInputStream(session, getContentLength64());
#else
		_pStream = new HTTPFixedLengthInputStream(session, getContentLength());
#endif
	else if (getMethod() == HTTPRequest::HTTP_GET || getMethod() == HTTPRequest::HTTP_HEAD)
		_pStream = new HTTPFixedLengthInputStream(session, 0);
	else
		_pStream = new HTTPInputStream(session);
}


HTTPServerRequestImpl::~HTTPServerRequestImpl()
{
	delete _pStream;
}


StreamSocket& HTTPServerRequestImpl::socket()
{
	return _session.socket();
}


StreamSocket HTTPServerRequestImpl::detachSocket()
{
	return _session.detachSocket();
}


bool HTTPServerRequestImpl::expectContinue() const
{
	const std::string& expect = get(EXPECT, EMPTY);
	return !expect.empty() && icompare(expect, "100-continue") == 0;
}


} } // namespace Poco::Net
