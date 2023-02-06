//
// ApacheServerRequest.cpp
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ApacheServerRequest.h"
#include "ApacheServerResponse.h"
#include "ApacheRequestHandlerFactory.h"
#include "Poco/Exception.h"
#include <set>


ApacheServerRequest::ApacheServerRequest(
	ApacheRequestRec* pApacheRequest, 
	const char* serverName, 
	int serverPort, 
	const char* clientName, 
	int clientPort):
	_pApacheRequest(pApacheRequest),
    _pResponse(0),
    _pStream(new ApacheInputStream(_pApacheRequest)),
	_serverAddress(serverName, serverPort),
	_clientAddress(clientName, clientPort)
{
}


ApacheServerRequest::~ApacheServerRequest()
{
	delete _pStream;
}

	
const Poco::Net::HTTPServerParams& ApacheServerRequest::serverParams() const
{
	throw Poco::NotImplementedException("No HTTPServerParams available in Apache modules.");
}


Poco::Net::HTTPServerResponse& ApacheServerRequest::response() const
{
	return *_pResponse;
}


void ApacheServerRequest::setResponse(ApacheServerResponse* pResponse)
{
	_pResponse = pResponse;
}


bool ApacheServerRequest::expectContinue() const
{
	return false;
}
