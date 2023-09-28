//
// AbstractHTTPRequestHandler.cpp
//
// Library: Net
// Package: HTTPServer
// Module:  AbstractHTTPRequestHandler
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/AbstractHTTPRequestHandler.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Exception.h"


using Poco::NumberFormatter;


namespace Poco {
namespace Net {


AbstractHTTPRequestHandler::AbstractHTTPRequestHandler():
	_pRequest(0),
	_pResponse(0),
	_pForm(0)
{
}


AbstractHTTPRequestHandler::~AbstractHTTPRequestHandler()
{
	delete _pForm;
}


void AbstractHTTPRequestHandler::handleRequest(HTTPServerRequest& request, HTTPServerResponse& response)
{
	_pRequest  = &request;
	_pResponse = &response;
	if (authenticate())
	{
		try
		{
			run();
		}
		catch (Poco::Exception& exc)
		{
			if (!response.sent())
			{
				sendErrorResponse(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, exc.displayText());
			}
		}
		catch (std::exception& exc)
		{
			if (!response.sent())
			{
				sendErrorResponse(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, exc.what());
			}
		}
	}
	else
	{
		sendErrorResponse(HTTPResponse::HTTP_UNAUTHORIZED, "");
	}
}


bool AbstractHTTPRequestHandler::authenticate()
{
	return true;
}


HTMLForm& AbstractHTTPRequestHandler::form()
{
	if (!_pForm)
		_pForm = new HTMLForm(request(), request().stream());

	return *_pForm;
}


void AbstractHTTPRequestHandler::sendErrorResponse(HTTPResponse::HTTPStatus status, const std::string& message)
{
	response().setStatusAndReason(status);
	std::string statusAndReason(NumberFormatter::format(static_cast<int>(response().getStatus())));
	statusAndReason += " - ";
	statusAndReason += response().getReason();
	std::string page("<HTML><HEAD><TITLE>");
	page += statusAndReason;
	page += "</TITLE></HEAD><BODY><H1>";
	page += statusAndReason;
	page += "</H1>";
	page += "<P>";
	page += message;
	page += "</P></BODY></HTML>";
	response().sendBuffer(page.data(), page.size());
}


} } // namespace Poco::Net
