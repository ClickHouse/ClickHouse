//
// AbstractHTTPRequestHandler.h
//
// Library: Net
// Package: HTTPServer
// Module:  AbstractHTTPRequestHandler
//
// Definition of the AbstractHTTPRequestHandler class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_AbstractHTTPRequestHandler_INCLUDED
#define Net_AbstractHTTPRequestHandler_INCLUDED


#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPResponse.h"


namespace Poco {
namespace Net {


class HTMLForm;


class Net_API AbstractHTTPRequestHandler: public HTTPRequestHandler
	/// The abstract base class for AbstractHTTPRequestHandlers 
	/// created by HTTPServer.
	///
	/// Derived classes must override the run() method.

	/// Contrary to a HTTPRequestHandler, an AbstractHTTPRequestHandler
	/// stores request and response as member variables to avoid having
	/// to pass them around as method parameters. Additionally, a
	/// HTMLForm object is created for use by subclasses.
	///
	/// The run() method must perform the complete handling
	/// of the HTTP request connection. As soon as the run() 
	/// method returns, the request handler object is destroyed.
	///
	/// A new AbstractHTTPRequestHandler object will be created for
	/// each new HTTP request that is received by the HTTPServer.
{
public:
	AbstractHTTPRequestHandler();
		/// Creates the AbstractHTTPRequestHandler.

	virtual ~AbstractHTTPRequestHandler();
		/// Destroys the AbstractHTTPRequestHandler.

	void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response);
		/// This class implements some common behavior,
		/// before calling run() to actually handle the request:
		///   - save request and response objects;
		///   - call authorize();
		///   - if authorize() returns true call run(),
		///     else send 401 (Unauthorized) response.
		///
		/// If run() throws an exception and the response has not been
		/// sent yet, sends a 500 (Internal Server Error) response with
		/// the exception's display text.

	HTTPServerRequest& request();
		/// Returns the request.

	HTTPServerResponse& response();
		/// Returns the response.

	HTMLForm& form();
		/// Returns a HTMLForm for the given request.
		/// The HTMLForm object is created when this
		/// member function is executed the first time.

	void sendErrorResponse(HTTPResponse::HTTPStatus status, const std::string& message);
		/// Sends a HTML error page for the given status code.
		/// The given message is added to the page:
		///     <HTML>
		///         <HEAD>
		///             <TITLE>status - reason</TITLE>
		///         </HEAD>
		///         <BODY>
		///            <H1>status - reason</H1>
		///            <P>message</P>
		///         </BODY>
		///     </HTML>
	
protected:
	virtual void run() = 0;
		/// Must be overridden by subclasses.
		///
		/// Handles the given request.

	virtual bool authenticate();
		/// Check authentication; returns true if okay, false if failed to authenticate.
		/// The default implementation always returns true.
		///
		/// Subclasses can override this member function to perform
		/// some form of client or request authentication before
		/// the request is actually handled.

private:
	HTTPServerRequest*  _pRequest;
	HTTPServerResponse* _pResponse;
	HTMLForm*           _pForm;
};


//
// inlines
//
inline HTTPServerRequest& AbstractHTTPRequestHandler::request()
{
	poco_check_ptr (_pRequest);
	
	return *_pRequest;
}


inline HTTPServerResponse& AbstractHTTPRequestHandler::response()
{
	poco_check_ptr (_pResponse);

	return *_pResponse;
}


} } // namespace Poco::Net


#endif // Net_AbstractHTTPRequestHandler_INCLUDED
