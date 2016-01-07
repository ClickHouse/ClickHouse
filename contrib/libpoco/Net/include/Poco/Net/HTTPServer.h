//
// HTTPServer.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPServer.h#1 $
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServer
//
// Definition of the HTTPServer class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPServer_INCLUDED
#define Net_HTTPServer_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/TCPServer.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPServerParams.h"


namespace Poco {
namespace Net {


class Net_API HTTPServer: public TCPServer
	/// A subclass of TCPServer that implements a
	/// full-featured multithreaded HTTP server.
	///
	/// A HTTPRequestHandlerFactory must be supplied.
	/// The ServerSocket must be bound and in listening state.
	///
	/// To configure various aspects of the server, a HTTPServerParams
	/// object can be passed to the constructor.
	///
	/// The server supports:
	///   - HTTP/1.0 and HTTP/1.1
	///   - automatic handling of persistent connections.
	///   - automatic decoding/encoding of request/response message bodies
	///     using chunked transfer encoding.
	///
	/// Please see the TCPServer class for information about
	/// connection and thread handling.
	///
	/// See RFC 2616 <http://www.faqs.org/rfcs/rfc2616.html> for more
	/// information about the HTTP protocol.
{
public:
	HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, Poco::UInt16 portNumber = 80, HTTPServerParams::Ptr pParams = new HTTPServerParams);
		/// Creates HTTPServer listening on the given port (default 80).
		///
		/// The server takes ownership of the HTTPRequstHandlerFactory
		/// and deletes it when it's no longer needed.
		///
		/// New threads are taken from the default thread pool.

	HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, const ServerSocket& socket, HTTPServerParams::Ptr pParams);
		/// Creates the HTTPServer, using the given ServerSocket.
		///
		/// The server takes ownership of the HTTPRequstHandlerFactory
		/// and deletes it when it's no longer needed.
		///
		/// The server also takes ownership of the HTTPServerParams object.
		///
		/// New threads are taken from the default thread pool.

	HTTPServer(HTTPRequestHandlerFactory::Ptr pFactory, Poco::ThreadPool& threadPool, const ServerSocket& socket, HTTPServerParams::Ptr pParams);
		/// Creates the HTTPServer, using the given ServerSocket.
		///
		/// The server takes ownership of the HTTPRequstHandlerFactory
		/// and deletes it when it's no longer needed.
		///
		/// The server also takes ownership of the HTTPServerParams object.
		///
		/// New threads are taken from the given thread pool.

	~HTTPServer();
		/// Destroys the HTTPServer and its HTTPRequestHandlerFactory.

	void stopAll(bool abortCurrent = false);
		/// Stops the server. In contrast to TCPServer::stop(), which also
		/// stops the server, but allows all client connections to finish at
		/// their pace, this allows finer control over client connections.
		///
		/// If abortCurrent is false, all current requests are allowed to
		/// complete. If abortCurrent is true, the underlying sockets of
		/// all client connections are shut down, causing all requests
		/// to abort.

private:
	HTTPRequestHandlerFactory::Ptr _pFactory;
};


} } // namespace Poco::Net


#endif // Net_HTTPServer_INCLUDED
