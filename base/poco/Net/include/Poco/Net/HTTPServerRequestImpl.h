//
// HTTPServerRequestImpl.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerRequestImpl
//
// Definition of the HTTPServerRequestImpl class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPServerRequestImpl_INCLUDED
#define Net_HTTPServerRequestImpl_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponseImpl.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/AutoPtr.h"
#include <istream>


namespace Poco {
namespace Net {


class HTTPServerSession;
class HTTPServerParams;
class StreamSocket;


class Net_API HTTPServerRequestImpl: public HTTPServerRequest
	/// This subclass of HTTPServerRequest is used for
	/// representing server-side HTTP requests.
	///
	/// A HTTPServerRequest is passed to the
	/// handleRequest() method of HTTPRequestHandler.
{
public:
	HTTPServerRequestImpl(HTTPServerResponseImpl& response, HTTPServerSession& session, HTTPServerParams* pParams);
		/// Creates the HTTPServerRequestImpl, using the
		/// given HTTPServerSession.

	~HTTPServerRequestImpl();
		/// Destroys the HTTPServerRequestImpl.
		
	std::istream& stream();
		/// Returns the input stream for reading
		/// the request body.
		///
		/// The stream is valid until the HTTPServerRequestImpl
		/// object is destroyed.
		
	const SocketAddress& clientAddress() const;
		/// Returns the client's address.

	const SocketAddress& serverAddress() const;
		/// Returns the server's address.
		
	const HTTPServerParams& serverParams() const;
		/// Returns a reference to the server parameters.

	HTTPServerResponse& response() const;
		/// Returns a reference to the associated response.
		
	bool secure() const;
		/// Returns true if the request is using a secure
		/// connection. Returns false if no secure connection
		/// is used, or if it is not known whether a secure
		/// connection is used.		
		
	StreamSocket& socket();
		/// Returns a reference to the underlying socket.
		
	StreamSocket detachSocket();
		/// Returns the underlying socket after detaching
		/// it from the server session.
		
	HTTPServerSession& session();
		/// Returns the underlying HTTPServerSession.

private:
	HTTPServerResponseImpl&         _response;
	HTTPServerSession&              _session;
	std::istream*                   _pStream;
	Poco::AutoPtr<HTTPServerParams> _pParams;
	SocketAddress                   _clientAddress;
	SocketAddress                   _serverAddress;
};


//
// inlines
//
inline std::istream& HTTPServerRequestImpl::stream()
{
	poco_check_ptr (_pStream);
	
	return *_pStream;
}


inline const SocketAddress& HTTPServerRequestImpl::clientAddress() const
{
	return _clientAddress;
}


inline const SocketAddress& HTTPServerRequestImpl::serverAddress() const
{
	return _serverAddress;
}


inline const HTTPServerParams& HTTPServerRequestImpl::serverParams() const
{
	return *_pParams;
}


inline HTTPServerResponse& HTTPServerRequestImpl::response() const
{
	return _response;
}


inline HTTPServerSession& HTTPServerRequestImpl::session()
{
	return _session;
}


} } // namespace Poco::Net


#endif // Net_HTTPServerRequestImpl_INCLUDED
