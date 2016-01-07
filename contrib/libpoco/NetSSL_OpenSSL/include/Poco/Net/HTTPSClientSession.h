//
// HTTPSClientSession.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/include/Poco/Net/HTTPSClientSession.h#2 $
//
// Library: NetSSL_OpenSSL
// Package: HTTPSClient
// Module:  HTTPSClientSession
//
// Definition of the HTTPSClientSession class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_HTTPSClientSession_INCLUDED
#define NetSSL_HTTPSClientSession_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/Utility.h"
#include "Poco/Net/HTTPClientSession.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/Session.h"
#include "Poco/Net/X509Certificate.h"


namespace Poco {
namespace Net {


class SecureStreamSocket;
class HTTPRequest;
class HTTPResponse;


class NetSSL_API HTTPSClientSession: public HTTPClientSession
	/// This class implements the client-side of
	/// a HTTPS session.
	///
	/// To send a HTTPS request to a HTTPS server, first
	/// instantiate a HTTPSClientSession object and
	/// specify the server's host name and port number.
	///
	/// Then create a HTTPRequest object, fill it accordingly,
	/// and pass it as argument to the sendRequst() method.
	///
	/// sendRequest() will return an output stream that can
	/// be used to send the request body, if there is any.
	///
	/// After you are done sending the request body, create
	/// a HTTPResponse object and pass it to receiveResponse().
	///
	/// This will return an input stream that can be used to
	/// read the response body.
	///
	/// See RFC 2616 <http://www.faqs.org/rfcs/rfc2616.html> for more
	/// information about the HTTP protocol.
	///
	/// Note that sending requests that neither contain a content length
	/// field in the header nor are using chunked transfer encoding will
	/// result in a SSL protocol violation, as the framework shuts down
	/// the socket after sending the message body. No orderly SSL shutdown
	/// will be performed in this case.
	///
	/// If session caching has been enabled for the Context object passed
	/// to the HTTPSClientSession, the HTTPSClientSession class will
	/// attempt to reuse a previously obtained Session object in
	/// case of a reconnect.
{
public:
	enum
	{
		HTTPS_PORT = 443
	};
	
	HTTPSClientSession();
		/// Creates an unconnected HTTPSClientSession.

	explicit HTTPSClientSession(const SecureStreamSocket& socket);
		/// Creates a HTTPSClientSession using the given socket.
		/// The socket must not be connected. The session
		/// takes ownership of the socket.

	HTTPSClientSession(const SecureStreamSocket& socket, Session::Ptr pSession);
		/// Creates a HTTPSClientSession using the given socket.
		/// The socket must not be connected. The session
		/// takes ownership of the socket.
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

	HTTPSClientSession(const std::string& host, Poco::UInt16 port = HTTPS_PORT);
		/// Creates a HTTPSClientSession using the given host and port.

	explicit HTTPSClientSession(Context::Ptr pContext);
		/// Creates an unconnected HTTPSClientSession, using the
		/// give SSL context.

	HTTPSClientSession(Context::Ptr pContext, Session::Ptr pSession);
		/// Creates an unconnected HTTPSClientSession, using the
		/// give SSL context.
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

	HTTPSClientSession(const std::string& host, Poco::UInt16 port, Context::Ptr pContext);
		/// Creates a HTTPSClientSession using the given host and port,
		/// using the given SSL context.

	HTTPSClientSession(const std::string& host, Poco::UInt16 port, Context::Ptr pContext, Session::Ptr pSession);
		/// Creates a HTTPSClientSession using the given host and port,
		/// using the given SSL context.
		///
		/// The given Session is reused, if possible (client session
		/// caching is enabled for the given Context, and the server
		/// agrees to reuse the session).

	~HTTPSClientSession();
		/// Destroys the HTTPSClientSession and closes
		/// the underlying socket.
	
	bool secure() const;
		/// Return true iff the session uses SSL or TLS,
		/// or false otherwise.
		
	X509Certificate serverCertificate();
		/// Returns the server's certificate.
		///
		/// The certificate is available after the first request has been sent.
		
	Session::Ptr sslSession();
		/// Returns the SSL Session object for the current 
		/// connection, if session caching has been enabled for
		/// the HTTPSClientSession's Context. A null pointer is 
		/// returned otherwise.
		///
		/// The Session object can be obtained after the first request has
		/// been sent.
		
	// HTTPSession
	void abort();

protected:
	void connect(const SocketAddress& address);
	std::string proxyRequestPrefix() const;
	void proxyAuthenticate(HTTPRequest& request);
	int read(char* buffer, std::streamsize length);

private:
	HTTPSClientSession(const HTTPSClientSession&);
	HTTPSClientSession& operator = (const HTTPSClientSession&);
	
	Context::Ptr _pContext;
	Session::Ptr _pSession;
};


} } // namespace Poco::Net


#endif // Net_HTTPSClientSession_INCLUDED
