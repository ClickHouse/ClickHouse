//
// ApacheServerResponse.h
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ApacheConnector_ApacheServerResponse_INCLUDED
#define ApacheConnector_ApacheServerResponse_INCLUDED


#include "ApacheConnector.h"
#include "ApacheStream.h"
#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPServerResponse.h"


class ApacheServerRequest;


class ApacheServerResponse: public Poco::Net::HTTPServerResponse
	/// This subclass of HTTPResponse is used for
	/// representing server-side HTTP responses for apache.
	///
	/// A ApacheServerResponse is passed to the
	/// handleRequest() method of HTTPRequestHandler.
	///
	/// handleRequest() must set a status code
	/// and optional reason phrase, set headers
	/// as necessary, and provide a message body.
{
public:
	ApacheServerResponse(ApacheServerRequest* pRequest);
		/// Creates the ApacheServerResponse.

	~ApacheServerResponse();
		/// Destroys the ApacheServerResponse.

	void sendContinue();
		/// Sends a 100 Continue response to the
		/// client.
		
	void sendErrorResponse(int status);
		/// Sends an error response with the given
		/// status back to the client.

	std::ostream& send();
		/// Sends the response header to the client and
		/// returns an output stream for sending the
		/// response body.
		///
		/// The returned stream is valid until the response
		/// object is destroyed.
		///
		/// Must not be called after sendFile(), sendBuffer() 
		/// or redirect() has been called.
		
	void sendFile(const std::string& path, const std::string& mediaType);
		/// Sends the response header to the client, followed
		/// by the content of the given file.
		///
		/// Must not be called after send(), sendBuffer() 
		/// or redirect() has been called.
		///
		/// Throws a FileNotFoundException if the file
		/// cannot be found, or an OpenFileException if
		/// the file cannot be opened.
		
	void sendBuffer(const void* pBuffer, std::size_t length);
		/// Sends the response header to the client, followed
		/// by the contents of the given buffer.
		///
		/// The Content-Length header of the response is set
		/// to length and chunked transfer encoding is disabled.
		///
		/// If both the HTTP message header and body (from the
		/// given buffer) fit into one single network packet, the 
		/// complete response can be sent in one network packet.
		///
		/// Must not be called after send(), sendFile()  
		/// or redirect() has been called.
		
	void redirect(const std::string& uri, Poco::Net::HTTPResponse::HTTPStatus status);
		/// Sets the status code, which must be one of
		/// HTTP_MOVED_PERMANENTLY (301), HTTP_FOUND (302),
		/// or HTTP_SEE_OTHER (303),
		/// and sets the "Location" header field
		/// to the given URI, which according to
		/// the HTTP specification, must be absolute.
		///
		/// Must not be called after send() has been called.
		
	void requireAuthentication(const std::string& realm);
		/// Sets the status code to 401 (Unauthorized)
		/// and sets the "WWW-Authenticate" header field
		/// according to the given realm.
		
	bool sent() const;
		/// Returns true if the response (header) has been sent.

private:
	void initApacheOutputStream();
		/// Initializes the ApacheOutputStram

	ApacheOutputStream* _pStream;
	ApacheRequestRec*   _pApacheRequest;
};


//
// inlines
//
inline bool ApacheServerResponse::sent() const
{
	return _pStream != 0;
}


#endif // ApacheConnector_ApacheServerResponse_INCLUDED
