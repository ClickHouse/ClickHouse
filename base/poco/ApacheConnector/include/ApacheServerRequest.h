//
// ApacheServerRequest.h
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ApacheConnector_ApacheServerRequest_INCLUDED
#define ApacheConnector_ApacheServerRequest_INCLUDED


#include "ApacheConnector.h"
#include "ApacheStream.h"
#include "Poco/Net/HTTPServerRequest.h"
#include <set>


class ApacheServerResponse;


class ApacheServerRequest: public Poco::Net::HTTPServerRequest
{
public:
	ApacheServerRequest(
		ApacheRequestRec* pApacheRequest, 
		const char* serverName, 
		int serverPort, 
		const char* clientName, 
		int clientPort);
		/// Creates a new ApacheServerRequest.

	~ApacheServerRequest();
		/// Destroys the ApacheServerRequest.

	std::istream& stream();
		/// Returns the input stream for reading
		/// the request body.
		///
		/// The stream is valid until the HTTPServerRequest
		/// object is destroyed.

	bool expectContinue() const;
		/// Returns true if the client expects a
		/// 100 Continue response.

	const Poco::Net::SocketAddress& clientAddress() const;
		/// Returns the client's address.

	const Poco::Net::SocketAddress& serverAddress() const;
		/// Returns the server's address.

	const Poco::Net::HTTPServerParams& serverParams() const;
		/// Returns a reference to the server parameters.

	Poco::Net::HTTPServerResponse& response() const;
		/// Returns a reference to the associated response

protected:
	void setResponse(ApacheServerResponse* pResponse);

private:
	ApacheRequestRec*        _pApacheRequest;
	ApacheServerResponse*    _pResponse;
	ApacheInputStream*       _pStream;
	Poco::Net::SocketAddress _serverAddress;
	Poco::Net::SocketAddress _clientAddress;
	
	friend class ApacheServerResponse;
};


//
// inlines
//
inline std::istream& ApacheServerRequest::stream()
{
	poco_check_ptr (_pStream);
	
	return *_pStream;
}


inline const Poco::Net::SocketAddress& ApacheServerRequest::clientAddress() const
{
	return _clientAddress;
}


inline const Poco::Net::SocketAddress& ApacheServerRequest::serverAddress() const
{
	return _serverAddress;
}


#endif // ApacheConnector_ApacheServerRequest_INCLUDED
