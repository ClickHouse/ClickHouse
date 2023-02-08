//
// HTTPSStreamFactory.h
//
// Library: NetSSL_OpenSSL
// Package: HTTPSClient
// Module:  HTTPSStreamFactory
//
// Definition of the HTTPSStreamFactory class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_HTTPSStreamFactory_INCLUDED
#define NetSSL_HTTPSStreamFactory_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/HTTPSession.h"
#include "Poco/URIStreamFactory.h"


namespace Poco {
namespace Net {


class NetSSL_API HTTPSStreamFactory: public Poco::URIStreamFactory
	/// An implementation of the URIStreamFactory interface
	/// that handles secure Hyper-Text Transfer Protocol (https) URIs.
{
public:
	HTTPSStreamFactory();
		/// Creates the HTTPSStreamFactory.

	HTTPSStreamFactory(const std::string& proxyHost, Poco::UInt16 proxyPort = HTTPSession::HTTP_PORT);
		/// Creates the HTTPSStreamFactory.
		///
		/// HTTPS connections will use the given proxy.

	HTTPSStreamFactory(const std::string& proxyHost, Poco::UInt16 proxyPort, const std::string& proxyUsername, const std::string& proxyPassword);
		/// Creates the HTTPSStreamFactory.
		///
		/// HTTPS connections will use the given proxy and
		/// will be authorized against the proxy using Basic authentication
		/// with the given proxyUsername and proxyPassword.

	~HTTPSStreamFactory();
		/// Destroys the HTTPSStreamFactory.
		
	std::istream* open(const Poco::URI& uri);
		/// Creates and opens a HTTPS stream for the given URI.
		/// The URI must be a https://... URI.
		///
		/// Throws a NetException if anything goes wrong.
		
	static void registerFactory();
		/// Registers the HTTPSStreamFactory with the
		/// default URIStreamOpener instance.	

	static void unregisterFactory();
		/// Unregisters the HTTPSStreamFactory with the
		/// default URIStreamOpener instance.	
		
private:
	enum
	{
		MAX_REDIRECTS = 10
	};
	
	std::string  _proxyHost;
	Poco::UInt16 _proxyPort;
	std::string  _proxyUsername;
	std::string  _proxyPassword;
};


} } // namespace Poco::Net


#endif // Net_HTTPSStreamFactory_INCLUDED
