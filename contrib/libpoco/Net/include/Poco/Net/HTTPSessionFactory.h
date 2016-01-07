//
// HTTPSessionFactory.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPSessionFactory.h#1 $
//
// Library: Net
// Package: HTTPClient
// Module:  HTTPSessionFactory
//
// Definition of the HTTPSessionFactory class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPSessionFactoryMgr_INCLUDED
#define Net_HTTPSessionFactoryMgr_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Mutex.h"
#include "Poco/URI.h"
#include "Poco/SingletonHolder.h"
#include "Poco/SharedPtr.h"
#include <map>


namespace Poco {
namespace Net {


class HTTPSessionInstantiator;
class HTTPClientSession;


class Net_API HTTPSessionFactory
	/// A factory for HTTPClientSession objects.
	///
	/// Given a URI, this class creates a HTTPClientSession
	/// (for http) or a HTTPSClientSession (for https) for
	/// accessing the URI.
	///
	/// The actual work of creating the session is done by
	/// HTTPSessionInstantiator objects that must be registered
	/// with a HTTPSessionFactory.
{
public:
	HTTPSessionFactory();
		/// Creates the HTTPSessionFactory.

	HTTPSessionFactory(const std::string& proxyHost, Poco::UInt16 proxyPort);
		/// Creates the HTTPSessionFactory and sets the proxy host and port.

	~HTTPSessionFactory();
		/// Destroys the HTTPSessionFactory.

	void registerProtocol(const std::string& protocol, HTTPSessionInstantiator* pSessionInstantiator);
		/// Registers the session instantiator for the given protocol.
		/// The factory takes ownership of the SessionInstantiator.
		///
		/// A protocol can be registered more than once. However, only the instantiator
		/// that has been registered first is used. Also, for each call to
		/// registerProtocol(), a corresponding call to unregisterProtocol() must
		/// be made.

	void unregisterProtocol(const std::string& protocol);
		/// Removes the registration of a protocol.
		///
		/// Throws a NotFoundException if no instantiator has been registered
		/// for the given protocol.

	bool supportsProtocol(const std::string& protocol);
		/// Returns true if a session instantiator for the given protocol has been registered.

	HTTPClientSession* createClientSession(const Poco::URI& uri);
		/// Creates a client session for the given uri scheme. Throws exception if no factory is registered for the given scheme

	const std::string& proxyHost() const;
		/// Returns the proxy host, if one has been set, or an empty string otherwise.
		
	Poco::UInt16 proxyPort() const;
		/// Returns the proxy port number, if one has been set, or zero otherwise.

	void setProxy(const std::string& proxyHost, Poco::UInt16 proxyPort);
		/// Sets the proxy host and port number.
		
	void setProxyCredentials(const std::string& username, const std::string& password);
		/// Sets the username and password for proxy authorization (Basic auth only).

	const std::string& proxyUsername() const;
		/// Returns the username for proxy authorization.
		
	const std::string& proxyPassword() const;
		/// Returns the password for proxy authorization.

	static HTTPSessionFactory& defaultFactory();
		/// Returns the default HTTPSessionFactory.

private:
	struct InstantiatorInfo
	{
		HTTPSessionInstantiator* pIn;
		int cnt;
		InstantiatorInfo(HTTPSessionInstantiator* pInst);
			// no destructor!!! this is by purpose, don't add one!
	};


	HTTPSessionFactory(const HTTPSessionFactory&);
	HTTPSessionFactory& operator = (const HTTPSessionFactory&);
	
	typedef std::map<std::string, InstantiatorInfo> Instantiators;

	Instantiators _instantiators;
	std::string   _proxyHost;
	Poco::UInt16  _proxyPort;
	std::string   _proxyUsername;
	std::string   _proxyPassword;

	mutable Poco::FastMutex _mutex;
};


//
// inlines
//
inline const std::string& HTTPSessionFactory::proxyHost() const
{
	return _proxyHost;
}


inline Poco::UInt16 HTTPSessionFactory::proxyPort() const
{
	return _proxyPort;
}


inline const std::string& HTTPSessionFactory::proxyUsername() const
{
	return _proxyUsername;
}


inline const std::string& HTTPSessionFactory::proxyPassword() const
{
	return _proxyPassword;
}


} } // namespace Poco::Net


#endif // Net_HTTPSessionFactoryMgr_INCLUDED
