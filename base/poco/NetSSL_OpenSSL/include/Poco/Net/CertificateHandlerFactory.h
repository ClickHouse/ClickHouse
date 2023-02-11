//
// CertificateHandlerFactory.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  CertificateHandlerFactory
//
// Definition of the CertificateHandlerFactory class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_CertificateHandlerFactory_INCLUDED
#define NetSSL_CertificateHandlerFactory_INCLUDED


#include "Poco/Net/NetSSL.h"


namespace Poco {
namespace Net {


class InvalidCertificateHandler;


class NetSSL_API CertificateHandlerFactory
	/// A CertificateHandlerFactory is responsible for creating InvalidCertificateHandlers.
	///
	/// You don't need to access this class directly. Use the macro
	///     POCO_REGISTER_CHFACTORY(namespace, InvalidCertificateHandlerName) 
	/// instead (see the documentation of InvalidCertificateHandler for an example).
{
public:
	CertificateHandlerFactory();
		/// Creates the CertificateHandlerFactory.

	virtual ~CertificateHandlerFactory();
		/// Destroys the CertificateHandlerFactory.

	virtual InvalidCertificateHandler* create(bool server) const = 0;
		/// Creates a new InvalidCertificateHandler. Set server to true if the certificate handler is used on the server side.
};


class NetSSL_API CertificateHandlerFactoryRegistrar
	/// Registrar class which automatically registers CertificateHandlerFactory at the CertificateHandlerFactoryMgr.
	/// You don't need to access this class directly. Use the macro
	///     POCO_REGISTER_CHFACTORY(namespace, InvalidCertificateHandlerName) 
	/// instead (see the documentation of InvalidCertificateHandler for an example).
{
public:
	CertificateHandlerFactoryRegistrar(const std::string& name, CertificateHandlerFactory* pFactory);
		/// Registers the CertificateHandlerFactory with the given name at the factory manager.

	virtual ~CertificateHandlerFactoryRegistrar();
		/// Destroys the CertificateHandlerFactoryRegistrar.
};


template <typename T>
class CertificateHandlerFactoryImpl: public Poco::Net::CertificateHandlerFactory
{
public:
	CertificateHandlerFactoryImpl()
	{
	}

	~CertificateHandlerFactoryImpl()
	{
	}

	InvalidCertificateHandler* create(bool server) const
	{
		return new T(server);
	}
};


} } // namespace Poco::Net


// DEPRECATED: register the factory directly at the FactoryMgr:
// Poco::Net::SSLManager::instance().certificateHandlerFactoryMgr().setFactory(name, new Poco::Net::CertificateHandlerFactoryImpl<MyConsoleHandler>());
#define POCO_REGISTER_CHFACTORY(API, PKCLS)		\
	static Poco::Net::CertificateHandlerFactoryRegistrar aRegistrar(std::string(#PKCLS), new Poco::Net::CertificateHandlerFactoryImpl<PKCLS>());


#endif // NetSSL_CertificateHandlerFactory_INCLUDED
