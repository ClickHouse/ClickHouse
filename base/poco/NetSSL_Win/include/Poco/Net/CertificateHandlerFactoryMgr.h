//
// CertificateHandlerFactoryMgr.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  CertificateHandlerFactoryMgr
//
// Definition of the CertificateHandlerFactoryMgr class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_CertificateHandlerFactoryMgr_INCLUDED
#define NetSSL_CertificateHandlerFactoryMgr_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/CertificateHandlerFactory.h"
#include "Poco/SharedPtr.h"
#include <map>


namespace Poco {
namespace Net {


class NetSSL_Win_API CertificateHandlerFactoryMgr
	/// A CertificateHandlerFactoryMgr manages all existing CertificateHandlerFactories.
{
public:
	typedef std::map<std::string, Poco::SharedPtr<CertificateHandlerFactory> > FactoriesMap;
	
	CertificateHandlerFactoryMgr();
		/// Creates the CertificateHandlerFactoryMgr.

	~CertificateHandlerFactoryMgr();
		/// Destroys the CertificateHandlerFactoryMgr.

	void setFactory(const std::string& name, CertificateHandlerFactory* pFactory);
		/// Registers the factory. Class takes ownership of the pointer.
		/// If a factory with the same name already exists, an exception is thrown.

	bool hasFactory(const std::string& name) const;
		/// Returns true if for the given name a factory is already registered
	
	const CertificateHandlerFactory* getFactory(const std::string& name) const;
		/// Returns NULL if for the given name a factory does not exist, otherwise the factory is returned

	void removeFactory(const std::string& name);
		/// Removes the factory from the manager. 

private:
	FactoriesMap _factories;
};


} } // namespace Poco::Net


#endif // NetSSL_CertificateHandlerFactoryMgr_INCLUDED
