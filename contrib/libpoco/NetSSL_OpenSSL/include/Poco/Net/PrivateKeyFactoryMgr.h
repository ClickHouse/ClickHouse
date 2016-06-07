//
// PrivateKeyFactoryMgr.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/include/Poco/Net/PrivateKeyFactoryMgr.h#1 $
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  PrivateKeyFactoryMgr
//
// Definition of the PrivateKeyFactoryMgr class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_PrivateKeyFactoryMgr_INCLUDED
#define NetSSL_PrivateKeyFactoryMgr_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/PrivateKeyFactory.h"
#include "Poco/SharedPtr.h"
#include <map>


namespace Poco {
namespace Net {


class NetSSL_API PrivateKeyFactoryMgr
	/// A PrivateKeyFactoryMgr manages all existing PrivateKeyFactories.
{
public:
	typedef std::map<std::string, Poco::SharedPtr<PrivateKeyFactory> > FactoriesMap;
	
	PrivateKeyFactoryMgr();
		/// Creates the PrivateKeyFactoryMgr.

	~PrivateKeyFactoryMgr();
		/// Destroys the PrivateKeyFactoryMgr.

	void setFactory(const std::string& name, PrivateKeyFactory* pFactory);
		/// Registers the factory. Class takes ownership of the pointer.
		/// If a factory with the same name already exists, an exception is thrown.

	bool hasFactory(const std::string& name) const;
		/// Returns true if for the given name a factory is already registered
	
	const PrivateKeyFactory* getFactory(const std::string& name) const;
		/// Returns NULL if for the given name a factory does not exist, otherwise the factory is returned

	void removeFactory(const std::string& name);
		/// Removes the factory from the manager. 

private:
	FactoriesMap _factories;
};


} } // namespace Poco::Net


#endif // NetSSL_PrivateKeyFactoryMgr_INCLUDED
