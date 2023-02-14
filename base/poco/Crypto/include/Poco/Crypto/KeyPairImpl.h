//
// KeyPairImpl.h
//
//
// Library: Crypto
// Package: CryptoCore
// Module:  KeyPairImpl
//
// Definition of the KeyPairImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_KeyPairImplImpl_INCLUDED
#define Crypto_KeyPairImplImpl_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"
#include <string>
#include <vector>


namespace Poco {
namespace Crypto {


class KeyPairImpl: public Poco::RefCountedObject
	/// Class KeyPairImpl
{
public:
	enum Type
	{
		KT_RSA_IMPL = 0,
		KT_EC_IMPL
	};

	typedef Poco::AutoPtr<KeyPairImpl> Ptr;
	typedef std::vector<unsigned char> ByteVec;

	KeyPairImpl(const std::string& name, Type type);
		/// Create KeyPairImpl with specified type and name.

	virtual ~KeyPairImpl();
		/// Destroys the KeyPairImpl.

	virtual int size() const = 0;
		/// Returns the key size.

	virtual void save(const std::string& publicKeyFile,
		const std::string& privateKeyFile = "",
		const std::string& privateKeyPassphrase = "") const = 0;
		/// Exports the public and private keys to the given files. 
		///
		/// If an empty filename is specified, the corresponding key
		/// is not exported.

	virtual void save(std::ostream* pPublicKeyStream,
		std::ostream* pPrivateKeyStream = 0,
		const std::string& privateKeyPassphrase = "") const = 0;
		/// Exports the public and private key to the given streams.
		///
		/// If a null pointer is passed for a stream, the corresponding
		/// key is not exported.

	const std::string& name() const;
		/// Returns key pair name

	Type type() const;
		/// Returns key pair type

private:
	KeyPairImpl();

	std::string _name;
	Type        _type;
	OpenSSLInitializer _openSSLInitializer;
};


//
// inlines
//


inline const std::string& KeyPairImpl::name() const
{
	return _name;
}


inline KeyPairImpl::Type KeyPairImpl::type() const
{
	return _type;
}


} } // namespace Poco::Crypto


#endif // Crypto_KeyPairImplImpl_INCLUDED
