//
// CipherImpl.h
//
// $Id: //poco/1.4/Crypto/include/Poco/Crypto/CipherImpl.h#2 $
//
// Library: Crypto
// Package: Cipher
// Module:  CipherImpl
//
// Definition of the CipherImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_CipherImpl_INCLUDED
#define Crypto_CipherImpl_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/Cipher.h"
#include "Poco/Crypto/CipherKey.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include <openssl/evp.h>


namespace Poco {
namespace Crypto {


class CipherImpl: public Cipher
	/// An implementation of the Cipher class for OpenSSL's crypto library.
{
public:
	CipherImpl(const CipherKey& key);
		/// Creates a new CipherImpl object for the given CipherKey.

	virtual ~CipherImpl();
		/// Destroys the CipherImpl.

	const std::string& name() const;
		/// Returns the name of the cipher.

	CryptoTransform* createEncryptor();
		/// Creates an encrytor object.

	CryptoTransform* createDecryptor();
		/// Creates a decrytor object.

private:
	CipherKey _key;
	OpenSSLInitializer _openSSLInitializer;
};


//
// Inlines
//
inline const std::string& CipherImpl::name() const
{
	return _key.name();
}


} } // namespace Poco::Crypto


#endif // Crypto_CipherImpl_INCLUDED
