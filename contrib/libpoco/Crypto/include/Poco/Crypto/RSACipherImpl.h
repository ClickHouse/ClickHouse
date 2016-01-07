//
// RSACipherImpl.h
//
// $Id: //poco/1.4/Crypto/include/Poco/Crypto/RSACipherImpl.h#2 $
//
// Library: Crypto
// Package: RSA
// Module:  RSACipherImpl
//
// Definition of the RSACipherImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_RSACipherImpl_INCLUDED
#define Crypto_RSACipherImpl_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/Cipher.h"
#include "Poco/Crypto/RSAKey.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include <openssl/evp.h>


namespace Poco {
namespace Crypto {


class RSACipherImpl: public Cipher
	/// An implementation of the Cipher class for 
	/// assymetric (public-private key) encryption
	/// based on the the RSA algorithm in OpenSSL's 
	/// crypto library.
	///
	/// Encryption is using the public key, decryption
	/// requires the private key.
{
public:
	RSACipherImpl(const RSAKey& key, RSAPaddingMode paddingMode);
		/// Creates a new RSACipherImpl object for the given RSAKey
		/// and using the given padding mode.

	virtual ~RSACipherImpl();
		/// Destroys the RSACipherImpl.

	const std::string& name() const;
		/// Returns the name of the Cipher.
	
	CryptoTransform* createEncryptor();
		/// Creates an encrytor object.

	CryptoTransform* createDecryptor();
		/// Creates a decrytor object.

private:
	RSAKey _key;
	RSAPaddingMode _paddingMode;
	OpenSSLInitializer _openSSLInitializer;
};


//
// Inlines
//
inline const std::string& RSACipherImpl::name() const
{
	return _key.name();
}


} } // namespace Poco::Crypto


#endif // Crypto_RSACipherImpl_INCLUDED
