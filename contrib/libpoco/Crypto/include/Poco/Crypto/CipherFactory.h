//
// CipherFactory.h
//
// $Id: //poco/1.4/Crypto/include/Poco/Crypto/CipherFactory.h#1 $
//
// Library: Crypto
// Package: Cipher
// Module:  CipherFactory
//
// Definition of the CipherFactory class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_CipherFactory_INCLUDED
#define Crypto_CipherFactory_INCLUDED


#include "Poco/Crypto/Crypto.h"


namespace Poco {
namespace Crypto {


class Cipher;
class CipherKey;
class RSAKey;


class Crypto_API CipherFactory
	/// A factory for Cipher objects. See the Cipher class for examples on how to
	/// use the CipherFactory.
{
public:
	CipherFactory();
		/// Creates a new CipherFactory object.

	virtual ~CipherFactory();
		/// Destroys the CipherFactory.

	Cipher* createCipher(const CipherKey& key);
		/// Creates a Cipher object for the given Cipher name. Valid cipher 
		/// names depend on the OpenSSL version the library is linked with;  
		/// see the output of
		///
		///     openssl enc --help
		///
		/// for a list of supported block and stream ciphers.
		///
		/// Common examples are:
		///
		///   * AES: "aes-128", "aes-256"
		///   * DES: "des", "des3"
		///   * Blowfish: "bf"

	Cipher* createCipher(const RSAKey& key, RSAPaddingMode paddingMode = RSA_PADDING_PKCS1);
		/// Creates a RSACipher using the given RSA key and padding mode
		/// for public key encryption/private key decryption.
	
	static CipherFactory& defaultFactory();
		/// Returns the default CipherFactory.

private:
	CipherFactory(const CipherFactory&);
	CipherFactory& operator = (const CipherFactory&);
};


} } // namespace Poco::Crypto


#endif // Crypto_CipherFactory_INCLUDED
