//
// RSAKey.h
//
// Library: Crypto
// Package: RSA
// Module:  RSAKey
//
// Definition of the RSAKey class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_RSAKey_INCLUDED
#define Crypto_RSAKey_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/KeyPair.h"
#include "Poco/Crypto/RSAKeyImpl.h"


namespace Poco {
namespace Crypto {


class X509Certificate;
class PKCS12Container;


class Crypto_API RSAKey : public KeyPair
	/// This class stores an RSA key pair, consisting
	/// of private and public key. Storage of the private
	/// key is optional.
	///
	/// If a private key is available, the RSAKey can be
	/// used for decrypting data (encrypted with the public key)
	/// or computing secure digital signatures.
{
public:
	enum KeyLength
	{
		KL_512  = 512,
		KL_1024 = 1024,
		KL_2048 = 2048,
		KL_4096 = 4096
	};

	enum Exponent
	{
		EXP_SMALL = 0,
		EXP_LARGE
	};

	RSAKey(const EVPPKey& key);
		/// Constructs ECKeyImpl by extracting the EC key.

	RSAKey(const X509Certificate& cert);
		/// Extracts the RSA public key from the given certificate.

	RSAKey(const PKCS12Container& cert);
		/// Extracts the RSA private key from the given certificate.

	RSAKey(KeyLength keyLength, Exponent exp);
		/// Creates the RSAKey. Creates a new public/private keypair using the given parameters.
		/// Can be used to sign data and verify signatures.

	RSAKey(const std::string& publicKeyFile,
		const std::string& privateKeyFile = "",
		const std::string& privateKeyPassphrase = "");
		/// Creates the RSAKey, by reading public and private key from the given files and
		/// using the given passphrase for the private key.
		///
		/// Cannot be used for signing or decryption unless a private key is available.
		///
		/// If a private key is specified, you don't need to specify a public key file.
		/// OpenSSL will auto-create the public key from the private key.

	RSAKey(std::istream* pPublicKeyStream,
		std::istream* pPrivateKeyStream = 0,
		const std::string& privateKeyPassphrase = "");
		/// Creates the RSAKey, by reading public and private key from the given streams and
		/// using the given passphrase for the private key.
		///
		/// Cannot be used for signing or decryption unless a private key is available.
		///
		/// If a private key is specified, you don't need to specify a public key file.
		/// OpenSSL will auto-create the public key from the private key.

	~RSAKey();
		/// Destroys the RSAKey.

	RSAKeyImpl::ByteVec modulus() const;
		/// Returns the RSA modulus.

	RSAKeyImpl::ByteVec encryptionExponent() const;
		/// Returns the RSA encryption exponent.

	RSAKeyImpl::ByteVec decryptionExponent() const;
		/// Returns the RSA decryption exponent.

	RSAKeyImpl::Ptr impl() const;
		/// Returns the impl object.
	
private:
	RSAKeyImpl::Ptr _pImpl;
};


//
// inlines
//
inline RSAKeyImpl::Ptr RSAKey::impl() const
{
	return _pImpl;
}


} } // namespace Poco::Crypto


#endif // Crypto_RSAKey_INCLUDED