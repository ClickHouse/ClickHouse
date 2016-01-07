//
// RSAKeyImpl.h
//
// $Id: //poco/1.4/Crypto/include/Poco/Crypto/RSAKeyImpl.h#3 $
//
// Library: Crypto
// Package: RSA
// Module:  RSAKeyImpl
//
// Definition of the RSAKeyImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_RSAKeyImplImpl_INCLUDED
#define Crypto_RSAKeyImplImpl_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"
#include <istream>
#include <ostream>
#include <vector>


struct bignum_st;
struct rsa_st;
typedef struct bignum_st BIGNUM;
typedef struct rsa_st RSA;


namespace Poco {
namespace Crypto {


class X509Certificate;


class RSAKeyImpl: public Poco::RefCountedObject
	/// class RSAKeyImpl
{
public:
	typedef Poco::AutoPtr<RSAKeyImpl> Ptr;
	typedef std::vector<unsigned char> ByteVec;

	explicit RSAKeyImpl(const X509Certificate& cert);
		/// Extracts the RSA public key from the given certificate.

	RSAKeyImpl(int keyLength, unsigned long exponent);
		/// Creates the RSAKey. Creates a new public/private keypair using the given parameters.
		/// Can be used to sign data and verify signatures.

	RSAKeyImpl(const std::string& publicKeyFile, const std::string& privateKeyFile, const std::string& privateKeyPassphrase);
		/// Creates the RSAKey, by reading public and private key from the given files and
		/// using the given passphrase for the private key. Can only by used for signing if 
		/// a private key is available. 

	RSAKeyImpl(std::istream* pPublicKeyStream, std::istream* pPrivateKeyStream, const std::string& privateKeyPassphrase);
		/// Creates the RSAKey. Can only by used for signing if pPrivKey
		/// is not null. If a private key file is specified, you don't need to
		/// specify a public key file. OpenSSL will auto-create it from the private key.

	~RSAKeyImpl();
		/// Destroys the RSAKeyImpl.

	RSA* getRSA();
		/// Returns the OpenSSL RSA object.

	const RSA* getRSA() const;
		/// Returns the OpenSSL RSA object.

	int size() const;
		/// Returns the RSA modulus size.

	ByteVec modulus() const;
		/// Returns the RSA modulus.

	ByteVec encryptionExponent() const;
		/// Returns the RSA encryption exponent.

	ByteVec decryptionExponent() const;
		/// Returns the RSA decryption exponent.

	void save(const std::string& publicKeyFile, const std::string& privateKeyFile = "", const std::string& privateKeyPassphrase = "");
		/// Exports the public and private keys to the given files. 
		///
		/// If an empty filename is specified, the corresponding key
		/// is not exported.

	void save(std::ostream* pPublicKeyStream, std::ostream* pPrivateKeyStream = 0, const std::string& privateKeyPassphrase = "");
		/// Exports the public and private key to the given streams.
		///
		/// If a null pointer is passed for a stream, the corresponding
		/// key is not exported.

private:
	void freeRSA();

	static ByteVec convertToByteVec(const BIGNUM* bn);

private:
	RSA* _pRSA;
	OpenSSLInitializer _openSSLInitializer;
};


//
// inlines
//
inline RSA* RSAKeyImpl::getRSA()
{
	return _pRSA;
}


inline const RSA* RSAKeyImpl::getRSA() const
{
	return _pRSA;
}


} } // namespace Poco::Crypto


#endif // Crypto_RSAKeyImplImpl_INCLUDED
