//
// CipherKeyImpl.h
//
// Library: Crypto
// Package: Cipher
// Module:  CipherKeyImpl
//
// Definition of the CipherKeyImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_CipherKeyImpl_INCLUDED
#define Crypto_CipherKeyImpl_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"
#include <vector>


struct evp_cipher_st;
typedef struct evp_cipher_st EVP_CIPHER;


namespace Poco {
namespace Crypto {


class CipherKeyImpl: public RefCountedObject
	/// An implementation of the CipherKey class for OpenSSL's crypto library.
{
public:
	typedef std::vector<unsigned char> ByteVec;
	typedef Poco::AutoPtr<CipherKeyImpl> Ptr;

	enum Mode
		/// Cipher mode of operation. This mode determines how multiple blocks
		/// are connected; this is essential to improve security.
	{
		MODE_STREAM_CIPHER,	/// Stream cipher
		MODE_ECB,			/// Electronic codebook (plain concatenation)
		MODE_CBC,			/// Cipher block chaining (default)
		MODE_CFB,			/// Cipher feedback
		MODE_OFB,			/// Output feedback
		MODE_CTR,           /// Counter mode
		MODE_GCM,           /// Galois/Counter mode
		MODE_CCM            /// Counter with CBC-MAC
	};

	CipherKeyImpl(const std::string& name,
		const std::string& passphrase,
		const std::string& salt,
		int iterationCount,
		const std::string& digest);
		/// Creates a new CipherKeyImpl object, using
		/// the given cipher name, passphrase, salt value
		/// and iteration count.

	CipherKeyImpl(const std::string& name,
		const ByteVec& key,
		const ByteVec& iv);
		/// Creates a new CipherKeyImpl object, using the
		/// given cipher name, key and initialization vector.

	CipherKeyImpl(const std::string& name);
		/// Creates a new CipherKeyImpl object. Autoinitializes key
		/// and initialization vector.

	virtual ~CipherKeyImpl();
		/// Destroys the CipherKeyImpl.

	const std::string& name() const;
		/// Returns the name of the Cipher.

	int keySize() const;
		/// Returns the key size of the Cipher.

	int blockSize() const;
		/// Returns the block size of the Cipher.

	int ivSize() const;
		/// Returns the IV size of the Cipher.

	Mode mode() const;
		/// Returns the Cipher's mode of operation.

	const ByteVec& getKey() const;
		/// Returns the key for the Cipher.

	void setKey(const ByteVec& key);
		/// Sets the key for the Cipher.

	const ByteVec& getIV() const;
		/// Returns the initialization vector (IV) for the Cipher.

	void setIV(const ByteVec& iv);
		/// Sets the initialization vector (IV) for the Cipher.

	const EVP_CIPHER* cipher();
		/// Returns the cipher object

private:
	void generateKey(const std::string& passphrase,
		const std::string& salt,
		int iterationCount);
	 	/// Generates key and IV from a password and optional salt string.

	void generateKey();
		/// Generates key and IV from random data.

	void getRandomBytes(ByteVec& vec, std::size_t count);
		/// Stores random bytes in vec.

private:
	const EVP_CIPHER*  _pCipher;
	const EVP_MD*      _pDigest;
	std::string	       _name;
	ByteVec		       _key;
	ByteVec		       _iv;
	OpenSSLInitializer _openSSLInitializer;
};


//
// Inlines
//
inline const std::string& CipherKeyImpl::name() const
{
	return _name;
}


inline const CipherKeyImpl::ByteVec& CipherKeyImpl::getKey() const
{
	return _key;
}


inline void CipherKeyImpl::setKey(const ByteVec& key)
{
	poco_assert(key.size() == static_cast<ByteVec::size_type>(keySize()));
	_key = key;
}


inline const CipherKeyImpl::ByteVec& CipherKeyImpl::getIV() const
{
	return _iv;
}


inline const EVP_CIPHER* CipherKeyImpl::cipher()
{
	return _pCipher;
}


} } // namespace Poco::Crypto


#endif // Crypto_CipherKeyImpl_INCLUDED
