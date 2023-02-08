//
// CipherKey.h
//
// Library: Crypto
// Package: Cipher
// Module:  CipherKey
//
// Definition of the CipherKey class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_CipherKey_INCLUDED
#define Crypto_CipherKey_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/CipherKeyImpl.h"


namespace Poco {
namespace Crypto {


class Crypto_API CipherKey
	/// CipherKey stores the key information for decryption/encryption of data.
	/// To create a random key, using the following code:
	///
	///     CipherKey key("aes-256");
	///
	/// Note that you won't be able to decrypt data encrypted with a random key
	/// once the Cipher is destroyed unless you persist the generated key and IV.
	/// An example usage for random keys is to encrypt data saved in a temporary
	/// file.
	///
	/// To create a key using a human-readable password
	/// string, use the following code. We create a AES Cipher and 
	/// use a salt value to make the key more robust:
	///
	///     std::string password = "secret";
	///     std::string salt("asdff8723lasdf(**923412");
	///     CipherKey key("aes-256", password, salt);
	///
	/// You may also control the digest and the number of iterations used to generate the key
	/// by specifying the specific values. Here we create a key with the same data as before,
	/// except that we use 100 iterations instead of DEFAULT_ITERATION_COUNT, and sha1 instead of
	/// the default md5:
	///
	///     std::string password = "secret";
	///     std::string salt("asdff8723lasdf(**923412");
	///     std::string digest ("sha1");
	///     CipherKey key("aes-256", password, salt, 100, digest);
	///
{
public:
	typedef CipherKeyImpl::Mode Mode;
	typedef CipherKeyImpl::ByteVec ByteVec;

	enum
	{
		DEFAULT_ITERATION_COUNT = 2000
			/// Default iteration count to use with
			/// generateKey(). RSA security recommends
			/// an iteration count of at least 1000.
	};

	CipherKey(const std::string& name, 
		const std::string& passphrase, 
		const std::string& salt = "",
		int iterationCount = DEFAULT_ITERATION_COUNT,
		const std::string& digest = "md5");
		/// Creates a new CipherKeyImpl object using the given
		/// cipher name, passphrase, salt value, iteration count and digest.

	CipherKey(const std::string& name, 
		const ByteVec& key, 
		const ByteVec& iv);
		/// Creates a new CipherKeyImpl object using the given cipher
		/// name, key and initialization vector (IV).
		///
		/// The size of the IV must match the cipher's expected
		/// IV size (see ivSize()), except for GCM mode, which allows
		/// a custom IV size.

	CipherKey(const std::string& name);
		/// Creates a new CipherKeyImpl object. Autoinitializes key and 
		/// initialization vector.

	~CipherKey();
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
		///
		/// The size of the vector must match the cipher's expected
		/// IV size (see ivSize()), except for GCM mode, which allows
		/// a custom IV size.

	CipherKeyImpl::Ptr impl();
		/// Returns the impl object

private:
	CipherKeyImpl::Ptr _pImpl;
};


//
// inlines
//
inline const std::string& CipherKey::name() const
{
	return _pImpl->name();
}


inline int CipherKey::keySize() const
{
	return _pImpl->keySize();
}


inline int CipherKey::blockSize() const
{
	return _pImpl->blockSize();
}


inline int CipherKey::ivSize() const
{
	return _pImpl->ivSize();
}


inline CipherKey::Mode CipherKey::mode() const
{
	return _pImpl->mode();
}


inline const CipherKey::ByteVec& CipherKey::getKey() const
{
	return _pImpl->getKey();
}


inline void CipherKey::setKey(const CipherKey::ByteVec& key)
{
	_pImpl->setKey(key);
}


inline const CipherKey::ByteVec& CipherKey::getIV() const
{
	return _pImpl->getIV();
}


inline void CipherKey::setIV(const CipherKey::ByteVec& iv)
{
	_pImpl->setIV(iv);
}


inline CipherKeyImpl::Ptr CipherKey::impl()
{
	return _pImpl;
}


} } // namespace Poco::Crypto


#endif // Crypto_CipherKey_INCLUDED
