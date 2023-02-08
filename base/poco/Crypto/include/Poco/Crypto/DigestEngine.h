//
// DigestEngine.h
//
// Library: Crypto
// Package: Digest
// Module:  DigestEngine
//
// Definition of the DigestEngine class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_DigestEngine_INCLUDED
#define Crypto_DigestEngine_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/DigestEngine.h"
#include <openssl/evp.h>


namespace Poco {
namespace Crypto {


class Crypto_API DigestEngine: public Poco::DigestEngine
	/// This class implements a Poco::DigestEngine for all
	/// digest algorithms supported by OpenSSL.
{
public:
	DigestEngine(const std::string& name);
		/// Creates a DigestEngine using the digest with the given name
		/// (e.g., "MD5", "SHA1", "SHA256", "SHA512", etc.).
		/// See the OpenSSL documentation for a list of supported digest algorithms.
		///
		/// Throws a Poco::NotFoundException if no algorithm with the given name exists.

	~DigestEngine();
		/// Destroys the DigestEngine.

	const std::string& algorithm() const;
		/// Returns the name of the digest algorithm.

	int nid() const;
		/// Returns the NID (OpenSSL object identifier) of the digest algorithm.

	// DigestEngine
	std::size_t digestLength() const;
	void reset();
	const Poco::DigestEngine::Digest& digest();

protected:
	void updateImpl(const void* data, std::size_t length);

private:
	std::string _name;
	EVP_MD_CTX* _pContext;
	Poco::DigestEngine::Digest _digest;
	OpenSSLInitializer _openSSLInitializer;
};


//
// inlines
//
inline const std::string& DigestEngine::algorithm() const
{
	return _name;
}


} } // namespace Poco::Crypto


#endif // Crypto_DigestEngine_INCLUDED
