//
// RSAKey.cpp
//
// Library: Crypto
// Package: RSA
// Module:  RSAKey
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/RSAKey.h"
#include <openssl/rsa.h>


namespace Poco {
namespace Crypto {


RSAKey::RSAKey(const EVPPKey& key):
	KeyPair(new RSAKeyImpl(key)),
	_pImpl(KeyPair::impl().cast<RSAKeyImpl>())
{
}


RSAKey::RSAKey(const X509Certificate& cert):
	KeyPair(new RSAKeyImpl(cert)),
	_pImpl(KeyPair::impl().cast<RSAKeyImpl>())
{
}


RSAKey::RSAKey(const PKCS12Container& cont):
	KeyPair(new RSAKeyImpl(cont)),
	_pImpl(KeyPair::impl().cast<RSAKeyImpl>())
{
}


RSAKey::RSAKey(KeyLength keyLength, Exponent exp):
	KeyPair(new RSAKeyImpl(keyLength, (exp == EXP_LARGE) ? RSA_F4 : RSA_3)),
	_pImpl(KeyPair::impl().cast<RSAKeyImpl>())
{
}


RSAKey::RSAKey(const std::string& publicKeyFile, const std::string& privateKeyFile, const std::string& privateKeyPassphrase):
	KeyPair(new RSAKeyImpl(publicKeyFile, privateKeyFile, privateKeyPassphrase)),
	_pImpl(KeyPair::impl().cast<RSAKeyImpl>())
{
}


RSAKey::RSAKey(std::istream* pPublicKeyStream, std::istream* pPrivateKeyStream, const std::string& privateKeyPassphrase):
	KeyPair(new RSAKeyImpl(pPublicKeyStream, pPrivateKeyStream, privateKeyPassphrase)),
	_pImpl(KeyPair::impl().cast<RSAKeyImpl>())
{
}


RSAKey::~RSAKey()
{
}

RSAKeyImpl::ByteVec RSAKey::modulus() const
{
	return _pImpl->modulus();
}


RSAKeyImpl::ByteVec RSAKey::encryptionExponent() const
{
	return _pImpl->encryptionExponent();
}


RSAKeyImpl::ByteVec RSAKey::decryptionExponent() const
{
	return _pImpl->decryptionExponent();
}


} } // namespace Poco::Crypto