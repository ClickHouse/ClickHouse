//
// RSAKey.cpp
//
// $Id: //poco/1.4/Crypto/src/RSAKey.cpp#2 $
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


RSAKey::RSAKey(const X509Certificate& cert):
	_pImpl(new RSAKeyImpl(cert))
{
}


RSAKey::RSAKey(KeyLength keyLength, Exponent exp):
	_pImpl(0)
{
	int keyLen = keyLength;
	unsigned long expVal = RSA_3;
	if (exp == EXP_LARGE)
		expVal = RSA_F4;
	_pImpl = new RSAKeyImpl(keyLen, expVal);
}


RSAKey::RSAKey(const std::string& publicKeyFile, const std::string& privateKeyFile, const std::string& privateKeyPassphrase):
	_pImpl(new RSAKeyImpl(publicKeyFile, privateKeyFile, privateKeyPassphrase))
{
}


RSAKey::RSAKey(std::istream* pPublicKeyStream, std::istream* pPrivateKeyStream, const std::string& privateKeyPassphrase):
	_pImpl(new RSAKeyImpl(pPublicKeyStream, pPrivateKeyStream, privateKeyPassphrase))
{
}


RSAKey::~RSAKey()
{
}


int RSAKey::size() const
{
	return _pImpl->size();
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


void RSAKey::save(const std::string& publicKeyFile, const std::string& privateKeyFile, const std::string& privateKeyPassphrase)
{
	_pImpl->save(publicKeyFile, privateKeyFile, privateKeyPassphrase);
}


void RSAKey::save(std::ostream* pPublicKeyStream, std::ostream* pPrivateKeyStream, const std::string& privateKeyPassphrase)
{
	_pImpl->save(pPublicKeyStream, pPrivateKeyStream, privateKeyPassphrase);
}


namespace
{
	static const std::string RSA("rsa");
}


const std::string& RSAKey::name() const
{
	return RSA;
}


} } // namespace Poco::Crypto
