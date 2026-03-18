//
// CipherFactory.cpp
//
// Library: Crypto
// Package: Cipher
// Module:  CipherFactory
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/CipherFactory.h"
#include "Poco/Crypto/Cipher.h"
#include "Poco/Crypto/CipherKey.h"
#include "Poco/Crypto/RSAKey.h"
#include "Poco/Crypto/CipherImpl.h"
#include "Poco/Crypto/RSACipherImpl.h"
#include "Poco/Exception.h"
#include "Poco/SingletonHolder.h"
#include <openssl/evp.h>
#include <openssl/err.h>


namespace Poco {
namespace Crypto {


CipherFactory::CipherFactory()
{
}


CipherFactory::~CipherFactory()
{
}


namespace
{
	static Poco::SingletonHolder<CipherFactory> holder;
}


CipherFactory& CipherFactory::defaultFactory()
{
	return *holder.get();
}


Cipher* CipherFactory::createCipher(const CipherKey& key)
{
	return new CipherImpl(key);
}


Cipher* CipherFactory::createCipher(const RSAKey& key, RSAPaddingMode paddingMode)
{
	return new RSACipherImpl(key, paddingMode);
}


} } // namespace Poco::Crypto
