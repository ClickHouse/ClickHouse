//
// OpenSSLInitializer.cpp
//
// Library: Crypto
// Package: CryptoCore
// Module:  OpenSSLInitializer
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include <openssl/provider.h>
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/conf.h>

#include "Poco/Crypto/OpenSSLInitializer.h"


namespace Poco {
namespace Crypto {


Poco::AtomicCounter OpenSSLInitializer::_rc;
OSSL_PROVIDER *OpenSSLInitializer::_fipsProvider = nullptr;


OpenSSLInitializer::OpenSSLInitializer()
{
	initialize();
}


OpenSSLInitializer::~OpenSSLInitializer()
{
	try
	{
		uninitialize();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void OpenSSLInitializer::initialize()
{
	if (++_rc == 1)
	{
        OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, nullptr);
	}
}


void OpenSSLInitializer::uninitialize()
{
    if (--_rc == 0)
    {
        if (_fipsProvider)
            OSSL_PROVIDER_unload(_fipsProvider);
    }
}


void initializeCrypto()
{
	OpenSSLInitializer::initialize();
}


void uninitializeCrypto()
{
	OpenSSLInitializer::uninitialize();
}


} } // namespace Poco::Crypto
